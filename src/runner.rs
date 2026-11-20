use crate::error::{Result, RunKeelError};
use crate::manifest::{PipelineManifest, StepConfig, StepKind};
use chrono::Utc;
use indexmap::IndexSet;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StepStatus {
    Passed,
    Failed,
    Skipped,
    DryRun,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepOutcome {
    pub id: String,
    pub kind: StepKind,
    pub status: StepStatus,
    pub attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub elapsed_ms: u64,
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRunReport {
    pub schema_version: String,
    pub command: String,
    pub run_id: String,
    pub pipeline: String,
    pub started_at: String,
    pub ok: bool,
    pub dry_run: bool,
    pub total_steps: usize,
    pub passed_steps: usize,
    pub failed_steps: usize,
    pub skipped_steps: usize,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audit_log_path: Option<String>,
    pub steps: Vec<StepOutcome>,
}

pub struct ExecOptions {
    pub base_dir: PathBuf,
    pub subset: IndexSet<String>,
    pub max_parallel: usize,
    pub fail_fast: bool,
    pub dry_run: bool,
    pub run_dir: Option<PathBuf>,
    pub save_audit: bool,
}

pub async fn execute_pipeline(
    manifest: &PipelineManifest,
    opts: &ExecOptions,
) -> Result<PipelineRunReport> {
    let started_at = Utc::now();
    let run_id = format!(
        "{}-{}-{}",
        started_at.format("%Y%m%dT%H%M%S_%3fZ"),
        std::process::id(),
        sanitize_slug(&manifest.pipeline.name)
    );
    let wall_start = Instant::now();

    let waves = manifest.compute_waves(&opts.subset)?;
    let total_steps: usize = waves.iter().map(Vec::len).sum();

    if opts.dry_run {
        let mut outcomes = Vec::with_capacity(total_steps);
        for wave in waves {
            for s in wave {
                let cmd = s.build_command()?;
                outcomes.push(StepOutcome {
                    id: s.id.clone(),
                    kind: s.kind,
                    status: StepStatus::DryRun,
                    attempts: 0,
                    exit_code: None,
                    elapsed_ms: 0,
                    command: cmd.display,
                    summary: s.description.clone(),
                    error: None,
                });
            }
        }
        return Ok(PipelineRunReport {
            schema_version: "1.0".to_string(),
            command: "run".to_string(),
            run_id,
            pipeline: manifest.pipeline.name.clone(),
            started_at: started_at.to_rfc3339(),
            ok: true,
            dry_run: true,
            total_steps,
            passed_steps: 0,
            failed_steps: 0,
            skipped_steps: 0,
            elapsed_ms: wall_start.elapsed().as_millis() as u64,
            audit_log_path: None,
            steps: outcomes,
        });
    }

    let step_map: HashMap<String, StepConfig> = manifest
        .steps
        .iter()
        .filter(|s| opts.subset.contains(&s.id))
        .map(|s| (s.id.clone(), s.clone()))
        .collect();

    let mut remaining: HashSet<String> = step_map.keys().cloned().collect();
    let mut completed: HashMap<String, StepOutcome> = HashMap::new();
    let mut running: HashSet<String> = HashSet::new();
    let mut join_set: JoinSet<StepOutcome> = JoinSet::new();
    let semaphore = Arc::new(Semaphore::new(opts.max_parallel.max(1)));
    let mut any_hard_failure = false;

    loop {
        // 1. Mark steps whose upstream dependency failed/skipped as Skipped
        let mut newly_skipped: Vec<String> = Vec::new();
        for id in &remaining {
            let step = &step_map[id];
            let blocked_by = step.depends_on.iter().find(|dep| {
                if !opts.subset.contains(*dep) {
                    return false;
                }
                completed.get(*dep).is_some_and(|o| {
                    let dep_step = &step_map[*dep];
                    o.status == StepStatus::Skipped
                        || (o.status == StepStatus::Failed && !dep_step.continue_on_error)
                })
            });
            if let Some(blocker) = blocked_by {
                newly_skipped.push(id.clone());
                let cmd = step.build_command()?;
                completed.insert(
                    id.clone(),
                    StepOutcome {
                        id: id.clone(),
                        kind: step.kind,
                        status: StepStatus::Skipped,
                        attempts: 0,
                        exit_code: None,
                        elapsed_ms: 0,
                        command: cmd.display,
                        summary: Some(format!("Skipped because upstream step '{blocker}' failed.")),
                        error: None,
                    },
                );
            } else if opts.fail_fast && any_hard_failure {
                newly_skipped.push(id.clone());
                let cmd = step.build_command()?;
                completed.insert(
                    id.clone(),
                    StepOutcome {
                        id: id.clone(),
                        kind: step.kind,
                        status: StepStatus::Skipped,
                        attempts: 0,
                        exit_code: None,
                        elapsed_ms: 0,
                        command: cmd.display,
                        summary: Some("Skipped due to `--fail-fast`.".to_string()),
                        error: None,
                    },
                );
            }
        }
        for id in newly_skipped {
            remaining.remove(&id);
        }

        // 2. Launch all ready steps whose dependencies have completed cleanly
        let mut to_launch: Vec<String> = remaining
            .iter()
            .filter(|id| {
                let step = &step_map[*id];
                step.depends_on
                    .iter()
                    .all(|dep| !opts.subset.contains(dep) || completed.contains_key(dep))
            })
            .cloned()
            .collect();
        to_launch.sort();

        for id in to_launch {
            remaining.remove(&id);
            running.insert(id.clone());
            let step = step_map[&id].clone();
            let sem = Arc::clone(&semaphore);
            let base_dir = opts.base_dir.clone();
            let pipeline_env = manifest.pipeline.env.clone();
            let default_retries = manifest.pipeline.default_retries;
            let default_delay = manifest.pipeline.default_retry_delay_ms;

            join_set.spawn(async move {
                let _permit = sem.acquire_owned().await.expect("semaphore open");
                run_single_step(step, base_dir, pipeline_env, default_retries, default_delay).await
            });
        }

        if running.is_empty() && remaining.is_empty() {
            break;
        }

        // 3. Await next finished step
        if let Some(res) = join_set.join_next().await {
            let outcome =
                res.map_err(|e| RunKeelError::General(format!("task join error: {e}")))?;
            running.remove(&outcome.id);
            if outcome.status == StepStatus::Failed && !step_map[&outcome.id].continue_on_error {
                any_hard_failure = true;
            }
            completed.insert(outcome.id.clone(), outcome);
        }
    }

    // Order outcomes deterministically in manifest order
    let mut ordered_outcomes = Vec::with_capacity(total_steps);
    for s in &manifest.steps {
        if let Some(o) = completed.remove(&s.id) {
            ordered_outcomes.push(o);
        }
    }

    let passed_steps = ordered_outcomes
        .iter()
        .filter(|o| o.status == StepStatus::Passed)
        .count();
    let failed_steps = ordered_outcomes
        .iter()
        .filter(|o| o.status == StepStatus::Failed)
        .count();
    let skipped_steps = ordered_outcomes
        .iter()
        .filter(|o| o.status == StepStatus::Skipped)
        .count();

    let mut report = PipelineRunReport {
        schema_version: "1.0".to_string(),
        command: "run".to_string(),
        run_id: run_id.clone(),
        pipeline: manifest.pipeline.name.clone(),
        started_at: started_at.to_rfc3339(),
        ok: !any_hard_failure,
        dry_run: false,
        total_steps,
        passed_steps,
        failed_steps,
        skipped_steps,
        elapsed_ms: wall_start.elapsed().as_millis() as u64,
        audit_log_path: None,
        steps: ordered_outcomes,
    };

    if opts.save_audit {
        let dir = opts
            .run_dir
            .clone()
            .unwrap_or_else(|| opts.base_dir.join(&manifest.pipeline.state_dir));
        let path = save_audit_log(&dir, &report)?;
        report.audit_log_path = Some(path.display().to_string());
    }

    Ok(report)
}

async fn run_single_step(
    step: StepConfig,
    base_dir: PathBuf,
    pipeline_env: indexmap::IndexMap<String, String>,
    default_retries: u32,
    default_retry_delay_ms: u64,
) -> StepOutcome {
    let started = Instant::now();
    let cmd = match step.build_command() {
        Ok(c) => c,
        Err(e) => {
            return StepOutcome {
                id: step.id,
                kind: step.kind,
                status: StepStatus::Failed,
                attempts: 0,
                exit_code: None,
                elapsed_ms: 0,
                command: String::new(),
                summary: None,
                error: Some(e.to_string()),
            };
        }
    };

    let max_retries = step.retries.unwrap_or(default_retries);
    let retry_delay = step.retry_delay_ms.unwrap_or(default_retry_delay_ms);
    let workdir = match step.workdir {
        Some(ref w) => {
            let p = PathBuf::from(w);
            if p.is_absolute() { p } else { base_dir.join(p) }
        }
        None => base_dir,
    };

    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let mut proc = Command::new(&cmd.program);
        proc.args(&cmd.args).current_dir(&workdir);
        for (k, v) in &pipeline_env {
            proc.env(k, v);
        }
        for (k, v) in &step.env {
            proc.env(k, v);
        }

        let exec_fut = proc.output();
        let output_res = match step.timeout_secs {
            Some(secs) => match tokio::time::timeout(Duration::from_secs(secs), exec_fut).await {
                Ok(res) => res,
                Err(_) => {
                    if attempt <= max_retries {
                        tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                        continue;
                    }
                    return StepOutcome {
                        id: step.id,
                        kind: step.kind,
                        status: StepStatus::Failed,
                        attempts: attempt,
                        exit_code: None,
                        elapsed_ms: started.elapsed().as_millis() as u64,
                        command: cmd.display,
                        summary: None,
                        error: Some(format!("Step timed out after {secs}s")),
                    };
                }
            },
            None => exec_fut.await,
        };

        match output_res {
            Ok(out) => {
                let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
                let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
                let exit_code = out.status.code();
                let parsed_summary = extract_child_summary(&stdout);

                if out.status.success() {
                    return StepOutcome {
                        id: step.id,
                        kind: step.kind,
                        status: StepStatus::Passed,
                        attempts: attempt,
                        exit_code,
                        elapsed_ms: started.elapsed().as_millis() as u64,
                        command: cmd.display,
                        summary: parsed_summary.or_else(|| truncate_preview(&stdout)),
                        error: None,
                    };
                }

                if attempt <= max_retries {
                    tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                    continue;
                }

                let err_msg = extract_child_error(&stderr)
                    .or_else(|| parsed_summary.clone())
                    .or_else(|| truncate_preview(&stderr))
                    .or_else(|| truncate_preview(&stdout))
                    .unwrap_or_else(|| format!("Exited with status {:?}", exit_code));

                return StepOutcome {
                    id: step.id,
                    kind: step.kind,
                    status: StepStatus::Failed,
                    attempts: attempt,
                    exit_code,
                    elapsed_ms: started.elapsed().as_millis() as u64,
                    command: cmd.display,
                    summary: parsed_summary,
                    error: Some(err_msg),
                };
            }
            Err(e) => {
                if attempt <= max_retries {
                    tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                    continue;
                }
                return StepOutcome {
                    id: step.id,
                    kind: step.kind,
                    status: StepStatus::Failed,
                    attempts: attempt,
                    exit_code: None,
                    elapsed_ms: started.elapsed().as_millis() as u64,
                    command: cmd.display,
                    summary: None,
                    error: Some(format!("Failed to spawn '{}': {e}", cmd.program)),
                };
            }
        }
    }
}

/// Extract structured `summary` from a child Keel tool's `--format agent` JSON stdout.
fn extract_child_summary(stdout: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(stdout).ok()?;
    v.get("summary")?.as_str().map(ToString::to_string)
}

/// Extract structured `error.code` + `error.message` from a child Keel tool's JSON stderr.
fn extract_child_error(stderr: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(stderr).ok()?;
    let err = v.get("error")?;
    let code = err.get("code")?.as_str()?;
    let msg = err.get("message")?.as_str()?;
    Some(format!("{code}: {msg}"))
}

fn truncate_preview(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    let first_lines: String = trimmed.lines().take(3).collect::<Vec<_>>().join(" | ");
    let capped: String = first_lines.chars().take(240).collect();
    Some(capped)
}

fn save_audit_log(dir: &Path, report: &PipelineRunReport) -> Result<PathBuf> {
    fs::create_dir_all(dir).map_err(|source| RunKeelError::IoWrite {
        path: dir.to_path_buf(),
        source,
    })?;
    let path = dir.join(format!("{}.json", report.run_id));
    let json = serde_json::to_string_pretty(report)
        .map_err(|e| RunKeelError::General(format!("audit JSON encode error: {e}")))?;
    fs::write(&path, json).map_err(|source| RunKeelError::IoWrite {
        path: path.clone(),
        source,
    })?;
    Ok(path)
}

pub fn load_history(dir: &Path, limit: usize) -> Result<Vec<PipelineRunReport>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .map_err(|source| RunKeelError::IoRead {
            path: dir.to_path_buf(),
            source,
        })?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.extension()
                    .and_then(|e| e.to_str())
                    .is_some_and(|e| e.eq_ignore_ascii_case("json"))
        })
        .collect();

    // Newest first (filenames begin with YYYYMMDDTHHMMSSZ)
    files.sort();
    files.reverse();

    let mut runs = Vec::new();
    for f in files.into_iter().take(limit) {
        if let Ok(raw) = fs::read_to_string(&f)
            && let Ok(rep) = serde_json::from_str::<PipelineRunReport>(&raw)
        {
            runs.push(rep);
        }
    }
    Ok(runs)
}

fn sanitize_slug(s: &str) -> String {
    let out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    if out.is_empty() {
        "pipeline".to_string()
    } else {
        out
    }
}
