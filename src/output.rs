use crate::cli::OutputFormatArg;
use crate::error::{Result, RunKeelError};
use crate::manifest::PlanReport;
use crate::runner::{PipelineRunReport, StepStatus};
use owo_colors::OwoColorize;
use std::io::IsTerminal;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedFormat {
    Human,
    Agent,
    Json,
}

pub fn resolve_format(arg: OutputFormatArg) -> ResolvedFormat {
    match arg {
        OutputFormatArg::Human => ResolvedFormat::Human,
        OutputFormatArg::Agent => ResolvedFormat::Agent,
        OutputFormatArg::Json => ResolvedFormat::Json,
        OutputFormatArg::Auto => {
            if std::io::stdout().is_terminal() {
                ResolvedFormat::Human
            } else {
                ResolvedFormat::Agent
            }
        }
    }
}

pub fn should_use_color(no_color: bool, format: ResolvedFormat) -> bool {
    if no_color || format != ResolvedFormat::Human {
        return false;
    }
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    std::io::stdout().is_terminal()
}

pub fn emit_run_report(
    report: &PipelineRunReport,
    format: ResolvedFormat,
    color: bool,
) -> Result<()> {
    match format {
        ResolvedFormat::Human => {
            let badge = if report.dry_run {
                if color {
                    " DRY RUN ".on_yellow().black().bold().to_string()
                } else {
                    "[DRY RUN]".to_string()
                }
            } else if report.ok {
                if color {
                    " PASS ".on_green().black().bold().to_string()
                } else {
                    "[PASS]".to_string()
                }
            } else if color {
                " FAIL ".on_red().white().bold().to_string()
            } else {
                "[FAIL]".to_string()
            };

            println!(
                "{badge} {} ({}) — {} passed, {} failed, {} skipped in {}ms",
                report.pipeline,
                report.run_id,
                report.passed_steps,
                report.failed_steps,
                report.skipped_steps,
                report.elapsed_ms
            );

            for s in &report.steps {
                let st = match s.status {
                    StepStatus::Passed => {
                        if color {
                            "PASS".green().bold().to_string()
                        } else {
                            "PASS".to_string()
                        }
                    }
                    StepStatus::Failed => {
                        if color {
                            "FAIL".red().bold().to_string()
                        } else {
                            "FAIL".to_string()
                        }
                    }
                    StepStatus::Skipped => {
                        if color {
                            "SKIP".yellow().bold().to_string()
                        } else {
                            "SKIP".to_string()
                        }
                    }
                    StepStatus::DryRun => {
                        if color {
                            "PLAN".cyan().bold().to_string()
                        } else {
                            "PLAN".to_string()
                        }
                    }
                };
                println!(
                    "\n  {st} {} [{:?}] ({}ms, attempts={})",
                    s.id, s.kind, s.elapsed_ms, s.attempts
                );
                if report.dry_run {
                    println!("    cmd    : {}", s.command);
                }
                if let Some(ref sum) = s.summary {
                    println!("    summary: {sum}");
                }
                if let Some(ref err) = s.error {
                    println!("    error  : {err}");
                }
            }

            if let Some(ref log) = report.audit_log_path {
                println!("\n  audit log: {log}");
            }
            Ok(())
        }
        ResolvedFormat::Agent => {
            let summary = if report.dry_run {
                format!(
                    "DRY RUN: pipeline '{}' has {} step(s).",
                    report.pipeline, report.total_steps
                )
            } else if report.ok {
                format!(
                    "PASS: pipeline '{}' completed {} step(s) in {}ms.",
                    report.pipeline, report.passed_steps, report.elapsed_ms
                )
            } else {
                let failed_ids: Vec<&str> = report
                    .steps
                    .iter()
                    .filter(|s| s.status == StepStatus::Failed)
                    .map(|s| s.id.as_str())
                    .collect();
                format!(
                    "FAIL: pipeline '{}' failed on [{}] ({} passed, {} failed, {} skipped).",
                    report.pipeline,
                    failed_ids.join(", "),
                    report.passed_steps,
                    report.failed_steps,
                    report.skipped_steps
                )
            };

            let payload = serde_json::json!({
                "schema_version": report.schema_version,
                "command": report.command,
                "ok": report.ok,
                "dry_run": report.dry_run,
                "run_id": report.run_id,
                "pipeline": report.pipeline,
                "summary": summary,
                "passed_steps": report.passed_steps,
                "failed_steps": report.failed_steps,
                "skipped_steps": report.skipped_steps,
                "elapsed_ms": report.elapsed_ms,
                "audit_log_path": report.audit_log_path,
                "steps": report.steps,
            });
            let json = serde_json::to_string(&payload)
                .map_err(|e| RunKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{json}");
            Ok(())
        }
        ResolvedFormat::Json => {
            let json = serde_json::to_string_pretty(report)
                .map_err(|e| RunKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{json}");
            Ok(())
        }
    }
}

pub fn emit_plan_report(report: &PlanReport, format: ResolvedFormat, color: bool) -> Result<()> {
    match format {
        ResolvedFormat::Human => {
            println!(
                "Plan for '{}' — {} step(s) across {} parallel wave(s):\n",
                report.pipeline, report.total_steps, report.wave_count
            );
            for w in &report.waves {
                if color {
                    println!("  {}", format!("Wave {}", w.wave).cyan().bold());
                } else {
                    println!("  Wave {}", w.wave);
                }
                for s in &w.steps {
                    let deps = if s.depends_on.is_empty() {
                        String::new()
                    } else {
                        format!("  (after: {})", s.depends_on.join(", "))
                    };
                    println!("    - {} [{:?}]{deps}", s.id, s.kind);
                    println!("      cmd: {}", s.command);
                }
            }
            Ok(())
        }
        ResolvedFormat::Agent | ResolvedFormat::Json => {
            let json = serde_json::to_string_pretty(report)
                .map_err(|e| RunKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{json}");
            Ok(())
        }
    }
}

pub fn emit_history(
    run_dir: &Path,
    runs: &[PipelineRunReport],
    format: ResolvedFormat,
    color: bool,
) -> Result<()> {
    match format {
        ResolvedFormat::Human => {
            println!(
                "Run history ({}) — showing {} run(s):\n",
                run_dir.display(),
                runs.len()
            );
            for r in runs {
                let st = if r.ok {
                    if color {
                        "PASS".green().bold().to_string()
                    } else {
                        "PASS".to_string()
                    }
                } else if color {
                    "FAIL".red().bold().to_string()
                } else {
                    "FAIL".to_string()
                };
                println!(
                    "  {st}  {}  {}  ({}/{}/{} pass/fail/skip, {}ms)",
                    r.run_id,
                    r.pipeline,
                    r.passed_steps,
                    r.failed_steps,
                    r.skipped_steps,
                    r.elapsed_ms
                );
            }
            Ok(())
        }
        ResolvedFormat::Agent | ResolvedFormat::Json => {
            let json = serde_json::to_string_pretty(&serde_json::json!({
                "schema_version": "1.0",
                "command": "history",
                "run_dir": run_dir.display().to_string(),
                "count": runs.len(),
                "runs": runs,
            }))
            .map_err(|e| RunKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{json}");
            Ok(())
        }
    }
}

pub fn emit_skill_installed(
    installed_path: &Path,
    format: ResolvedFormat,
    color: bool,
) -> Result<()> {
    match format {
        ResolvedFormat::Human => {
            let badge = if color {
                " INSTALLED ".on_green().black().bold().to_string()
            } else {
                "[INSTALLED]".to_string()
            };
            println!(
                "{badge} Agent skill written to {}",
                installed_path.display()
            );
            Ok(())
        }
        ResolvedFormat::Agent | ResolvedFormat::Json => {
            let json = serde_json::to_string(&serde_json::json!({
                "schema_version": "1.0",
                "command": "skill",
                "ok": true,
                "installed_path": installed_path.display().to_string(),
            }))
            .map_err(|e| RunKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{json}");
            Ok(())
        }
    }
}

pub fn emit_error(err: &RunKeelError, format: ResolvedFormat, color: bool) {
    match format {
        ResolvedFormat::Human => {
            let badge = if color {
                " ERROR ".on_red().white().bold().to_string()
            } else {
                "[ERROR]".to_string()
            };
            eprintln!("{badge} {}: {err}", err.code());
            if let Some(hint) = err.fix_hint() {
                eprintln!("  hint: {hint}");
            }
        }
        ResolvedFormat::Agent | ResolvedFormat::Json => {
            let env = err.to_envelope();
            if let Ok(json) = serde_json::to_string_pretty(&env) {
                eprintln!("{json}");
            } else {
                eprintln!("{{\"ok\":false,\"error\":{{\"code\":\"{}\"}}}}", err.code());
            }
        }
    }
}
