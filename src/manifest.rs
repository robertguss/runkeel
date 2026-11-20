use crate::error::{Result, RunKeelError};
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineManifest {
    pub pipeline: PipelineHeader,
    #[serde(default)]
    pub steps: Vec<StepConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineHeader {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default = "default_max_parallel")]
    pub max_parallel: usize,
    #[serde(default = "default_state_dir")]
    pub state_dir: String,
    #[serde(default)]
    pub default_retries: u32,
    #[serde(default = "default_retry_delay_ms")]
    pub default_retry_delay_ms: u64,
    #[serde(default)]
    pub env: IndexMap<String, String>,
}

fn default_max_parallel() -> usize {
    4
}

fn default_state_dir() -> String {
    ".keel/runs".to_string()
}

fn default_retry_delay_ms() -> u64 {
    500
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Fetch,
    Validate,
    Diff,
    Load,
    SqlCheck,
    SqlLineage,
    Shell,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StepConfig {
    pub id: String,
    pub kind: StepKind,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub workdir: Option<String>,
    #[serde(default)]
    pub env: IndexMap<String, String>,
    #[serde(default)]
    pub retries: Option<u32>,
    #[serde(default)]
    pub retry_delay_ms: Option<u64>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub continue_on_error: bool,

    // --- shell ---
    pub cmd: Option<String>,

    // --- fetch (fetchkeel) ---
    pub config: Option<String>,
    pub stream: Option<String>,
    pub output: Option<String>,
    pub output_dir: Option<String>,
    pub state: Option<String>,
    pub max_pages: Option<usize>,

    // --- validate / diff (datakeel) ---
    pub contract: Option<String>,
    pub data: Option<String>,
    #[serde(default)]
    pub refs: IndexMap<String, String>,
    pub clean: Option<String>,
    pub quarantine: Option<String>,
    pub max_quarantine_rate: Option<f64>,
    #[serde(default)]
    pub strict: bool,
    pub baseline: Option<String>,
    pub current: Option<String>,
    pub max_null_delta: Option<f64>,
    pub max_row_drop_pct: Option<f64>,

    // --- load (loadkeel) ---
    pub input: Option<String>,
    pub target: Option<String>,
    pub mode: Option<String>,
    #[serde(default)]
    pub key: Vec<String>,
    #[serde(default)]
    pub flatten: bool,
    #[serde(default)]
    pub auto_create: bool,
    #[serde(default)]
    pub evolve: bool,

    // --- sql_check / sql_lineage (sqlkeel) ---
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub schemas: Vec<String>,
    pub fail_on: Option<String>,
    pub column: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedCommand {
    pub program: String,
    pub args: Vec<String>,
    pub display: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlannedWave {
    pub wave: usize,
    pub steps: Vec<PlannedStep>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlannedStep {
    pub id: String,
    pub kind: StepKind,
    pub depends_on: Vec<String>,
    pub command: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanReport {
    pub schema_version: &'static str,
    pub command: &'static str,
    pub ok: bool,
    pub pipeline: String,
    pub total_steps: usize,
    pub wave_count: usize,
    pub waves: Vec<PlannedWave>,
}

impl PipelineManifest {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path).map_err(|source| RunKeelError::IoRead {
            path: path.to_path_buf(),
            source,
        })?;
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let src = path.display().to_string();

        let manifest: Self = match ext.as_str() {
            "toml" => toml::from_str(&content).map_err(|e| RunKeelError::ManifestParse {
                path: src.clone(),
                detail: e.to_string(),
            })?,
            "yaml" | "yml" => {
                serde_yaml_ng::from_str(&content).map_err(|e| RunKeelError::ManifestParse {
                    path: src.clone(),
                    detail: e.to_string(),
                })?
            }
            _ => {
                return Err(RunKeelError::ManifestParse {
                    path: src,
                    detail: format!(
                        "unsupported manifest extension '.{ext}' (expected .toml, .yaml, or .yml)"
                    ),
                });
            }
        };

        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        if self.steps.is_empty() {
            return Err(RunKeelError::ManifestParse {
                path: self.pipeline.name.clone(),
                detail: "pipeline must define at least one step in `[[steps]]`".to_string(),
            });
        }

        let mut ids: HashSet<&str> = HashSet::new();
        for s in &self.steps {
            if !ids.insert(s.id.as_str()) {
                return Err(RunKeelError::DuplicateStep { id: s.id.clone() });
            }
            // Validate step command builds cleanly
            let _ = s.build_command()?;
        }

        for s in &self.steps {
            for dep in &s.depends_on {
                if !ids.contains(dep.as_str()) {
                    return Err(RunKeelError::UnknownDependency {
                        step: s.id.clone(),
                        dep: dep.clone(),
                    });
                }
            }
        }

        // Cycle check via topological waves
        let _ = self.compute_waves(&self.all_step_ids())?;
        Ok(())
    }

    pub fn all_step_ids(&self) -> IndexSet<String> {
        self.steps.iter().map(|s| s.id.clone()).collect()
    }

    pub fn select_subset(
        &self,
        only: &[String],
        from: Option<&str>,
    ) -> Result<IndexSet<String>> {
        let all = self.all_step_ids();
        let available = || {
            self.steps
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };

        if !only.is_empty() {
            let mut set = IndexSet::new();
            for id in only {
                let id = id.trim();
                if !all.contains(id) {
                    return Err(RunKeelError::UnknownStep {
                        step: id.to_string(),
                        available: available(),
                    });
                }
                set.insert(id.to_string());
            }
            return Ok(set);
        }

        if let Some(start) = from {
            if !all.contains(start) {
                return Err(RunKeelError::UnknownStep {
                    step: start.to_string(),
                    available: available(),
                });
            }
            // BFS downstream from `start`
            let mut reverse_adj: HashMap<&str, Vec<&str>> = HashMap::new();
            for s in &self.steps {
                for dep in &s.depends_on {
                    reverse_adj
                        .entry(dep.as_str())
                        .or_default()
                        .push(s.id.as_str());
                }
            }
            let mut selected = IndexSet::new();
            let mut q = VecDeque::new();
            selected.insert(start.to_string());
            q.push_back(start);
            while let Some(curr) = q.pop_front() {
                if let Some(children) = reverse_adj.get(curr) {
                    for &child in children {
                        if selected.insert(child.to_string()) {
                            q.push_back(child);
                        }
                    }
                }
            }
            return Ok(selected);
        }

        Ok(all)
    }

    /// Compute topological execution waves (all steps in wave `k` can run concurrently).
    pub fn compute_waves(&self, subset: &IndexSet<String>) -> Result<Vec<Vec<&StepConfig>>> {
        let step_map: HashMap<&str, &StepConfig> = self
            .steps
            .iter()
            .filter(|s| subset.contains(&s.id))
            .map(|s| (s.id.as_str(), s))
            .collect();

        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

        for (&id, &step) in &step_map {
            let mut deg = 0usize;
            for dep in &step.depends_on {
                if step_map.contains_key(dep.as_str()) {
                    deg += 1;
                    dependents.entry(dep.as_str()).or_default().push(id);
                }
            }
            in_degree.insert(id, deg);
        }

        let mut waves: Vec<Vec<&StepConfig>> = Vec::new();
        let mut scheduled = 0usize;

        while scheduled < step_map.len() {
            let mut ready: Vec<&str> = in_degree
                .iter()
                .filter(|(_, deg)| **deg == 0)
                .map(|(&id, _)| id)
                .collect();
            ready.sort();

            if ready.is_empty() {
                let mut stuck: Vec<&str> = in_degree.keys().copied().collect();
                stuck.sort();
                return Err(RunKeelError::CycleDetected {
                    nodes: stuck.join(" -> "),
                });
            }

            let mut wave_steps = Vec::with_capacity(ready.len());
            for id in &ready {
                in_degree.remove(id);
                wave_steps.push(step_map[id]);
                scheduled += 1;
            }
            for id in &ready {
                if let Some(children) = dependents.get(id) {
                    for &child in children {
                        if let Some(deg) = in_degree.get_mut(child) {
                            *deg = deg.saturating_sub(1);
                        }
                    }
                }
            }
            waves.push(wave_steps);
        }

        Ok(waves)
    }

    pub fn build_plan_report(&self, subset: &IndexSet<String>) -> Result<PlanReport> {
        let waves_raw = self.compute_waves(subset)?;
        let mut waves = Vec::with_capacity(waves_raw.len());
        let mut total_steps = 0usize;

        for (idx, w) in waves_raw.into_iter().enumerate() {
            let mut planned = Vec::with_capacity(w.len());
            for s in w {
                let cmd = s.build_command()?;
                let deps = s
                    .depends_on
                    .iter()
                    .filter(|d| subset.contains(*d))
                    .cloned()
                    .collect();
                planned.push(PlannedStep {
                    id: s.id.clone(),
                    kind: s.kind,
                    depends_on: deps,
                    command: cmd.display,
                });
                total_steps += 1;
            }
            waves.push(PlannedWave {
                wave: idx + 1,
                steps: planned,
            });
        }

        Ok(PlanReport {
            schema_version: "1.0",
            command: "plan",
            ok: true,
            pipeline: self.pipeline.name.clone(),
            total_steps,
            wave_count: waves.len(),
            waves,
        })
    }

    pub fn render_mermaid(&self, subset: &IndexSet<String>) -> String {
        let mut out = String::from("flowchart LR\n");
        for s in &self.steps {
            if !subset.contains(&s.id) {
                continue;
            }
            let nid = mermaid_id(&s.id);
            out.push_str(&format!("    {nid}[\"{} ({:?})\"]\n", s.id, s.kind));
        }
        for s in &self.steps {
            if !subset.contains(&s.id) {
                continue;
            }
            for dep in &s.depends_on {
                if subset.contains(dep) {
                    out.push_str(&format!("    {} --> {}\n", mermaid_id(dep), mermaid_id(&s.id)));
                }
            }
        }
        out
    }

    pub fn resolve_base_dir(manifest_path: &Path) -> PathBuf {
        manifest_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    }
}

impl StepConfig {
    pub fn build_command(&self) -> Result<ResolvedCommand> {
        let req = |opt: &Option<String>, field: &str| -> Result<String> {
            opt.clone().ok_or_else(|| RunKeelError::InvalidStep {
                step: self.id.clone(),
                detail: format!("step kind `{:?}` requires field `{field}`", self.kind),
            })
        };

        let (program, args): (String, Vec<String>) = match self.kind {
            StepKind::Shell => {
                let cmd = req(&self.cmd, "cmd")?;
                ("sh".to_string(), vec!["-c".to_string(), cmd])
            }
            StepKind::Fetch => {
                let mut a = vec!["run".to_string(), req(&self.config, "config")?];
                if let Some(ref s) = self.stream {
                    a.extend(["--stream".to_string(), s.clone()]);
                }
                if let Some(ref o) = self.output {
                    a.extend(["-o".to_string(), o.clone()]);
                }
                if let Some(ref od) = self.output_dir {
                    a.extend(["--output-dir".to_string(), od.clone()]);
                }
                if let Some(ref st) = self.state {
                    a.extend(["--state".to_string(), st.clone()]);
                }
                if let Some(mp) = self.max_pages {
                    a.extend(["--max-pages".to_string(), mp.to_string()]);
                }
                a.extend(["--format".to_string(), "agent".to_string()]);
                ("fetchkeel".to_string(), a)
            }
            StepKind::Validate => {
                let mut a = vec![
                    "check".to_string(),
                    req(&self.contract, "contract")?,
                    req(&self.data, "data")?,
                ];
                for (rk, rv) in &self.refs {
                    a.extend(["--ref".to_string(), format!("{rk}={rv}")]);
                }
                if let Some(ref c) = self.clean {
                    a.extend(["--clean".to_string(), c.clone()]);
                }
                if let Some(ref q) = self.quarantine {
                    a.extend(["--quarantine".to_string(), q.clone()]);
                }
                if let Some(r) = self.max_quarantine_rate {
                    a.extend(["--max-quarantine-rate".to_string(), r.to_string()]);
                }
                if self.strict {
                    a.push("--strict".to_string());
                }
                a.extend(["--format".to_string(), "agent".to_string()]);
                ("datakeel".to_string(), a)
            }
            StepKind::Diff => {
                let mut a = vec![
                    "diff".to_string(),
                    req(&self.baseline, "baseline")?,
                    req(&self.current, "current")?,
                ];
                if let Some(d) = self.max_null_delta {
                    a.extend(["--max-null-delta".to_string(), d.to_string()]);
                }
                if let Some(d) = self.max_row_drop_pct {
                    a.extend(["--max-row-drop-pct".to_string(), d.to_string()]);
                }
                a.extend(["--format".to_string(), "agent".to_string()]);
                ("datakeel".to_string(), a)
            }
            StepKind::Load => {
                let mut a = vec![
                    "load".to_string(),
                    req(&self.input, "input")?,
                    req(&self.target, "target")?,
                ];
                if let Some(ref c) = self.contract {
                    a.extend(["--contract".to_string(), c.clone()]);
                }
                if let Some(ref m) = self.mode {
                    a.extend(["--mode".to_string(), m.clone()]);
                }
                if !self.key.is_empty() {
                    a.extend(["--key".to_string(), self.key.join(",")]);
                }
                if self.flatten {
                    a.push("--flatten".to_string());
                }
                if self.auto_create {
                    a.push("--auto-create".to_string());
                }
                if self.evolve {
                    a.push("--evolve".to_string());
                }
                a.extend(["--format".to_string(), "agent".to_string()]);
                ("loadkeel".to_string(), a)
            }
            StepKind::SqlCheck => {
                if self.paths.is_empty() {
                    return Err(RunKeelError::InvalidStep {
                        step: self.id.clone(),
                        detail: "step kind `SqlCheck` requires non-empty `paths`".to_string(),
                    });
                }
                let mut a = vec!["check".to_string()];
                a.extend(self.paths.clone());
                for s in &self.schemas {
                    a.extend(["--schema".to_string(), s.clone()]);
                }
                if let Some(ref fo) = self.fail_on {
                    a.extend(["--fail-on".to_string(), fo.clone()]);
                }
                a.extend(["--format".to_string(), "agent".to_string()]);
                ("sqlkeel".to_string(), a)
            }
            StepKind::SqlLineage => {
                if self.paths.is_empty() {
                    return Err(RunKeelError::InvalidStep {
                        step: self.id.clone(),
                        detail: "step kind `SqlLineage` requires non-empty `paths`".to_string(),
                    });
                }
                let mut a = vec!["lineage".to_string()];
                a.extend(self.paths.clone());
                for s in &self.schemas {
                    a.extend(["--schema".to_string(), s.clone()]);
                }
                if let Some(ref c) = self.column {
                    a.extend(["--column".to_string(), c.clone()]);
                }
                a.extend(["--format".to_string(), "agent".to_string()]);
                ("sqlkeel".to_string(), a)
            }
        };

        let display = if program == "sh" && args.len() == 2 && args[0] == "-c" {
            args[1].clone()
        } else {
            format!("{} {}", program, args.join(" "))
        };

        Ok(ResolvedCommand {
            program,
            args,
            display,
        })
    }
}

fn mermaid_id(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}
