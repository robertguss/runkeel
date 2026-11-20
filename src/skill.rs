use crate::error::{Result, RunKeelError};
use std::fs;
use std::path::{Path, PathBuf};

pub const SKILL_MD: &str = r#"---
name: runkeel
description: Orchestrate declarative multi-step ETL DAGs (fetchkeel, datakeel, loadkeel, sqlkeel, and custom shell/SQL steps) in parallel with cycle detection, retries, --from resume, Mermaid DAG rendering, and persisted JSON audit logs. Use instead of brittle bash scripts or heavyweight Airflow/Dagster daemons.
---

# RunKeel Agent Manual

## Identity & Scope

`runkeel` is the zero-daemon Rust DAG runner for the **Keel Data Stack** (`fetchkeel` → `datakeel` → `loadkeel` → `sqlkeel`). It validates your `runkeel.toml` / `.yaml` pipeline graph, executes independent branches concurrently, parses structured JSON summaries from child Keel tools, and persists per-run audit logs to `.keel/runs/`.

## Output & Exit Code Contract

- **TTY-aware dual mode:** Compact single-line JSON (`--format agent`) when piped/non-TTY; human summary in TTY.
- **Strict stream separation:** `stdout` = pure report/plan/mermaid; `stderr` = structured JSON `ErrorEnvelope` on config/DAG errors.
- **Exit codes:**
  - `0` — Pipeline / plan succeeded
  - `1` — One or more pipeline steps failed (inspect `steps[].status`, `steps[].error`, and `audit_log_path`)
  - `2` — Manifest parse error, duplicate step, unknown dependency, or cycle (`RK_CYCLE_DETECTED`)

## Core Command Matrix

| Goal | Command |
| :--- | :--- |
| Validate DAG & inspect parallel execution waves | `runkeel plan runkeel.toml --format agent` |
| Render Mermaid DAG diagram for docs/PRs | `runkeel plan runkeel.toml --mermaid` |
| Dry-run pipeline without executing commands | `runkeel run runkeel.toml --dry-run --format agent` |
| Run full DAG with parallel waves & audit log | `runkeel run runkeel.toml --format agent` |
| Resume from a failed step + its downstream nodes | `runkeel run runkeel.toml --from load_populi --format agent` |
| Run a single isolated step | `runkeel run runkeel.toml --only verify_sql --format agent` |
| Inspect recent run audit logs | `runkeel history .keel/runs --limit 10 --format agent` |

## Agent Decision Tree

1. **After editing `runkeel.toml`:** Always run `runkeel plan runkeel.toml --format agent` to catch cycles (`RK_CYCLE_DETECTED`) or typos in `depends_on` (`RK_UNKNOWN_DEPENDENCY`) before executing.
2. **If `runkeel run` exits `1`:** Inspect `steps` where `status == "failed"`, fix the underlying issue (bad contract, SQL bug, or API issue), then resume cleanly with `runkeel run runkeel.toml --from <failed_step_id>`.
"#;

pub fn install_skill(target: &Path) -> Result<PathBuf> {
    let dest = if target
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("md"))
    {
        target.to_path_buf()
    } else {
        target.join("SKILL.md")
    };

    if let Some(parent) = dest.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| RunKeelError::IoWrite {
            path: dest.clone(),
            source,
        })?;
    }

    fs::write(&dest, SKILL_MD).map_err(|source| RunKeelError::IoWrite {
        path: dest.clone(),
        source,
    })?;

    Ok(dest)
}
