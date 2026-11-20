use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

fn runkeel() -> Command {
    Command::cargo_bin("runkeel").expect("runkeel binary built")
}

fn example_manifest() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/seminary-pipeline/runkeel.toml")
}

#[test]
fn plan_computes_parallel_waves_mermaid_and_catches_cycles() {
    // 1. Plan on flagship seminary pipeline
    let out = runkeel()
        .args([
            "plan",
            example_manifest().to_str().unwrap(),
            "--format",
            "agent",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let plan: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(plan["ok"], true);
    assert_eq!(plan["total_steps"], 8);
    assert!(plan["wave_count"].as_u64().unwrap() >= 4);

    // 2. Mermaid output
    runkeel()
        .args(["plan", example_manifest().to_str().unwrap(), "--mermaid"])
        .assert()
        .success()
        .stdout(predicate::str::contains("flowchart LR"))
        .stdout(predicate::str::contains("load_canvas --> refresh_risk_views"));

    // 3. Cycle detection -> exit 2 + RK_CYCLE_DETECTED
    let dir = tempdir().unwrap();
    let cyclic = dir.path().join("cycle.toml");
    fs::write(
        &cyclic,
        r#"
[pipeline]
name = "cyclic"

[[steps]]
id = "a"
kind = "shell"
depends_on = ["b"]
cmd = "echo a"

[[steps]]
id = "b"
kind = "shell"
depends_on = ["a"]
cmd = "echo b"
"#,
    )
    .unwrap();

    let stderr = runkeel()
        .args(["plan", cyclic.to_str().unwrap(), "--format", "agent"])
        .assert()
        .code(2)
        .get_output()
        .stderr
        .clone();
    let env: Value = serde_json::from_slice(&stderr).unwrap();
    assert_eq!(env["error"]["code"], "RK_CYCLE_DETECTED");
}

#[test]
fn parallel_dag_execution_retries_failure_skipping_resume_and_history() {
    let dir = tempdir().unwrap();
    let run_dir = dir.path().join("runs");
    let marker = dir.path().join("retry_flag");
    let manifest = dir.path().join("pipe.toml");

    // Step `flaky` fails once then succeeds on retry!
    // Step `fail_branch` fails permanently -> its child `downstream_skipped` is Skipped.
    // Independent branch `good_a` -> `good_b` still succeeds!
    fs::write(
        &manifest,
        format!(
            r#"
[pipeline]
name = "dag-test"
max_parallel = 4

[[steps]]
id = "flaky"
kind = "shell"
retries = 1
retry_delay_ms = 10
cmd = "if [ -f '{marker}' ]; then echo 'recovered'; else touch '{marker}'; exit 1; fi"

[[steps]]
id = "good_b"
kind = "shell"
depends_on = ["flaky"]
cmd = "echo 'all good'"

[[steps]]
id = "fail_branch"
kind = "shell"
cmd = "echo 'boom' >&2; exit 1"

[[steps]]
id = "downstream_skipped"
kind = "shell"
depends_on = ["fail_branch"]
cmd = "echo 'should never run'"
"#,
            marker = marker.display()
        ),
    )
    .unwrap();

    let out = runkeel()
        .args([
            "run",
            manifest.to_str().unwrap(),
            "--run-dir",
            run_dir.to_str().unwrap(),
            "--format",
            "agent",
        ])
        .assert()
        .code(1)
        .get_output()
        .stdout
        .clone();

    let rep: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(rep["ok"], false);
    assert_eq!(rep["passed_steps"], 2);
    assert_eq!(rep["failed_steps"], 1);
    assert_eq!(rep["skipped_steps"], 1);

    // Check `--from good_b` runs only `good_b` and succeeds (exit 0)
    runkeel()
        .args([
            "run",
            manifest.to_str().unwrap(),
            "--from",
            "good_b",
            "--run-dir",
            run_dir.to_str().unwrap(),
            "--format",
            "agent",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"passed_steps\":1"));

    // History lists both persisted runs
    let hist = runkeel()
        .args(["history", run_dir.to_str().unwrap(), "--format", "agent"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let hval: Value = serde_json::from_slice(&hist).unwrap();
    assert_eq!(hval["count"], 2);
}

#[test]
fn skill_prints_and_installs() {
    let dir = tempdir().unwrap();
    let skill_dir = dir.path().join(".agents/skills/runkeel");
    runkeel()
        .args(["skill", "--install", skill_dir.to_str().unwrap()])
        .assert()
        .success();
    assert!(skill_dir.join("SKILL.md").exists());
}
