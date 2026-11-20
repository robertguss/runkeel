# runkeel

**Blazing-fast, zero-daemon, agent-first (DX + AX) Rust DAG pipeline runner for the Keel Data Stack and shell/SQL workflows.**

Unifies the complete **Keel Data Stack** without Airflow, Dagster, or fragile bash scripts:
- **[`fetchkeel`](https://github.com/robertguss/fetchkeel)** — Extracts paginated REST APIs into JSONL
- **[`dapkeel`](https://github.com/robertguss/dapkeel)** — Extracts & replicates Instructure Canvas Data 2 (DAP) snapshots, incremental CDC, and ODCS contracts
- **[`datakeel`](https://github.com/robertguss/datakeel)** — Validates contracts, catches drift, splits clean vs. quarantined rows
- **[`loadkeel`](https://github.com/robertguss/loadkeel)** — Flattens JSON & bulk-loads Postgres via `COPY` + atomic upserts
- **[`sqlkeel`](https://github.com/robertguss/sqlkeel)** — Statically checks SQL semantic bugs, schema hallucinations, migrations & column lineage
- **`runkeel`** — Orchestrates the whole DAG in parallel waves with retries, `--from` resume, Mermaid diagrams, and JSON audit logs

## Quickstart

```bash
# 1. Validate the DAG & inspect parallel execution waves
runkeel plan examples/seminary-pipeline/runkeel.toml
runkeel plan examples/seminary-pipeline/runkeel.toml --mermaid

# 2. Dry-run without executing commands
runkeel run examples/seminary-pipeline/runkeel.toml --dry-run

# 3. Execute the full DAG concurrently & write a run audit log
runkeel run runkeel.toml

# 4. Resume from a failed step + all its downstream dependents
runkeel run runkeel.toml --from load_populi

# 5. Inspect recent persisted runs
runkeel history .keel/runs

# 6. Install the built-in AI Agent Skill
runkeel skill --install .agents/skills/runkeel
```

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).
