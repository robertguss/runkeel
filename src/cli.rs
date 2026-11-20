use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "runkeel",
    version,
    about = "Blazing-fast, zero-daemon, agent-first (DX + AX) Rust DAG pipeline runner for the Keel Data Stack and shell/SQL workflows.",
    long_about = None
)]
pub struct Cli {
    /// Output format: auto (human in TTY, agent when piped), human, agent, or json
    #[arg(long, global = true, value_enum, default_value_t = OutputFormatArg::Auto)]
    pub format: OutputFormatArg,

    /// Disable ANSI colors in human output
    #[arg(long, global = true)]
    pub no_color: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormatArg {
    Auto,
    Human,
    Agent,
    Json,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Execute a declarative runkeel.toml / .yaml pipeline DAG with parallel waves & audit logging
    Run {
        /// Path to pipeline manifest (.toml, .yaml, or .yml)
        manifest: PathBuf,

        /// Preview DAG execution waves and exact commands without executing anything
        #[arg(long)]
        dry_run: bool,

        /// Run only specific step id(s) (comma-separated)
        #[arg(long, value_delimiter = ',')]
        only: Vec<String>,

        /// Resume pipeline starting from a specific step id and run all its downstream dependents
        #[arg(long)]
        from: Option<String>,

        /// Override maximum concurrent steps (defaults to manifest setting or 4)
        #[arg(long)]
        max_parallel: Option<usize>,

        /// Stop scheduling new steps immediately after the first failure
        #[arg(long)]
        fail_fast: bool,

        /// Directory for persisted run audit JSON logs (overrides manifest `state_dir`)
        #[arg(long)]
        run_dir: Option<PathBuf>,

        /// Disable writing run audit JSON log to disk
        #[arg(long)]
        no_save: bool,
    },

    /// Validate a pipeline DAG (cycle & dependency checks) and show parallel waves or Mermaid diagram
    Plan {
        /// Path to pipeline manifest (.toml, .yaml, or .yml)
        manifest: PathBuf,

        /// Optional `--from <step>` filter to preview a resume subgraph
        #[arg(long)]
        from: Option<String>,

        /// Output a Mermaid `flowchart LR` diagram of the pipeline DAG
        #[arg(long)]
        mermaid: bool,
    },

    /// Inspect recent persisted pipeline run audit logs
    History {
        /// Directory containing run audit JSON files (default: .keel/runs)
        #[arg(default_value = ".keel/runs")]
        run_dir: PathBuf,

        /// Maximum number of recent runs to show (default: 10)
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },

    /// Print or install the bundled SKILL.md agent manual (for Claude Code, Antigravity, Codex, Cursor)
    Skill {
        /// Install SKILL.md into a directory (e.g. .agents/skills/runkeel)
        #[arg(long)]
        install: Option<PathBuf>,
    },
}
