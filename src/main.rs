use clap::Parser;
use runkeel::cli::{Cli, Commands};
use runkeel::error::{EXIT_ERROR, EXIT_OK, EXIT_STEP_FAILED, Result};
use runkeel::manifest::PipelineManifest;
use runkeel::output::{self, ResolvedFormat};
use runkeel::runner::{self, ExecOptions};
use runkeel::skill;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let format = output::resolve_format(cli.format);
    let use_color = output::should_use_color(cli.no_color, format);

    match run(cli, format, use_color).await {
        Ok(code) => ExitCode::from(code as u8),
        Err(err) => {
            output::emit_error(&err, format, use_color);
            ExitCode::from(EXIT_ERROR as u8)
        }
    }
}

async fn run(cli: Cli, format: ResolvedFormat, use_color: bool) -> Result<i32> {
    match cli.command {
        Commands::Run {
            manifest: manifest_path,
            dry_run,
            only,
            from,
            max_parallel,
            fail_fast,
            run_dir,
            no_save,
        } => {
            let manifest = PipelineManifest::load(&manifest_path)?;
            let subset = manifest.select_subset(&only, from.as_deref())?;
            let base_dir = PipelineManifest::resolve_base_dir(&manifest_path);
            let concurrency = max_parallel.unwrap_or(manifest.pipeline.max_parallel).max(1);

            let report = runner::execute_pipeline(
                &manifest,
                &ExecOptions {
                    base_dir,
                    subset,
                    max_parallel: concurrency,
                    fail_fast,
                    dry_run,
                    run_dir,
                    save_audit: !no_save,
                },
            )
            .await?;

            output::emit_run_report(&report, format, use_color)?;
            if report.ok {
                Ok(EXIT_OK)
            } else {
                Ok(EXIT_STEP_FAILED)
            }
        }

        Commands::Plan {
            manifest: manifest_path,
            from,
            mermaid,
        } => {
            let manifest = PipelineManifest::load(&manifest_path)?;
            let subset = manifest.select_subset(&[], from.as_deref())?;
            if mermaid {
                print!("{}", manifest.render_mermaid(&subset));
            } else {
                let plan = manifest.build_plan_report(&subset)?;
                output::emit_plan_report(&plan, format, use_color)?;
            }
            Ok(EXIT_OK)
        }

        Commands::History { run_dir, limit } => {
            let runs = runner::load_history(&run_dir, limit)?;
            output::emit_history(&run_dir, &runs, format, use_color)?;
            Ok(EXIT_OK)
        }

        Commands::Skill { install } => {
            if let Some(dir) = install {
                let installed = skill::install_skill(&dir)?;
                output::emit_skill_installed(&installed, format, use_color)?;
            } else {
                print!("{}", skill::SKILL_MD);
            }
            Ok(EXIT_OK)
        }
    }
}
