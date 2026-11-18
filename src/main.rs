use arguments::{Arguments, WorkerCommand};
use clap::Parser;
use nexora_analysis_worker::RepositoryWorkerHost;
use nexora_runtime::DatabaseConnections;
use std::process::ExitCode;

mod arguments;

#[tokio::main]
async fn main() -> ExitCode {
    match run(Arguments::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("nexora-analysis-worker: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(arguments: Arguments) -> Result<(), Box<dyn std::error::Error>> {
    let connections = DatabaseConnections::load(&arguments.connections)?;
    let once = arguments.once || matches!(arguments.command, Some(WorkerCommand::Once));
    RepositoryWorkerHost::run(
        connections,
        arguments.schema,
        arguments.settings,
        arguments.git_policy,
        std::env::var("NEXORA_WORKER_ID").ok(),
        once,
    )
    .await?;
    Ok(())
}
