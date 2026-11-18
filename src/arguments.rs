use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "nexora-analysis-worker")]
pub struct Arguments {
    #[arg(long, default_value = ".nexora/local/connections.json")]
    pub connections: PathBuf,
    #[arg(long, default_value = "nexora_platform")]
    pub schema: String,
    #[arg(long, default_value = ".nexora/local/analysis-worker.json")]
    pub settings: PathBuf,
    #[arg(long)]
    pub git_policy: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    pub once: bool,
    #[command(subcommand)]
    pub command: Option<WorkerCommand>,
}

#[derive(Debug, Subcommand)]
pub enum WorkerCommand {
    /// Process one repository job and exit.
    Once,
}
