use clap::Parser;

/// Serve global word population via H3 cells.
#[derive(Parser)]
pub struct Cli {
    /// Path to serialized H3 (cell, population) pairs.
    pub path: std::path::PathBuf,
}
