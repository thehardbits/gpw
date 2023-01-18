use clap::Parser;

/// Process global world population (GPW) asc files into serialized H3
/// cell/value pairs.
#[derive(Parser, Debug)]
pub struct Args {
    /// Intermediate H3 resolution.
    #[arg(short, long, default_value_t = 10)]
    pub res: u8,
    /// Input GPW ASCII file.
    pub src: std::path::PathBuf,
    /// Output directory.
    pub out: std::path::PathBuf,
}
