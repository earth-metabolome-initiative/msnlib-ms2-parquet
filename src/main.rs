mod build;
mod download;
mod line_source;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Zenodo record id of MSnLib v8.
pub const MSNLIB_V8_RECORD_ID: u64 = 21105617;
/// File-name suffix of the MS2 spectral libraries in MSnLib.
pub const MS2_SUFFIX: &str = "ms2.mgf";

/// Builds a MassSpecGym-style Parquet dataset from the MSnLib v8 MS2 spectra.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download the `*ms2.mgf` files of MSnLib v8 from Zenodo.
    Download {
        /// Directory the MGF files are written to.
        #[arg(long, default_value = "downloads")]
        dir: PathBuf,
        /// Download again even if a file of the expected size is present.
        #[arg(long)]
        force: bool,
    },
    /// Parse the downloaded MGF files and write the Parquet dataset.
    Build {
        /// Directory containing the downloaded `*ms2.mgf` files.
        #[arg(long, default_value = "downloads")]
        input_dir: PathBuf,
        /// Output Parquet file.
        #[arg(long, default_value = "output/msnlib_ms2.parquet")]
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    match Cli::parse().command {
        Command::Download { dir, force } => download::run(&dir, force).await,
        Command::Build { input_dir, output } => build::run(&input_dir, &output),
    }
}
