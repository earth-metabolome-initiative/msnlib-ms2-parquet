mod build;
mod download;
mod line_source;
mod publish;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Zenodo record id of MSnLib v8.
pub const MSNLIB_V8_RECORD_ID: u64 = 21105617;
/// DOI of MSnLib v8.
pub const MSNLIB_V8_DOI: &str = "10.5281/zenodo.21105617";
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
    /// Publish the Parquet dataset to Zenodo, as a new record or a new version of one.
    Publish {
        /// Parquet file to upload.
        #[arg(long, default_value = "output/msnlib_ms2.parquet")]
        file: PathBuf,
        /// Publish to sandbox.zenodo.org using ZENODO_SANDBOX_TOKEN.
        #[arg(long)]
        sandbox: bool,
        /// Print the deposit metadata and the planned upload without contacting Zenodo.
        #[arg(long)]
        dry_run: bool,
        /// Publish as a new version of this deposition id; its files are replaced by `--file`.
        #[arg(long, value_name = "DEPOSITION_ID", requires = "dataset_version")]
        new_version: Option<u64>,
        /// Version label in the record metadata, e.g. `v2` [default: v1; required with --new-version].
        #[arg(long, value_name = "LABEL")]
        dataset_version: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    match Cli::parse().command {
        Command::Download { dir, force } => download::run(&dir, force).await,
        Command::Build { input_dir, output } => build::run(&input_dir, &output),
        Command::Publish {
            file,
            sandbox,
            dry_run,
            new_version,
            dataset_version,
        } => {
            let dataset_version = dataset_version.as_deref().unwrap_or("v1");
            publish::run(&file, sandbox, dry_run, new_version, dataset_version).await
        }
    }
}
