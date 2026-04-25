use anyhow::Result;
use clap::{Parser, Subcommand};
use okx_lob::delta;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "okx-delta",
    about = "Encode/decode single OKX L2 Parquet files for compact transfer"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Encode one final Parquet file into the temporary transfer format.
    Encode {
        input: PathBuf,
        #[arg(short, long)]
        output: PathBuf,
        #[arg(long, default_value_t = 1_000_000)]
        scale: i64,
        #[arg(long = "zstd-level", default_value_t = 19)]
        zstd_level: i32,
    },
    /// Decode one transfer file back to the final Parquet format.
    Decode {
        input: PathBuf,
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Read transfer metadata.
    Inspect { input: PathBuf },
    /// Verify that a transfer file decodes to the same cells as a Parquet file.
    Verify { parquet: PathBuf, encoded: PathBuf },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let report = match cli.command {
        Command::Encode {
            input,
            output,
            scale,
            zstd_level,
        } => delta::encode_file(&input, &output, scale, zstd_level)?,
        Command::Decode { input, output } => delta::decode_file(&input, &output)?,
        Command::Inspect { input } => delta::inspect_file(&input)?,
        Command::Verify { parquet, encoded } => delta::verify_file(&parquet, &encoded)?,
    };

    println!(
        "rows={} depth={} scale={}",
        report.rows, report.depth, report.scale
    );
    Ok(())
}
