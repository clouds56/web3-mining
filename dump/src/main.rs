#[macro_use] extern crate tracing;

pub mod rpc;
pub mod metrics;

use std::{path::Path, str::FromStr as _};

use anyhow::Result;
use ethers_providers::{JsonRpcClient, Middleware, Provider};
use polars::io::{parquet::{ParquetReader, ParquetWriter}, SerReader};

async fn get_block_number<P: JsonRpcClient>(client: &Provider<P>) -> Result<u64> {
  let block_number = client.get_block_number().await?;
  Ok(block_number.as_u64())
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Stage {
  _cut: Option<u64>,
  #[serde(default)]
  block_metrics: u64,
}

const DEFAULT_CUT: u64 = 1000000;
/// A cut means 0..CUT, CUT..2*CUT, etc.
/// which means block number 10000 is in a new file.
/// just like what reth do.
const fn next_cut(i: u64, cut: u64) -> u64 {
  i / cut * cut + cut
}

async fn run<M: Middleware, P: AsRef<Path>>(client: &M, data_dir: P, mut start: u64, end: u64, stage: &mut Stage) -> Result<()> where M::Error: 'static {
  let cut = stage._cut.unwrap_or(DEFAULT_CUT);
  while start < end {
    let checkpoint = next_cut(start, cut).min(end);
    info!(start, end, "running for {}..{}", start, checkpoint);
    if start < checkpoint {
      let tmp_filename = data_dir.as_ref().join(format!("block_metrics_{}.{}.parquet.tmp", cut, start/cut));
      let mut df = metrics::fetch::fetch_blocks(client, start, checkpoint).await?;
      if start % cut != 0 {
        let old_file = std::fs::File::open(tmp_filename.with_extension(""))?;
        let old_df = ParquetReader::new(old_file).finish()?;
        df = old_df.vstack(&df)?;
      }
      assert_eq!(df.shape().0 as u64, checkpoint - start + start % cut);
      let file = std::fs::File::create(&tmp_filename)?;
      ParquetWriter::new(file).finish(&mut df)?;
      std::fs::rename(&tmp_filename, tmp_filename.with_extension(""))?;
    }
    stage.block_metrics = checkpoint;
    start = checkpoint;
  }
  Ok(())
}

fn load_stage<P: AsRef<Path>>(data_dir: P) -> Result<Stage> {
  let filename = data_dir.as_ref().join("stage.toml");
  match std::fs::read_to_string(&filename) {
    Ok(content) => Ok(toml::from_str(&content)?),
    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
      info!(stage_file=%filename.display(), "stage file not found, using default");
      Ok(Stage::default())
    }
    Err(e) => return Err(e)?,
  }
}

fn save_stage<P: AsRef<Path>>(data_dir: P, stage: &Stage) -> Result<()> {
  let filename = data_dir.as_ref().join("stage.toml.tmp");
  let content = toml::to_string(stage)?;
  std::fs::write(&filename, content)?;
  std::fs::rename(&filename, filename.with_extension(""))?;
  Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
  dotenvy::dotenv().ok();
  tracing_subscriber::fmt::fmt().with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE).init();
  info!(cwd=%std::env::current_dir().unwrap().display());
  let endpoint = format!("http://{}", std::env::var("RETH_HTTP_RPC").as_deref().unwrap_or("127.0.0.1:8545"));
  let data_dir = std::env::var("DATA_DIR").unwrap_or("data".to_string());
  std::fs::create_dir_all(&data_dir)?;
  let client = Provider::new(ethers_providers::Http::from_str(&endpoint)?);
  let block_length = get_block_number(&client).await?;
  info!(block_length, "hello");

  let mut stage = load_stage(&data_dir)?;
  let cut = stage._cut.unwrap_or(DEFAULT_CUT);
  stage._cut = Some(cut);
  info!(?stage);

  // let magic_number = 98672723;
  // let block_length = magic_number * (stage.block_metrics + 1) % (10 * DEFAULT_CUT) + stage.block_metrics;
  // info!(block_length, "faking");
  run(&client, &data_dir, stage.block_metrics, block_length+1, &mut stage).await?;

  save_stage(&data_dir, &stage)?;
  Ok(())
}
