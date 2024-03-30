#[macro_use] extern crate tracing;

pub mod rpc;
pub mod metrics;

use std::{path::Path, str::FromStr as _};

use anyhow::Result;
use ethers_providers::{JsonRpcClient, Middleware, Provider};
use futures::Future;
use polars::{frame::DataFrame, io::{parquet::{ParquetReader, ParquetWriter}, SerReader}};
use tracing_subscriber::fmt::format::FmtSpan;

async fn get_block_number<P: JsonRpcClient>(client: &Provider<P>) -> Result<u64> {
  let block_number = client.get_block_number().await?;
  Ok(block_number.as_u64())
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Stage {
  _cut: Option<u64>,
  #[serde(default)]
  block_metrics: u64,
  #[serde(default)]
  uniswap_factory: u64,
}

const DEFAULT_CUT: u64 = 1000000;
/// A cut means 0..CUT, CUT..2*CUT, etc.
/// which means block number 10000 is in a new file.
/// just like what reth do.
const fn next_cut(i: u64, cut: u64) -> u64 {
  i / cut * cut + cut
}

pub trait Executor {
  fn run(&self, start: u64, end: u64) -> impl Future<Output = Result<DataFrame>>;
}
#[allow(refining_impl_trait)]
impl<Fut: Future<Output = Result<DataFrame>>, F: Fn(u64, u64) -> Fut > Executor for F {
  fn run(&self, start: u64, end: u64) -> Fut {
    self(start, end)
  }
}

pub trait EventListener<E> {
  fn on_event(&mut self, event: E) -> bool;
}
impl<E> EventListener<E> for () {
  fn on_event(&mut self, _: E) -> bool {true}
}
impl<F: FnMut(E), E> EventListener<E> for F {
  fn on_event(&mut self, event: E) -> bool {
    self(event);
    true
  }
}

pub struct RunConfig<'a, Fn: Executor> {
  data_dir: &'a Path,
  start: u64,
  end: u64,
  cut: u64,
  name: &'a str,
  executor: &'a Fn,
}

#[allow(unused)]
pub struct RunEvent {
  start: u64,
  checkpoint: u64,
  len: u64,
  cut: u64,
  end: u64,
}

macro_rules! is_break {
  ($expr:expr) => {
    if !$expr {
      anyhow::bail!("break");
    }
  };
}

impl<'a, Fn: Executor> RunConfig<'a, Fn> {
  async fn run(self, mut tracker: impl EventListener<RunEvent>) -> Result<()> {
    let config = self;
    let mut start = config.start;
    let end = config.end;
    let cut = config.cut;
    is_break!(tracker.on_event(RunEvent { start, checkpoint: start, len: 0, cut, end }));
    while start < end {
      let checkpoint = next_cut(start, cut).min(end);
      info!(config.start, config.end, config.name, "running for {}..{}", start, checkpoint);
      if start < checkpoint {
        let tmp_filename = config.data_dir.join(format!("{}_{}.{}.parquet.tmp", config.name, cut, start/cut));
        // metrics::block::fetch_blocks(client, start, checkpoint).await?;
        let mut df = config.executor.run(start, checkpoint).await?;
        if start % cut != 0 {
          let old_file = std::fs::File::open(tmp_filename.with_extension(""))?;
          let old_df = ParquetReader::new(old_file).finish()?;
          df = old_df.vstack(&df)?;
        }
        let file = std::fs::File::create(&tmp_filename)?;
        ParquetWriter::new(file).finish(&mut df)?;
        is_break!(tracker.on_event(RunEvent { start, checkpoint, len: df.shape().0 as u64, cut, end }));
        std::fs::rename(&tmp_filename, tmp_filename.with_extension(""))?;
      }
      start = checkpoint;
    }
    Ok(())
  }
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
  tracing_subscriber::fmt::fmt()
    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
    .init();
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
  RunConfig {
    data_dir: data_dir.as_ref(),
    start: stage.block_metrics,
    end: block_length,
    cut,
    name: "block_metrics",
    executor: &|start, end| metrics::block::fetch_blocks(&client, start, end),
  }.run(|e: RunEvent| {
    assert_eq!(Some(e.cut), stage._cut);
    if e.len > 0 {
      assert_eq!(e.len, e.checkpoint - e.start + e.start % cut);
    }
    stage.block_metrics = e.checkpoint
  }).await?;
  // run(&client, &data_dir, stage.block_metrics, block_length+1, &mut stage).await?;

  RunConfig {
    data_dir: data_dir.as_ref(),
    start: stage.uniswap_factory.max(9_000_000),
    end: block_length,
    cut,
    name: "uniswap_factory",
    executor: &|start, end| metrics::uniswap::fetch_uniswap(&client, start, end),
  }.run(|e: RunEvent| {
    stage.uniswap_factory = e.checkpoint;
  }).await?;

  save_stage(&data_dir, &stage)?;
  Ok(())
}
