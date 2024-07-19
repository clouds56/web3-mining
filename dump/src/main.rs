#[macro_use] extern crate tracing;

pub mod rpc;
pub mod metrics;
pub mod migration;

use std::{path::Path, str::FromStr as _, sync::{atomic::AtomicU64, Arc}};

use anyhow::Result;
use ethers_providers::{JsonRpcClient, Middleware, Provider};
use futures::Future;
use indexmap::IndexMap;
use migration::StageMigration;
use polars::{frame::DataFrame, prelude::{ParquetReader, ParquetWriter}, io::SerReader};
use tracing_subscriber::fmt::format::FmtSpan;

use crate::metrics::ToChecksumHex;

async fn get_block_number<P: JsonRpcClient>(client: &Provider<P>) -> Result<u64> {
  let block_number = client.get_block_number().await?;
  Ok(block_number.as_u64())
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct PairStage {
  pub contract: String,
  #[serde(alias="crated")]
  pub created: u64,
  #[serde(default, skip_serializing_if = "checkpoint_is_none")]
  pub checkpoint: Arc<AtomicU64>,
}
fn checkpoint_is_none(data: &AtomicU64) -> bool {
  data.load(std::sync::atomic::Ordering::SeqCst) == 0
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Stage {
  _cut: Option<u64>,
  #[serde(default)]
  block_metrics: u64,
  #[serde(default)]
  uniswap_factory_events: u64,
  #[serde(default)]
  uniswap3_factory_events: u64,
  #[serde(default)]
  uniswap_pair_events: IndexMap<String, PairStage>,
  #[serde(default)]
  uniswap3_pair_events: IndexMap<String, PairStage>,
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

pub struct DatasetName<'a> {
  name: &'a str,
  cut: u64,
  idx: usize,
}

impl<'a> DatasetName<'a> {
  pub fn new(name: &'a str, cut: u64, idx: usize) -> Self {
    Self { name, cut, idx }
  }
  pub fn filename(&self) -> String {
    format!("{}_{}.{}.parquet", self.name, self.cut, self.idx)
  }
  pub fn tmp_filename(&self) -> String {
    format!("{}.tmp", self.filename())
  }
  pub fn part_filename(&self) -> String {
    format!("{}.part", self.filename())
  }
  pub fn from_string(name: &'a str) -> Option<(Self, &'a str)> {
    let (name, rest) = if let Some(name) = name.strip_suffix(".tmp") {
      (name, ".tmp")
    } else if let Some(name) = name.strip_suffix(".part") {
      (name, ".part")
    } else {
      (name, "")
    };
    let name = name.strip_suffix(".parquet")?;
    let mut split = name.rsplitn(2, '.');
    let idx = split.next()?.parse().ok()?;
    let mut split = split.next()?.rsplitn(2, '_');
    let cut = split.next()?.parse().ok()?;
    let name = split.next()?;
    assert_eq!(split.next(), None);
    Some((Self { name, cut, idx }, rest))
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
  let mut stage: Stage = match std::fs::read_to_string(&filename) {
    Ok(content) => {
      let stage = toml::from_str::<Stage>(&content)?;
      let migration = toml::from_str::<StageMigration>(&content)?;
      migration::migrate(data_dir.as_ref(), stage, migration)?
    }
    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
      info!(stage_file=%filename.display(), "stage file not found, using default");
      Stage::default()
    }
    Err(e) => return Err(e)?,
  };

  stage.uniswap_pair_events.entry("usdc_weth".to_string()).or_insert_with(|| PairStage {
    contract: metrics::uniswap_v2::consts::CONTRACT_UniswapV2_USDC_WETH.to_checksum_hex(),
    created: 10_000_000,
    checkpoint: Arc::new(AtomicU64::new(0)),
  });
  stage.uniswap3_pair_events.entry("wbtc_weth".to_string()).or_insert_with(|| PairStage {
    contract: metrics::uniswap_v3::consts::CONTRACT_UniswapV3_WBTC_WETH.to_checksum_hex(),
    created: 12_000_000,
    checkpoint: Arc::new(AtomicU64::new(0)),
  });
  // save_stage(data_dir.as_ref(), &stage).ok();

  Ok(stage)
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
  let endpoint = format!("http://{}", std::env::var("RETH_HTTP_RPC").as_deref().unwrap_or("127.0.0.1:8545"));
  info!(cwd=%std::env::current_dir().unwrap().display(), endpoint);
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
    stage.block_metrics = e.checkpoint;
    save_stage(&data_dir, &stage).ok();
  }).await?;

  RunConfig {
    data_dir: data_dir.as_ref(),
    start: stage.uniswap_factory_events.max(9_000_000),
    end: block_length,
    cut,
    name: "uniswap_factory_events",
    executor: &|start, end| metrics::uniswap_v2::fetch_uniswap_factory(&client, start, end),
  }.run(|e: RunEvent| {
    stage.uniswap_factory_events = e.checkpoint;
    save_stage(&data_dir, &stage).ok();
  }).await?;

  RunConfig {
    data_dir: data_dir.as_ref(),
    start: stage.uniswap3_factory_events.max(11_000_000),
    end: block_length,
    cut,
    name: "uniswap3_factory_events",
    executor: &|start, end| metrics::uniswap_v3::fetch_factory(&client, start, end),
  }.run(|e: RunEvent| {
    stage.uniswap3_factory_events = e.checkpoint;
    save_stage(&data_dir, &stage).ok();
  }).await?;

  for (name, pair) in &stage.uniswap_pair_events {
    if checkpoint_is_none(&pair.checkpoint) {
      pair.checkpoint.store(pair.created / cut * cut, std::sync::atomic::Ordering::SeqCst);
    }
    let contract = pair.contract.parse().unwrap();
    RunConfig {
      data_dir: data_dir.as_ref(),
      start: pair.checkpoint.load(std::sync::atomic::Ordering::SeqCst),
      end: block_length,
      cut,
      name: &format!("uniswap_pair_events_{}", name),
      executor: &|start, end| metrics::uniswap_v2::fetch_uniswap_pair(&client, start, end, contract),
    }.run(|e: RunEvent| {
      pair.checkpoint.store(e.checkpoint, std::sync::atomic::Ordering::SeqCst);
      save_stage(&data_dir, &stage).ok();
    }).await?;
  }

  for (name, pair) in &stage.uniswap3_pair_events {
    if checkpoint_is_none(&pair.checkpoint) {
      pair.checkpoint.store(pair.created / cut * cut, std::sync::atomic::Ordering::SeqCst);
    }
    let contract = pair.contract.parse().unwrap();
    RunConfig {
      data_dir: data_dir.as_ref(),
      start: pair.checkpoint.load(std::sync::atomic::Ordering::SeqCst),
      end: block_length,
      cut,
      name: &format!("uniswap3_pair_events_{}", name),
      executor: &|start, end| metrics::uniswap_v3::fetch_uniswap_pair(&client, start, end, contract),
    }.run(|e: RunEvent| {
      pair.checkpoint.store(e.checkpoint, std::sync::atomic::Ordering::SeqCst);
      save_stage(&data_dir, &stage).ok();
    }).await?;
  }

  save_stage(&data_dir, &stage)?;
  Ok(())
}
