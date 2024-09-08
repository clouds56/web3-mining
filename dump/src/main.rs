#[macro_use] extern crate tracing;

pub mod rpc;
pub mod metrics;
pub mod tasks;
pub mod config;

use std::{path::Path, str::FromStr as _, sync::{atomic::AtomicU64, Arc}};

use anyhow::Result;
use config::Config;
use ethers_providers::{JsonRpcClient, Middleware, Provider};
use indexmap::IndexMap;
use tasks::{uniswap::UniswapStage, RunConfig, RunEvent};
use tracing_subscriber::fmt::format::FmtSpan;

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
  block_metrics: Arc<AtomicU64>,

  #[serde(flatten)]
  uniswap: UniswapStage,
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

fn load_stage<P: AsRef<Path>>(data_dir: P) -> Result<Stage> {
  let filename = data_dir.as_ref().join("stage.toml");
  let stage: Stage = match std::fs::read_to_string(&filename) {
    Ok(content) => {
      toml::from_str::<Stage>(&content)?
    }
    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
      info!(stage_file=%filename.display(), "stage file not found, using default");
      Stage::default()
    }
    Err(e) => return Err(e)?,
  };
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
  let mut config = Config::from_env();
  info!(cwd=%std::env::current_dir().unwrap().display(), config.endpoint);
  std::fs::create_dir_all(&config.data_dir)?;
  let client = Provider::new(ethers_providers::Http::from_str(&config.endpoint)?);
  let block_length = get_block_number(&client).await?;
  info!(block_length, "hello");

  let mut stage = load_stage(&config.data_dir)?;
  if let Some(cut) = stage._cut {
    config.cut = cut
  } else {
    stage._cut = Some(config.cut);
  }
  info!(?stage);

  let default_event_listener = |_: RunEvent| {
    save_stage(&config.data_dir, &stage).ok();
  };

  // let magic_number = 98672723;
  // let block_length = magic_number * (stage.block_metrics + 1) % (10 * DEFAULT_CUT) + stage.block_metrics;
  // info!(block_length, "faking");
  RunConfig::new(&config, stage.block_metrics.clone(), "block_metrics", &|start, end|
    metrics::block::fetch_blocks(&client, start, end)
  ).run(|e: RunEvent| {
    assert_eq!(Some(e.cut), stage._cut);
    if e.len > 0 {
      assert_eq!(e.len, e.checkpoint - e.start + e.start % e.cut);
    }
    default_event_listener(e);
  }).await?;

  RunConfig::new(&config, stage.uniswap.uniswap_factory_events.clone(), "uniswap_factory_events", &|start, end|
    metrics::uniswap_v2::fetch_uniswap_factory(&client, start, end)
  ).run(default_event_listener).await?;

  RunConfig::new(&config, stage.uniswap.uniswap3_factory_events.clone(), "uniswap3_factory_events", &|start, end|
    metrics::uniswap_v3::fetch_factory(&client, start, end)
  ).run(default_event_listener).await?;

  for (name, pair) in &stage.uniswap.uniswap_pair_events {
    if checkpoint_is_none(&pair.checkpoint) {
      pair.checkpoint.store(pair.created / config.cut * config.cut, std::sync::atomic::Ordering::SeqCst);
    }
    let contract = pair.contract.parse().unwrap();
    RunConfig::new(&config, pair.checkpoint.clone(), &format!("uniswap_pair_events_{}", name), &|start, end|
      metrics::uniswap_v2::fetch_uniswap_pair(&client, start, end, contract)
    ).run(default_event_listener).await?;
  }

  for (name, pair) in &stage.uniswap.uniswap3_pair_events {
    if checkpoint_is_none(&pair.checkpoint) {
      pair.checkpoint.store(pair.created / config.cut * config.cut, std::sync::atomic::Ordering::SeqCst);
    }
    let contract = pair.contract.parse().unwrap();
    RunConfig::new(&config, pair.checkpoint.clone(), &format!("uniswap3_pair_events_{}", name), &|start, end|
      metrics::uniswap_v3::fetch_uniswap_pair(&client, start, end, contract)
    ).run(default_event_listener).await?;
  }

  save_stage(&config.data_dir, &stage)?;
  Ok(())
}
