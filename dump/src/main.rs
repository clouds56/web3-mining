#[macro_use] extern crate tracing;

pub mod rpc;
pub mod metrics;
pub mod tasks;
pub mod config;

use std::{path::Path, str::FromStr as _, sync::{atomic::AtomicU64, Arc}};

use anyhow::Result;
use config::Config;
use ethers_providers::{JsonRpcClient, Middleware, Provider};
use tasks::{pendle::PendleStage, uniswap::UniswapStage, RunConfig, RunEvent};
use tracing_subscriber::fmt::format::FmtSpan;

async fn get_block_number<P: JsonRpcClient>(client: &Provider<P>) -> Result<u64> {
  let block_number = client.get_block_number().await?;
  Ok(block_number.as_u64())
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Stage {
  _cut: Option<u64>,
  #[serde(default)]
  block_metrics: Arc<AtomicU64>,

  #[serde(flatten)]
  uniswap: UniswapStage,

  #[serde(flatten)]
  pendle: PendleStage,
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
  let client = Arc::new(Provider::new(ethers_providers::Http::from_str(&config.endpoint)?));
  config.block_length = get_block_number(&client).await?;
  info!(config.block_length, "hello");

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
    metrics::block::fetch_blocks(client.clone(), start, end)
  ).run(|e: RunEvent| {
    assert_eq!(Some(e.cut), stage._cut);
    if e.len > 0 {
      assert_eq!(e.len, e.checkpoint - e.start + e.start % e.cut);
    }
    default_event_listener(e);
  }).await?;

  stage.uniswap.run_tasks(client.clone(), &config, default_event_listener).await?;
  stage.pendle.run_tasks(client.clone(), &config, default_event_listener).await?;

  save_stage(&config.data_dir, &stage)?;
  Ok(())
}
