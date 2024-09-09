use std::sync::{atomic::AtomicU64, Arc};

use ethers_providers::Middleware;
use indexmap::IndexMap;

use crate::{config::Config, metrics, tasks::{EventListener, RunEvent}};

use super::{ContractStage, RunConfig};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UniswapStage {
  #[serde(default = "UniswapStage::default_uniswap_factory_events")]
  pub uniswap_factory_events: Arc<AtomicU64>,
  #[serde(default = "UniswapStage::default_uniswap3_factory_events")]
  pub uniswap3_factory_events: Arc<AtomicU64>,
  #[serde(default)]
  pub uniswap_pair_events: IndexMap<String, ContractStage>,
  #[serde(default)]
  pub uniswap3_pair_events: IndexMap<String, ContractStage>,
}

impl Default for UniswapStage {
  fn default() -> Self {
    Self {
      uniswap_factory_events: Self::default_uniswap_factory_events(),
      uniswap3_factory_events: Self::default_uniswap3_factory_events(),
      uniswap_pair_events: Default::default(),
      uniswap3_pair_events: Default::default(),
    }
  }
}

impl UniswapStage {
  fn default_uniswap_factory_events() -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(9_000_000))
  }
  fn default_uniswap3_factory_events() -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(11_000_000))
  }

  pub async fn run_tasks<P: Middleware>(&self, client: Arc<P>, config: &Config, default_event_listener: impl EventListener<RunEvent> + Copy) -> crate::Result<()> {
    RunConfig::new(&config, self.uniswap_factory_events.clone(), "uniswap_factory_events", &|start, end|
      metrics::uniswap_v2::fetch_uniswap_factory(client.clone(), start, end)
    ).run(default_event_listener).await?;

    RunConfig::new(&config, self.uniswap3_factory_events.clone(), "uniswap3_factory_events", &|start, end|
      metrics::uniswap_v3::fetch_factory(client.clone(), start, end)
    ).run(default_event_listener).await?;

    for (name, pair) in &self.uniswap_pair_events {
      pair.init_checkpoint(config.cut);
      let contract = pair.contract.parse().unwrap();
      RunConfig::new(&config, pair.checkpoint.clone(), &format!("uniswap_pair_events_{}", name), &|start, end|
        metrics::uniswap_v2::fetch_uniswap_pair(client.clone(), start, end, contract)
      ).run(default_event_listener).await?;
    }

    for (name, pair) in &self.uniswap3_pair_events {
      pair.init_checkpoint(config.cut);
      let contract = pair.contract.parse().unwrap();
      RunConfig::new(&config, pair.checkpoint.clone(), &format!("uniswap3_pair_events_{}", name), &|start, end|
        metrics::uniswap_v3::fetch_uniswap_pair(client.clone(), start, end, contract)
      ).run(default_event_listener).await?;
    }
    Ok(())
  }
}
