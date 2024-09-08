use std::sync::{atomic::AtomicU64, Arc};

use ethers_providers::Middleware;
use indexmap::IndexMap;

use crate::{config::Config, metrics};

use super::{ContractStage, EventListener, RunConfig, RunEvent};


#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendleStage {
  #[serde(default = "PendleStage::default_pendle2_market_factory_events")]
  pub pendle2_market_factory_events: Arc<AtomicU64>,
  #[serde(default)]
  pub pendle2_market_events: IndexMap<String, ContractStage>,
}

impl Default for PendleStage {
  fn default() -> Self {
    Self {
      pendle2_market_factory_events: Self::default_pendle2_market_factory_events(),
      pendle2_market_events: Default::default(),
    }
  }
}

impl PendleStage {
  fn default_pendle2_market_factory_events() -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(18_000_000))
  }

  pub async fn run_tasks<P: Middleware>(&self, client: &P, config: &Config, default_event_listener: impl EventListener<RunEvent> + Copy) -> crate::Result<()> {
    RunConfig::new(&config, self.pendle2_market_factory_events.clone(), "pendle2_market_factory_events", &|start, end|
      metrics::pendle::fetch_pendle_market_factory(&client, start, end)
    ).run(default_event_listener).await?;

    for (name, market) in &self.pendle2_market_events {
      market.init_checkpoint(config.cut);
      let contract = market.contract.parse().unwrap();
      RunConfig::new(&config, market.checkpoint.clone(), &format!("pendle2_market_events_{}", name), &|start, end|
        metrics::pendle::fetch_pendle_market(&client, start, end, contract)
      ).run(default_event_listener).await?;
    }

    Ok(())
  }
}
