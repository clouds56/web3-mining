use std::sync::Arc;

use ethers_providers::Middleware;
use indexmap::IndexMap;

use crate::{config::Config, metrics};

use super::{ContractStage, EventListener, RunConfig, RunEvent};

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct Erc4626Stage {
  #[serde(default)]
  pub erc4626_events: IndexMap<String, ContractStage>,
}

impl Erc4626Stage {
  pub async fn run_tasks<P: Middleware>(&self, client: Arc<P>, config: &Config, default_event_listener: impl EventListener<RunEvent> + Copy) -> crate::Result<()> {
    for (name, stage) in &self.erc4626_events {
      stage.init_checkpoint(config.cut);
      let address = stage.contract.parse().unwrap();
      RunConfig::new(&config, stage.checkpoint.clone(), &format!("erc4626_events_{}", name), &|start, end|
        metrics::erc4626::fetch_erc4626(client.clone(), start, end, address)
      ).run(default_event_listener).await?;
    }

    Ok(())
  }
}
