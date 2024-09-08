use std::sync::{atomic::AtomicU64, Arc};

use indexmap::IndexMap;

use crate::PairStage;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UniswapStage {
  #[serde(default = "UniswapStage::default_uniswap_factory_events")]
  pub uniswap_factory_events: Arc<AtomicU64>,
  #[serde(default = "UniswapStage::default_uniswap3_factory_events")]
  pub uniswap3_factory_events: Arc<AtomicU64>,
  #[serde(default)]
  pub uniswap_pair_events: IndexMap<String, PairStage>,
  #[serde(default)]
  pub uniswap3_pair_events: IndexMap<String, PairStage>,
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
}
