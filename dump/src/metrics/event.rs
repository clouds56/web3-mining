use anyhow::Result;
use ethers_core::types::{Address, Log, H256};
use polars::{frame::DataFrame, prelude::NamedFrom as _, series::Series};

use super::{ToChecksumHex as _, ToHex as _, Value};


#[allow(non_camel_case_types)]
pub struct LogMetric {
  pub height: u64,
  pub block_index: u64,
  // pub timestamp: u64,
  pub contract: Address,
  pub tx_hash: String,
  pub topic0: H256,
  pub topic1: Option<H256>,
  pub topic2: Option<H256>,
  pub topic3: Option<H256>,
  // pub topic4: Option<H256>,
  pub value: Vec<H256>,
}

impl From<Log> for LogMetric {
  fn from(log: Log) -> Self {
    LogMetric {
      height: log.block_number.unwrap_or_default().as_u64(),
      block_index: log.log_index.unwrap_or_default().as_u64(),
      contract: log.address,
      tx_hash: log.transaction_hash.unwrap_or_default().to_hex(),
      // timestamp: log.timestamp.as_u64(),
      topic0: log.topics.get(0).copied().unwrap_or_default(),
      topic1: log.topics.get(1).copied(),
      topic2: log.topics.get(2).copied(),
      topic3: log.topics.get(3).copied(),
      // topic4: log.topics.get(4).copied(),
      value: log.data.to_vec().chunks(32).map(|i| H256::from_slice(i)).collect::<Vec<_>>(),
    }
  }
}

impl LogMetric {
  pub fn to_df(log_metrics: &[Self]) -> Result<DataFrame> {
    let df = DataFrame::new(vec![
      Series::new("height", log_metrics.iter().map(|i| i.height).collect::<Vec<_>>()),
      Series::new("block_index", log_metrics.iter().map(|i| i.block_index).collect::<Vec<_>>()),
      Series::new("contract", log_metrics.iter().map(|i| i.contract.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("tx_hash", log_metrics.iter().map(|i| i.tx_hash.clone()).collect::<Vec<_>>()),
      Series::new("topic0", log_metrics.iter().map(|i| i.topic0.to_hex()).collect::<Vec<_>>()),
      Series::new("topic1", log_metrics.iter().map(|i| i.topic1.map(|i| i.to_hex())).collect::<Vec<_>>()),
      Series::new("topic2", log_metrics.iter().map(|i| i.topic2.map(|i| i.to_hex())).collect::<Vec<_>>()),
      Series::new("topic3", log_metrics.iter().map(|i| i.topic3.map(|i| i.to_hex())).collect::<Vec<_>>()),
      // Series::new("topic4", log_metrics.iter().map(|i| i.topic4.map(|i| i.to_hex())).collect::<Vec<_>>()),
    ])?;
    Ok(df)
  }

  pub fn topic1(&self) -> Result<Value> { Ok(Value(self.topic1.ok_or_else(|| anyhow::anyhow!("topic1 not present"))?)) }
  pub fn topic2(&self) -> Result<Value> { Ok(Value(self.topic2.ok_or_else(|| anyhow::anyhow!("topic2 not present"))?)) }
  pub fn topic3(&self) -> Result<Value> { Ok(Value(self.topic3.ok_or_else(|| anyhow::anyhow!("topic3 not present"))?)) }


  pub fn get_arg(&self, index: usize) -> Result<Value> {
    let value = self.value.get(index).copied().ok_or_else(|| anyhow::anyhow!("arg{} not present", index))?;
    Ok(Value(value))
  }
}
