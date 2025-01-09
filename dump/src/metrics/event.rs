use anyhow::Result;
use ethers_contract::EthEvent;
use ethers_core::{abi::RawLog, types::{Address, Log, H256}};
use polars::{df, frame::DataFrame};

use super::{ToChecksumHex as _, ToHex as _, Value};


#[allow(non_camel_case_types)]
pub struct LogMetric {
  pub height: u64,
  pub block_index: u64,
  // pub timestamp: u64,
  pub contract: Address,
  pub tx_hash: String,
  pub raw: RawLog,
}

impl From<Log> for LogMetric {
  fn from(log: Log) -> Self {
    LogMetric {
      height: log.block_number.unwrap_or_default().as_u64(),
      block_index: log.log_index.unwrap_or_default().as_u64(),
      contract: log.address,
      tx_hash: log.transaction_hash.unwrap_or_default().to_hex(),
      raw: RawLog {
        topics: log.topics,
        data: log.data.to_vec(),
      },
    }
  }
}

impl LogMetric {
  pub fn to_df(log_metrics: &[Self]) -> Result<DataFrame> {
    let df = df! {
      "height" => log_metrics.iter().map(|i| i.height).collect::<Vec<_>>(),
      "block_index" => log_metrics.iter().map(|i| i.block_index).collect::<Vec<_>>(),
      "contract" => log_metrics.iter().map(|i| i.contract.to_checksum_hex()).collect::<Vec<_>>(),
      "tx_hash" => log_metrics.iter().map(|i| i.tx_hash.clone()).collect::<Vec<_>>(),
      "topic0" => log_metrics.iter().map(|i| i.topic0().0.to_hex()).collect::<Vec<_>>(),
      "topic1" => log_metrics.iter().map(|i| i.topic1().ok().map(|i| i.0.to_hex())).collect::<Vec<_>>(),
      "topic2" => log_metrics.iter().map(|i| i.topic2().ok().map(|i| i.0.to_hex())).collect::<Vec<_>>(),
      "topic3" => log_metrics.iter().map(|i| i.topic3().ok().map(|i| i.0.to_hex())).collect::<Vec<_>>(),
    }?;
    Ok(df)
  }

  pub fn topic0(&self) -> Value { Value(self.raw.topics.get(0).copied().unwrap_or_default()) }
  pub fn topic1(&self) -> Result<Value> { Ok(Value(self.raw.topics.get(1).copied().ok_or_else(|| anyhow::anyhow!("topic1 not present"))?)) }
  pub fn topic2(&self) -> Result<Value> { Ok(Value(self.raw.topics.get(2).copied().ok_or_else(|| anyhow::anyhow!("topic2 not present"))?)) }
  pub fn topic3(&self) -> Result<Value> { Ok(Value(self.raw.topics.get(3).copied().ok_or_else(|| anyhow::anyhow!("topic3 not present"))?)) }

  pub fn get_arg(&self, index: usize) -> Result<Value> {
    let offset = index * 0x20;
    let end = self.raw.data.len().min(offset + 0x20);
    if offset >= end {
      return Err(anyhow::anyhow!("data too short"));
    }
    let data = &self.raw.data[offset..end];
    Ok(Value(H256::from_slice(data)))
  }

  pub fn decode<T: EthEvent>(&self) -> Result<T> {
    let result = T::decode_log(&self.raw);
    Ok(result?)
  }
}
