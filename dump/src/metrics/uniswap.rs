use anyhow::Result;
use ethers_core::types::Log;
use ethers_providers::Middleware;
use polars::{frame::DataFrame, prelude::NamedFrom as _, series::Series};

use crate::rpc;

use super::{ToChecksumHex, ToHex};

#[allow(non_upper_case_globals)]
mod consts {
  use ethers_core::types::H256;

  lazy_static::lazy_static! {
    pub static ref TOPIC_PairCreated: H256 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".parse().unwrap();
  }
}

#[allow(non_camel_case_types)]
pub struct LogMetric {
  pub height: u64,
  // pub timestamp: u64,
  pub contract: String,
  pub tx_hash: String,
  pub topic0: String,
  pub topic1: Option<String>,
  pub topic2: Option<String>,
  pub topic3: Option<String>,
  pub topic4: Option<String>,
}

impl From<Log> for LogMetric {
  fn from(log: Log) -> Self {
    LogMetric {
      height: log.block_number.unwrap_or_default().as_u64(),
      contract: log.address.to_checksum_hex(),
      tx_hash: log.transaction_hash.unwrap_or_default().to_hex(),
      // timestamp: log.timestamp.as_u64(),
      topic0: log.topics.get(0).map(|i| i.to_hex()).unwrap_or_default(),
      topic1: log.topics.get(1).map(|i| i.to_hex()),
      topic2: log.topics.get(2).map(|i| i.to_hex()),
      topic3: log.topics.get(3).map(|i| i.to_hex()),
      topic4: log.topics.get(4).map(|i| i.to_hex()),
    }
  }
}

impl LogMetric {
  pub fn to_df(log_metrics: &[Self]) -> Result<DataFrame> {
    let df = DataFrame::new(vec![
      Series::new("height", log_metrics.iter().map(|i| i.height).collect::<Vec<_>>()),
      Series::new("contract", log_metrics.iter().map(|i| i.contract.clone()).collect::<Vec<_>>()),
      Series::new("tx_hash", log_metrics.iter().map(|i| i.tx_hash.clone()).collect::<Vec<_>>()),
      Series::new("topic0", log_metrics.iter().map(|i| i.topic0.clone()).collect::<Vec<_>>()),
      Series::new("topic1", log_metrics.iter().map(|i| i.topic1.clone()).collect::<Vec<_>>()),
      Series::new("topic2", log_metrics.iter().map(|i| i.topic2.clone()).collect::<Vec<_>>()),
      Series::new("topic3", log_metrics.iter().map(|i| i.topic3.clone()).collect::<Vec<_>>()),
      Series::new("topic4", log_metrics.iter().map(|i| i.topic4.clone()).collect::<Vec<_>>()),
    ])?;
    Ok(df)
  }
}

pub async fn fetch_uniswap<P: Middleware>(client: &P, height_from: u64, height_to: u64) -> Result<DataFrame>
where
  P::Error: 'static
{
  let logs = rpc::get_logs(client, Some(consts::TOPIC_PairCreated.clone()), None, height_from..height_to).await?;
  debug!(logs.len=?logs.len(), height_from, height_to);
  let logs = logs.into_iter().map(LogMetric::from).collect::<Vec<_>>();
  let df = LogMetric::to_df(&logs)?;
  debug!("{}", df.head(None));
  Ok(df)
}
