use anyhow::Result;
use ethers_core::types::{Address, Log, H256};
use ethers_providers::Middleware;
use polars::{frame::DataFrame, prelude::NamedFrom as _, series::Series};

use crate::rpc;

use super::{ToChecksumHex, ToHex};

#[allow(non_upper_case_globals)]
mod consts {
  use ethers_core::types::{Address, H256};

  lazy_static::lazy_static! {
    pub static ref TOPIC_PairCreated: H256 = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9".parse().unwrap();
    pub static ref CONTRACT_UniswapV2Factory: Address = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f".parse().unwrap();
  }
}

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
      Series::new("topic0", log_metrics.iter().map(|i| i.topic0.to_string()).collect::<Vec<_>>()),
      Series::new("topic1", log_metrics.iter().map(|i| i.topic1.map(|i| i.to_string())).collect::<Vec<_>>()),
      Series::new("topic2", log_metrics.iter().map(|i| i.topic2.map(|i| i.to_string())).collect::<Vec<_>>()),
      Series::new("topic3", log_metrics.iter().map(|i| i.topic3.map(|i| i.to_string())).collect::<Vec<_>>()),
      // Series::new("topic4", log_metrics.iter().map(|i| i.topic4.map(|i| i.to_string())).collect::<Vec<_>>()),
    ])?;
    Ok(df)
  }

  pub fn topic_as_address(value: H256) -> Result<Address> {
    if value.0[..12] != [0; 12] {
      return Err(anyhow::anyhow!("Invalid address"));
    }
    Ok(Address::from_slice(&value.0[12..]))
  }

  pub fn parse_value() {

  }
}

// PairCreated (index_topic_1 address token0, index_topic_2 address token1, address pair, uint256)
#[allow(non_camel_case_types)]
pub struct Log_CreatePair {
  pub height: u64,
  pub block_index: u64,
  pub contract: Address,
  pub tx_hash: H256,
  pub token0: Address,
  pub token1: Address,
  pub pair: Address,
  pub all_pair_count: u64,
}

impl TryFrom<LogMetric> for Log_CreatePair {
  type Error = anyhow::Error;
  fn try_from(log: LogMetric) -> anyhow::Result<Self> {
    let result = Log_CreatePair {
      height: log.height,
      block_index: log.block_index,
      contract: log.contract,
      tx_hash: log.tx_hash.parse().unwrap(),
      token0: LogMetric::topic_as_address(log.topic1.ok_or_else(||anyhow::anyhow!("topic1 not present"))?)?,
      token1: LogMetric::topic_as_address(log.topic2.ok_or_else(||anyhow::anyhow!("topic2 not present"))?)?,
      pair: LogMetric::topic_as_address(log.value.get(0).copied().ok_or_else(||anyhow::anyhow!("arg0 not present"))?)?,
      all_pair_count: log.value.get(1).copied().unwrap_or_default().to_low_u64_be(),
    };
    Ok(result)
  }
}

impl Log_CreatePair {
  pub fn to_df(log_metrics: &[Self]) -> Result<DataFrame> {
    let df = DataFrame::new(vec![
      Series::new("height", log_metrics.iter().map(|i| i.height).collect::<Vec<_>>()),
      Series::new("block_index", log_metrics.iter().map(|i| i.block_index).collect::<Vec<_>>()),
      Series::new("contract", log_metrics.iter().map(|i| i.contract.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("tx_hash", log_metrics.iter().map(|i| i.tx_hash.to_hex()).collect::<Vec<_>>()),
      Series::new("token0", log_metrics.iter().map(|i| i.token0.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("token1", log_metrics.iter().map(|i| i.token1.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("pair", log_metrics.iter().map(|i| i.pair.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("all_pair_count", log_metrics.iter().map(|i| i.all_pair_count).collect::<Vec<_>>()),
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
  let logs = logs.into_iter().map(LogMetric::from).filter_map(|i| Log_CreatePair::try_from(i).ok()).collect::<Vec<_>>();
  let df = Log_CreatePair::to_df(&logs)?;
  debug!("{}", df.head(None));
  Ok(df)
}
