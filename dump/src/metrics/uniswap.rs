use anyhow::{bail, Result};
use ethers_core::types::{Address, Log, H256};
use ethers_providers::Middleware;
use polars::{frame::DataFrame, prelude::NamedFrom as _, series::Series};

use crate::rpc;

use super::{ToChecksumHex, ToHex, Value};

#[allow(non_upper_case_globals)]
pub mod consts {
  use ethers_core::types::{Address, H256};

  lazy_static::lazy_static! {
    pub static ref TOPIC_PairCreated: H256 = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9".parse().unwrap();

    /// Sync (uint112 reserve0, uint112 reserve1)
    pub static ref TOPIC_Sync: H256 = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1".parse().unwrap();
    /// Swap (index_topic_1 address sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, index_topic_2 address to)
    pub static ref TOPIC_Swap: H256 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822".parse().unwrap();
    /// Transfer (index_topic_1 address from, index_topic_2 address to, uint256 value)
    pub static ref TOPIC_Transfer: H256 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".parse().unwrap();
    /// Mint (index_topic_1 address sender, uint256 amount0, uint256 amount1)
    pub static ref TOPIC_Mint: H256 = "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f".parse().unwrap();
    /// Approval (index_topic_1 address owner, index_topic_2 address spender, uint256 value)
    pub static ref TOPIC_Approval: H256 = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925".parse().unwrap();
    /// Burn (index_topic_1 address sender, uint256 amount0, uint256 amount1, index_topic_2 address to)
    pub static ref TOPIC_Burn: H256 = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496".parse().unwrap();

    pub static ref CONTRACT_UniswapV2Factory: Address = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f".parse().unwrap();
    pub static ref CONTRACT_UniswapV2_USDC_WETH: Address = "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc".parse().unwrap();
    pub static ref CONTRACT_UniswapV2_DAI_USDC: Address = "0xAE461cA67B15dc8dc81CE7615e0320dA1A9aB8D5".parse().unwrap();
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
      token0: log.topic1()?.as_address()?,
      token1: log.topic2()?.as_address()?,
      pair: log.get_arg(0)?.as_address()?,
      all_pair_count: log.get_arg(1)?.as_u64(),
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

pub async fn fetch_uniswap_factory<P: Middleware>(client: &P, height_from: u64, height_to: u64) -> Result<DataFrame>
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

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Pair_ActionType {
  Sync,
  Swap,
  Transfer,
  Mint,
  Approval,
  Burn,
}

#[allow(non_camel_case_types)]
pub struct Log_Pair {
  pub height: u64,
  pub block_index: u64,
  pub contract: Address,
  pub tx_hash: String,
  pub topic0: H256,
  pub action: Pair_ActionType,
  pub sender: Option<Address>,
  pub to: Option<Address>,
  pub value_in: Option<u128>,
  pub value_out: Option<u128>,
  pub amount0_in: Option<u128>,
  pub amount1_in: Option<u128>,
  pub amount0_out: Option<u128>,
  pub amount1_out: Option<u128>,
  pub reserve0: Option<u128>,
  pub reserve1: Option<u128>,
}

impl TryFrom<LogMetric> for Log_Pair {
  type Error = anyhow::Error;
  fn try_from(log: LogMetric) -> anyhow::Result<Self> {
    let action = if log.topic0 == *consts::TOPIC_Sync { Pair_ActionType::Sync }
    else if log.topic0 == *consts::TOPIC_Swap { Pair_ActionType::Swap }
    else if log.topic0 == *consts::TOPIC_Transfer { Pair_ActionType::Transfer }
    else if log.topic0 == *consts::TOPIC_Mint { Pair_ActionType::Mint }
    else if log.topic0 == *consts::TOPIC_Approval { Pair_ActionType::Approval }
    else if log.topic0 == *consts::TOPIC_Burn { Pair_ActionType::Burn }
    else { bail!("unknown action type") };
    let mut result = Log_Pair {
      height: log.height,
      block_index: log.block_index,
      contract: log.contract,
      tx_hash: log.tx_hash.clone(),
      topic0: log.topic0,
      action,
      sender: None,
      to: None,
      value_in: None,
      value_out: None,
      amount0_in: None,
      amount1_in: None,
      amount0_out: None,
      amount1_out: None,
      reserve0: None,
      reserve1: None,
    };
    match action {
      // Sync (uint112 reserve0, uint112 reserve1)
      Pair_ActionType::Sync => {
        result.reserve0 = Some(log.get_arg(0)?.as_u128());
        result.reserve1 = Some(log.get_arg(1)?.as_u128());
      }
      // Swap (index_topic_1 address sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, index_topic_2 address to)
      Pair_ActionType::Swap => {
        result.sender = Some(log.topic1()?.as_address()?);
        result.amount0_in = Some(log.get_arg(0)?.as_u128());
        result.amount1_in = Some(log.get_arg(1)?.as_u128());
        result.amount0_out = Some(log.get_arg(2)?.as_u128());
        result.amount1_out = Some(log.get_arg(3)?.as_u128());
        result.to = Some(log.topic2()?.as_address()?);
      }
      // Transfer (index_topic_1 address from, index_topic_2 address to, uint256 value)
      Pair_ActionType::Transfer => {
        let from = log.topic1()?.as_address()?;
        let to = log.topic2()?.as_address()?;
        if from == Address::zero() {
          result.value_in = Some(log.get_arg(0)?.as_u128());
        }
        if to == Address::zero() {
          result.value_out = Some(log.get_arg(0)?.as_u128());
        }
      }
      // Mint (index_topic_1 address sender, uint256 amount0, uint256 amount1)
      Pair_ActionType::Mint => {
        result.sender = Some(log.topic1()?.as_address()?);
        result.amount0_in = Some(log.get_arg(0)?.as_u128());
        result.amount1_in = Some(log.get_arg(1)?.as_u128());
      }
      // Approval (index_topic_1 address owner, index_topic_2 address spender, uint256 value)
      Pair_ActionType::Approval => {
        result.sender = Some(log.topic1()?.as_address()?);
        result.to = Some(log.topic2()?.as_address()?);
        // result.value_in = Some(log.get_arg(0)?.as_u128());
      }
      // Burn (index_topic_1 address sender, uint256 amount0, uint256 amount1, index_topic_2 address to)
      Pair_ActionType::Burn => {
        result.sender = Some(log.topic1()?.as_address()?);
        result.amount0_out = Some(log.get_arg(0)?.as_u128());
        result.amount1_out = Some(log.get_arg(1)?.as_u128());
        result.to = Some(log.topic2()?.as_address()?);
      }
    }
    Ok(result)
  }
}

impl Log_Pair {
  pub fn to_df(log_metrics: &[Self]) -> Result<DataFrame> {
    let df = DataFrame::new(vec![
      Series::new("height", log_metrics.iter().map(|i| i.height).collect::<Vec<_>>()),
      Series::new("block_index", log_metrics.iter().map(|i| i.block_index).collect::<Vec<_>>()),
      Series::new("contract", log_metrics.iter().map(|i| i.contract.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("tx_hash", log_metrics.iter().map(|i| i.tx_hash.clone()).collect::<Vec<_>>()),
      Series::new("action", log_metrics.iter().map(|i| format!("{:?}", i.action)).collect::<Vec<_>>()),
      Series::new("sender", log_metrics.iter().map(|i| i.sender.map(|i| i.to_checksum_hex())).collect::<Vec<_>>()),
      Series::new("to", log_metrics.iter().map(|i| i.to.map(|i| i.to_checksum_hex())).collect::<Vec<_>>()),
      Series::new("value_in", log_metrics.iter().map(|i| i.value_in.map(|x| x as f64)).collect::<Vec<_>>()),
      Series::new("value_out", log_metrics.iter().map(|i| i.value_out.map(|x| x as f64)).collect::<Vec<_>>()),
      Series::new("amount0_in", log_metrics.iter().map(|i| i.amount0_in.map(|x| x as f64)).collect::<Vec<_>>()),
      Series::new("amount1_in", log_metrics.iter().map(|i| i.amount1_in.map(|x| x as f64)).collect::<Vec<_>>()),
      Series::new("amount0_out", log_metrics.iter().map(|i| i.amount0_out.map(|x| x as f64)).collect::<Vec<_>>()),
      Series::new("amount1_out", log_metrics.iter().map(|i| i.amount1_out.map(|x| x as f64)).collect::<Vec<_>>()),
      Series::new("reserve0", log_metrics.iter().map(|i| i.reserve0.map(|x| x as f64)).collect::<Vec<_>>()),
      Series::new("reserve1", log_metrics.iter().map(|i| i.reserve1.map(|x| x as f64)).collect::<Vec<_>>()),
    ])?;
    Ok(df)
  }
}

pub async fn fetch_uniswap_pair<P: Middleware>(client: &P, height_from: u64, height_to: u64, pair: Address) -> Result<DataFrame>
where
  P::Error: 'static
{
  let logs = rpc::get_logs(client, None, Some(pair), height_from..height_to).await?;
  debug!(logs.len=?logs.len(), height_from, height_to);
  let logs = logs.into_iter().filter(|i| i.removed != Some(true)).map(LogMetric::from).filter_map(|i| Log_Pair::try_from(i).ok()).collect::<Vec<_>>();
  let df = Log_Pair::to_df(&logs)?;
  debug!("{}", df.head(None));
  Ok(df)
}
