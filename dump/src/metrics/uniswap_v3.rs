use anyhow::{bail, Result};
use ethers_core::types::{Address, H256};
use ethers_providers::Middleware;
use polars::{frame::DataFrame, prelude::NamedFrom as _, series::Series};

use crate::rpc;

use super::{event::LogMetric, ToChecksumHex, ToHex};

#[allow(non_upper_case_globals)]
pub mod consts {
  use ethers_core::types::{Address, H256};

  lazy_static::lazy_static! {
    /// PoolCreated (index_topic_1 address token0, index_topic_2 address token1, index_topic_3 uint24 fee, int24 tickSpacing, address pool)
    pub static ref TOPIC_PoolCreated: H256 = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118".parse().unwrap();

    /// Initialize (uint160 sqrtPriceX96, int24 tick)
    pub static ref TOPIC_Initialize: H256 = "0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95".parse().unwrap();
    /// Flash (index_topic_1 address sender, index_topic_2 address recipient, uint256 amount0, uint256 amount1, uint256 paid0, uint256 paid1)
    pub static ref TOPIC_Flash: H256 = "0xbdbdb71d7860376ba52b25a5028beea23581364a40522f6bcfb86bb1f2dca633".parse().unwrap();
    /// Collect (index_topic_1 address owner, address recipient, index_topic_2 int24 tickLower, index_topic_3 int24 tickUpper, uint128 amount0, uint128 amount1)
    pub static ref TOPIC_Collect: H256 = "0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01".parse().unwrap();
    /// Swap (index_topic_1 address sender, index_topic_2 address recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
    pub static ref TOPIC_Swap: H256 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67".parse().unwrap();
    /// Transfer (index_topic_1 address from, index_topic_2 address to, uint256 value)
    pub static ref TOPIC_Transfer: H256 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".parse().unwrap();
    /// Mint (address sender, index_topic_1 address owner, index_topic_2 int24 tickLower, index_topic_3 int24 tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
    pub static ref TOPIC_Mint: H256 = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde".parse().unwrap();
    /// Approval (index_topic_1 address owner, index_topic_2 address spender, uint256 value)
    pub static ref TOPIC_Approval: H256 = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925".parse().unwrap();
    /// Burn (index_topic_1 address owner, index_topic_2 int24 tickLower, index_topic_3 int24 tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
    pub static ref TOPIC_Burn: H256 = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c".parse().unwrap();

    pub static ref CONTRACT_UniswapV3Factory: Address = "0x1F98431c8aD98523631AE4a59f267346ea31F984".parse().unwrap();
    pub static ref CONTRACT_UniswapV3_WBTC_WETH: Address = "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD".parse().unwrap();
  }
}

// PairCreated (index_topic_1 address token0, index_topic_2 address token1, address pair, uint256)
#[allow(non_camel_case_types)]
pub struct Log_PoolCreated {
  pub height: u64,
  pub block_index: u64,
  pub contract: Address,
  pub tx_hash: H256,
  pub token0: Address,
  pub token1: Address,
  pub pair: Address,
  pub fee: u32,
  pub tick_spacing: u32,
}

impl TryFrom<LogMetric> for Log_PoolCreated {
  type Error = anyhow::Error;
  fn try_from(log: LogMetric) -> anyhow::Result<Self> {
    let result = Log_PoolCreated {
      height: log.height,
      block_index: log.block_index,
      contract: log.contract,
      tx_hash: log.tx_hash.parse().unwrap(),
      token0: log.topic1()?.as_address()?,
      token1: log.topic2()?.as_address()?,
      fee: log.topic3()?.as_u32(),
      tick_spacing: log.get_arg(0)?.as_u32(),
      pair: log.get_arg(1)?.as_address()?,
    };
    Ok(result)
  }
}

impl Log_PoolCreated {
  pub fn to_df(log_metrics: &[Self]) -> Result<DataFrame> {
    let df = DataFrame::new(vec![
      Series::new("height", log_metrics.iter().map(|i| i.height).collect::<Vec<_>>()),
      Series::new("block_index", log_metrics.iter().map(|i| i.block_index).collect::<Vec<_>>()),
      Series::new("contract", log_metrics.iter().map(|i| i.contract.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("tx_hash", log_metrics.iter().map(|i| i.tx_hash.to_hex()).collect::<Vec<_>>()),
      Series::new("token0", log_metrics.iter().map(|i| i.token0.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("token1", log_metrics.iter().map(|i| i.token1.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("pair", log_metrics.iter().map(|i| i.pair.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("fee", log_metrics.iter().map(|i| i.fee).collect::<Vec<_>>()),
      Series::new("tick_spacing", log_metrics.iter().map(|i| i.tick_spacing).collect::<Vec<_>>()),
    ])?;
    Ok(df)
  }
}

pub async fn fetch_factory<P: Middleware>(client: &P, height_from: u64, height_to: u64) -> Result<DataFrame>
where
  P::Error: 'static
{
  const PAGE_SIZE: u64 = 10000;
  let logs = rpc::get_logs(client, Some(consts::TOPIC_PoolCreated.clone()), None, height_from..height_to, PAGE_SIZE).await?;
  debug!(logs.len=?logs.len(), height_from, height_to);
  let logs = logs.into_iter().map(LogMetric::from).filter_map(|i| Log_PoolCreated::try_from(i).ok()).collect::<Vec<_>>();
  let df = Log_PoolCreated::to_df(&logs)?;
  debug!("{}", df.head(None));
  Ok(df)
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Pair_ActionType {
  Initialize,
  Flash,
  Collect,
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
  pub tick_lower: u32,
  pub tick_upper: u32,
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
    let action = if log.topic0 == *consts::TOPIC_Collect { Pair_ActionType::Collect }
    else if log.topic0 == *consts::TOPIC_Flash { Pair_ActionType::Flash }
    else if log.topic0 == *consts::TOPIC_Initialize { Pair_ActionType::Initialize }
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
      tick_lower: 0,
      tick_upper: 0,
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
      // Initialize (uint160 sqrtPriceX96, int24 tick)
      Pair_ActionType::Initialize => {
        result.tick_lower = log.get_arg(1)?.as_u32();
      }
      // Flash (index_topic_1 address sender, index_topic_2 address recipient, uint256 amount0, uint256 amount1, uint256 paid0, uint256 paid1)
      Pair_ActionType::Flash => {
        result.sender = Some(log.topic1()?.as_address()?);
        result.to = Some(log.topic2()?.as_address()?);
        result.amount0_in = Some(log.get_arg(0)?.as_u128());
        result.amount1_in = Some(log.get_arg(1)?.as_u128());
        result.amount0_out = Some(log.get_arg(2)?.as_u128());
        result.amount1_out = Some(log.get_arg(3)?.as_u128());
      }
      // Collect (index_topic_1 address owner, address recipient, index_topic_2 int24 tickLower, index_topic_3 int24 tickUpper, uint128 amount0, uint128 amount1)
      Pair_ActionType::Collect => {
        result.sender = Some(log.topic1()?.as_address()?);
        result.to = Some(log.get_arg(0)?.as_address()?);
        result.tick_lower = log.topic2()?.as_u32();
        result.tick_upper = log.topic3()?.as_u32();
        result.amount0_in = Some(log.get_arg(1)?.as_u128());
        result.amount1_in = Some(log.get_arg(2)?.as_u128());
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
        result.sender = Some(log.topic1()?.as_address()?);
        result.to = Some(log.topic2()?.as_address()?);
        // if from == Address::zero() {
        //   result.value_in = Some(log.get_arg(0)?.as_u128());
        // }
        // if to == Address::zero() {
        //   result.value_out = Some(log.get_arg(0)?.as_u128());
        // }
      }
      // Mint (address sender, index_topic_1 address owner, index_topic_2 int24 tickLower, index_topic_3 int24 tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
      Pair_ActionType::Mint => {
        result.sender = Some(log.get_arg(0)?.as_address()?);
        result.to = Some(log.topic1()?.as_address()?);
        result.tick_lower = log.topic2()?.as_u32();
        result.tick_upper = log.topic3()?.as_u32();
        result.value_in = Some(log.get_arg(0)?.as_u128());
        result.amount0_in = Some(log.get_arg(2)?.as_u128());
        result.amount1_in = Some(log.get_arg(3)?.as_u128());
      }
      // Approval (index_topic_1 address owner, index_topic_2 address spender, uint256 value)
      Pair_ActionType::Approval => {
        result.sender = Some(log.topic1()?.as_address()?);
        result.to = Some(log.topic2()?.as_address()?);
        // result.value_in = Some(log.get_arg(0)?.as_u128());
      }
      // Burn (index_topic_1 address owner, index_topic_2 int24 tickLower, index_topic_3 int24 tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
      Pair_ActionType::Burn => {
        result.sender = Some(log.topic1()?.as_address()?);
        result.tick_lower = log.topic2()?.as_u32();
        result.tick_upper = log.topic3()?.as_u32();
        result.value_out = Some(log.get_arg(0)?.as_u128());
        result.amount0_out = Some(log.get_arg(1)?.as_u128());
        result.amount1_out = Some(log.get_arg(2)?.as_u128());
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
  const PAGE_SIZE: u64 = 2000;
  let logs = rpc::get_logs(client, None, Some(pair), height_from..height_to, PAGE_SIZE).await?;
  debug!(logs.len=?logs.len(), height_from, height_to);
  let logs = logs.into_iter().filter(|i| i.removed != Some(true)).map(LogMetric::from).filter_map(|i| Log_Pair::try_from(i).ok()).collect::<Vec<_>>();
  let df = Log_Pair::to_df(&logs)?;
  debug!("{}", df.head(None));
  Ok(df)
}
