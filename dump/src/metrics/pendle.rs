use std::sync::Arc;

use anyhow::{bail, Result};
use ethers_core::types::{Address, H256, I256, U256};
use ethers_providers::Middleware;
use polars::{frame::DataFrame, prelude::NamedFrom as _, series::Series};

use crate::rpc::{self, contract::PendleAssetType};

use super::{event::LogMetric, ToChecksumHex, ToHex};

#[allow(non_upper_case_globals)]
pub mod consts {
  use ethers_core::types::{Address, H256};

  lazy_static::lazy_static! {
    pub static ref TOPIC_CreateNewMarket: H256 = "0xae811fae25e2770b6bd1dcb1475657e8c3a976f91d1ebf081271db08eef920af".parse().unwrap();

    /// Mint (index_topic_1 address receiver, uint256 netLpMinted, uint256 netSyUsed, uint256 netPtUsed)
    pub static ref TOPIC_Mint: H256 = "0xb4c03061fb5b7fed76389d5af8f2e0ddb09f8c70d1333abbb62582835e10accb".parse().unwrap();
    /// UpdateImpliedRate (index_topic_1 uint256 timestamp, uint256 lnLastImpliedRate)
    pub static ref TOPIC_UpdateImpliedRate: H256 = "0x5c0e21d57bb4cf91d8fe238d6f92e2685a695371b19209afcce6217b478f83e1".parse().unwrap();
    /// Swap (index_topic_1 address caller, index_topic_2 address receiver, int256 netPtOut, int256 netSyOut, uint256 netSyFee, uint256 netSyToReserve)
    pub static ref TOPIC_Swap: H256 = "0x829000a5bc6a12d46e30cdcecd7c56b1efd88f6d7d059da6734a04f3764557c4".parse().unwrap();
    /// Burn (index_topic_1 address receiverSy, index_topic_2 address receiverPt, uint256 netLpBurned, uint256 netSyOut, uint256 netPtOut)
    pub static ref TOPIC_Burn: H256 = "0x4cf25bc1d991c17529c25213d3cc0cda295eeaad5f13f361969b12ea48015f90".parse().unwrap();
    /// RedeemRewards (index_topic_1 address user, uint256[] rewardsOut)
    pub static ref TOPIC_RedeemRewards: H256 = "0x78d61a0c27b13f43911095f9f356f14daa3cd8b125eea1aa22421245e90e813d".parse().unwrap();
    /// Approval (index_topic_1 address owner, index_topic_2 address spender, uint256 value)
    pub static ref TOPIC_Approval: H256 = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925".parse().unwrap();
    /// Transfer (index_topic_1 address from, index_topic_2 address to, uint256 value)
    pub static ref TOPIC_Transfer: H256 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".parse().unwrap();

    pub static ref CONTRACT_MarketFactory: Address = "0x1A6fCc85557BC4fB7B534ed835a03EF056552D52".parse().unwrap();
    pub static ref CONTRACT_PendleLPT26: Address = "0xd1D7D99764f8a52Aff007b7831cc02748b2013b5".parse().unwrap();
  }
}

// PairCreated (index_topic_1 address token0, index_topic_2 address token1, address pair, uint256)
#[allow(non_camel_case_types)]
pub struct Log_CreateNewMarket {
  pub height: u64,
  pub block_index: u64,
  pub contract: Address,
  pub tx_hash: H256,
  pub market_address: Address,
  pub scala: I256,
  pub anchor: I256,
  pub fee_rate: U256,

  pub expiry: Option<u64>,
  pub reward_tokens: Option<Vec<Address>>,
  /// or sy_address
  pub tt_address: Option<Address>,
  pub pt_address: Address,
  pub rt_address: Option<Address>,
  pub at_type: Option<PendleAssetType>,
  pub at_address: Option<Address>,
  pub at_decimal: Option<u8>,
}

impl TryFrom<LogMetric> for Log_CreateNewMarket {
  type Error = anyhow::Error;
  fn try_from(log: LogMetric) -> anyhow::Result<Self> {
    let result = Log_CreateNewMarket {
      height: log.height,
      block_index: log.block_index,
      contract: log.contract,
      tx_hash: log.tx_hash.parse().unwrap(),
      market_address: log.topic1()?.as_address()?,
      pt_address: log.topic2()?.as_address()?,
      scala: log.get_arg(0)?.as_i256(),
      anchor: log.get_arg(1)?.as_i256(),
      fee_rate: log.get_arg(2)?.as_u256(),
      expiry: None,
      reward_tokens: None,
      tt_address: None,
      rt_address: None,
      at_type: None,
      at_address: None,
      at_decimal: None,
    };
    Ok(result)
  }
}

impl Log_CreateNewMarket {
  pub fn to_df(log_metrics: &[Self]) -> Result<DataFrame> {
    let df = DataFrame::new(vec![
      Series::new("height", log_metrics.iter().map(|i| i.height).collect::<Vec<_>>()),
      Series::new("block_index", log_metrics.iter().map(|i| i.block_index).collect::<Vec<_>>()),
      Series::new("contract", log_metrics.iter().map(|i| i.contract.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("tx_hash", log_metrics.iter().map(|i| i.tx_hash.to_hex()).collect::<Vec<_>>()),
      Series::new("pt_address", log_metrics.iter().map(|i| i.pt_address.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("market_address", log_metrics.iter().map(|i| i.market_address.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("scala", log_metrics.iter().map(|i| i.scala.as_i128() as f64 * 1e-18).collect::<Vec<_>>()),
      Series::new("anchor", log_metrics.iter().map(|i| i.anchor.as_i128() as f64 * 1e-18).collect::<Vec<_>>()),
      Series::new("fee_rate", log_metrics.iter().map(|i| i.fee_rate.as_u128() as f64 * 1e-18).collect::<Vec<_>>()),
      Series::new("expiry", log_metrics.iter().map(|i| i.expiry.map(|i| i as u64)).collect::<Vec<_>>()),
      Series::new("reward_tokens", log_metrics.iter().map(|i|
        i.reward_tokens.as_ref().map(|i| i.into_iter().map(|j| j.to_checksum_hex()).collect::<Series>())
      ).collect::<Vec<_>>()),
      Series::new("tt_address", log_metrics.iter().map(|i| i.tt_address.map(|i| i.to_checksum_hex())).collect::<Vec<_>>()),
      Series::new("rt_address", log_metrics.iter().map(|i| i.rt_address.map(|i| i.to_checksum_hex())).collect::<Vec<_>>()),
      Series::new("at_type", log_metrics.iter().map(|i| i.at_type.map(|i| format!("{:?}", i))).collect::<Vec<_>>()),
      Series::new("at_address", log_metrics.iter().map(|i| i.at_address.map(|i| i.to_checksum_hex())).collect::<Vec<_>>()),
      Series::new("at_decimal", log_metrics.iter().map(|i| i.at_decimal.map(|i| i as u32)).collect::<Vec<_>>()),
    ])?;
    Ok(df)
  }
}

pub async fn fetch_pendle_market_factory<P: Middleware + 'static>(client: P, height_from: u64, height_to: u64) -> Result<DataFrame>
where P::Error: 'static {
  const PAGE_SIZE: u64 = 10000;
  let client = Arc::new(client);
  let logs = rpc::eth::get_logs(client.clone(), Some(consts::TOPIC_CreateNewMarket.clone()), None, height_from..height_to, PAGE_SIZE).await?;
  debug!(logs.len=?logs.len(), height_from, height_to);
  let mut result = Vec::with_capacity(logs.len());
  for log in logs {
    let Ok(log) = Log_CreateNewMarket::try_from(LogMetric::from(log)) else {
      continue
    };
    let info = rpc::contract::get_pendle_market_info(client.clone(), log.market_address).await?;
    let log = Log_CreateNewMarket {
      expiry: Some(info.expiry),
      reward_tokens: Some(info.reward_tokens),
      tt_address: Some(info.tt_address),
      rt_address: Some(info.rt_address),
      at_type: Some(info.at_type),
      at_address: Some(info.at_address),
      at_decimal: Some(info.at_decimal),
      ..log
    };
    result.push(log);
  }
  let df = Log_CreateNewMarket::to_df(&result)?;
  debug!("{}", df.head(None));
  Ok(df)
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Pair_ActionType {
  Mint,
  Swap,
  Rate,
  Burn,
  Rewards,
  Transfer,
  Approval,
}

#[allow(non_camel_case_types)]
pub struct Log_Market {
  pub height: u64,
  pub block_index: u64,
  pub contract: Address,
  pub tx_hash: String,
  pub topic0: H256,
  pub action: Pair_ActionType,
  pub sender: Option<Address>,
  pub to: Option<Address>,
  pub value: Option<i128>,
  pub pt_value: Option<i128>,
  /// aka Sy
  pub tt_value: Option<i128>,
  /// in unit tt, netSyFee
  pub fee1: Option<u128>,
  /// in unit tt, netSyToReserve
  pub fee2: Option<u128>,
  pub ln_fee_rate: Option<i128>,
}

impl TryFrom<LogMetric> for Log_Market {
  type Error = anyhow::Error;
  fn try_from(log: LogMetric) -> anyhow::Result<Self> {
    let action =
    if log.topic0 == *consts::TOPIC_Swap { Pair_ActionType::Swap }
    else if log.topic0 == *consts::TOPIC_Mint { Pair_ActionType::Mint }
    else if log.topic0 == *consts::TOPIC_Burn { Pair_ActionType::Burn }
    else if log.topic0 == *consts::TOPIC_UpdateImpliedRate { Pair_ActionType::Rate }
    else if log.topic0 == *consts::TOPIC_RedeemRewards { Pair_ActionType::Rewards }
    else if log.topic0 == *consts::TOPIC_Approval { Pair_ActionType::Approval }
    else if log.topic0 == *consts::TOPIC_Transfer { Pair_ActionType::Transfer }
    else { bail!("unknown action type") };
    let mut result = Log_Market {
      height: log.height,
      block_index: log.block_index,
      contract: log.contract,
      tx_hash: log.tx_hash.clone(),
      topic0: log.topic0,
      action,
      sender: None,
      to: None,
      value: None,
      pt_value: None,
      tt_value: None,
      fee1: None,
      fee2: None,
      ln_fee_rate: None,
    };
    match action {
      // Mint (index_topic_1 address receiver, uint256 netLpMinted, uint256 netSyUsed, uint256 netPtUsed)
      Pair_ActionType::Mint => {
        result.to = Some(log.topic1()?.as_address()?);
        result.value = Some(log.get_arg(0)?.as_i128());
        result.tt_value = Some(log.get_arg(1)?.as_i128());
        result.pt_value = Some(log.get_arg(2)?.as_i128());
      },
      // Swap (index_topic_1 address caller, index_topic_2 address receiver, int256 netPtOut, int256 netSyOut, uint256 netSyFee, uint256 netSyToReserve)
      Pair_ActionType::Swap => {
        result.sender = Some(log.topic1()?.as_address()?);
        result.to = Some(log.topic2()?.as_address()?);
        result.pt_value = Some(log.get_arg(0)?.as_i128());
        result.tt_value = Some(log.get_arg(1)?.as_i128());
        result.fee1 = Some(log.get_arg(2)?.as_u128());
        result.fee2 = Some(log.get_arg(3)?.as_u128());
      },
      // UpdateImpliedRate (index_topic_1 uint256 timestamp, uint256 lnLastImpliedRate)
      Pair_ActionType::Rate => {
        result.ln_fee_rate = Some(log.get_arg(0)?.as_i128());
      },
      // Burn (index_topic_1 address receiverSy, index_topic_2 address receiverPt, uint256 netLpBurned, uint256 netSyOut, uint256 netPtOut)
      Pair_ActionType::Burn => {
        result.to = Some(log.topic1()?.as_address()?);
        result.value = Some(-log.get_arg(0)?.as_i128());
        result.tt_value = Some(-log.get_arg(1)?.as_i128());
        result.pt_value = Some(-log.get_arg(2)?.as_i128());
      },
      // RedeemRewards (index_topic_1 address user, uint256[] rewardsOut)
      Pair_ActionType::Rewards => {
        result.to = Some(log.topic1()?.as_address()?);
      },
      // Transfer (index_topic_1 address from, index_topic_2 address to, uint256 value)
      Pair_ActionType::Transfer => {
        result.sender = Some(log.topic1()?.as_address()?);
        result.to = Some(log.topic2()?.as_address()?);
        // result.value = Some(log.get_arg(0)?.as_i128());
      },
      // Approval (index_topic_1 address owner, index_topic_2 address spender, uint256 value)
      Pair_ActionType::Approval => {
        result.sender = Some(log.topic1()?.as_address()?);
        result.to = Some(log.topic2()?.as_address()?);
        // result.value = Some(log.get_arg(0)?.as_i128());
      },
    }
    Ok(result)
  }
}

impl Log_Market {
  pub fn to_df(log_metrics: &[Self]) -> Result<DataFrame> {
    let df = DataFrame::new(vec![
      Series::new("height", log_metrics.iter().map(|i| i.height).collect::<Vec<_>>()),
      Series::new("block_index", log_metrics.iter().map(|i| i.block_index).collect::<Vec<_>>()),
      Series::new("contract", log_metrics.iter().map(|i| i.contract.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("tx_hash", log_metrics.iter().map(|i| i.tx_hash.clone()).collect::<Vec<_>>()),
      Series::new("action", log_metrics.iter().map(|i| format!("{:?}", i.action)).collect::<Vec<_>>()),
      Series::new("sender", log_metrics.iter().map(|i| i.sender.map(|i| i.to_checksum_hex())).collect::<Vec<_>>()),
      Series::new("to", log_metrics.iter().map(|i| i.to.map(|i| i.to_checksum_hex())).collect::<Vec<_>>()),
      Series::new("value", log_metrics.iter().map(|i| i.value.map(|i| i as f64)).collect::<Vec<_>>()),
      Series::new("pt_value", log_metrics.iter().map(|i| i.pt_value.map(|i| i as f64)).collect::<Vec<_>>()),
      Series::new("tt_value", log_metrics.iter().map(|i| i.tt_value.map(|i| i as f64)).collect::<Vec<_>>()),
      Series::new("fee1", log_metrics.iter().map(|i| i.fee1.map(|i| i as f64)).collect::<Vec<_>>()),
      Series::new("fee2", log_metrics.iter().map(|i| i.fee2.map(|i| i as f64)).collect::<Vec<_>>()),
      Series::new("ln_fee_rate", log_metrics.iter().map(|i| i.ln_fee_rate.map(|i| i as f64)).collect::<Vec<_>>()),
    ])?;
    Ok(df)
  }
}

pub async fn fetch_pendle_market<P: Middleware>(client: P, height_from: u64, height_to: u64, pair: Address) -> Result<DataFrame>
where P::Error: 'static {
  const PAGE_SIZE: u64 = 2000;
  let logs = rpc::eth::get_logs(client, None, Some(pair), height_from..height_to, PAGE_SIZE).await?;
  debug!(logs.len=?logs.len(), height_from, height_to);
  let logs = logs.into_iter().filter(|i| i.removed != Some(true)).map(LogMetric::from).filter_map(|i| Log_Market::try_from(i).ok()).collect::<Vec<_>>();
  let df = Log_Market::to_df(&logs)?;
  debug!("{}", df.head(None));
  Ok(df)
}
