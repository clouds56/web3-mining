use std::sync::Arc;

use anyhow::{bail, Result};
use ethers_core::types::{Address, H256, I256, U256};
use ethers_providers::Middleware;
use polars::{df, frame::DataFrame, series::Series};

use crate::rpc;

use super::{event::LogMetric, ToChecksumHex, ToHex};

#[allow(non_upper_case_globals)]
pub mod consts {
  use ethers_core::types::{Address, H256};

  lazy_static::lazy_static! {
    /// CreateNewMarket (index_topic_1 address market, index_topic_2 address PT, int256 scalarRoot, int256 initialAnchor, uint256 lnFeeRateRoot)
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

    /// NewInterestIndex (index_topic_1 uint256 newIndex)
    pub static ref TOPIC_YT_NewInterestIndex: H256 = "0x71475f2f645813fdbebf53a58968008bff11ee21a58f01c5a9cc263d0bc4703d".parse().unwrap();
    /// Mint (index_topic_1 address caller, index_topic_2 address receiverPT, index_topic_3 address receiverYT, uint256 amountSyToMint, uint256 amountPYOut)
    pub static ref TOPIC_YT_Mint: H256 = "0xc0025304673122449dd60b9b0093874b0e2fd6fe57af1c7c2fbfee0ccf5ead58".parse().unwrap();
    /// Burn (index_topic_1 address caller, index_topic_2 address receiver, uint256 amountPYToRedeem, uint256 amountSyOut)
    pub static ref TOPIC_YT_Burn: H256 = "0x5d624aa9c148153ab3446c1b154f660ee7701e549fe9b62dab7171b1c80e6fa2".parse().unwrap();
    /// RedeemInterest (index_topic_1 address user, uint256 interestOut)
    pub static ref TOPIC_YT_RedeemInterest: H256 = "0x83a945bd12c713615b59a6e48a3467c05d1a7442350600d6f7fce6af9f7190e9".parse().unwrap();
    /// RedeemRewards (index_topic_1 address user, uint256[] amountRewardsOut)
    pub static ref TOPIC_YT_RedeemRewards: H256 = "0x78d61a0c27b13f43911095f9f356f14daa3cd8b125eea1aa22421245e90e813d".parse().unwrap();
    /// CollectInterestFee (uint256 amountInterestFee)
    pub static ref TOPIC_YT_CollectInterestFee: H256 = "0x004e8d79e4b41c5fad7561dc7c07786ee4e52292da7a3f5dc7ab90e32cc30423".parse().unwrap();

    pub static ref CONTRACT_MarketFactory: Address = "0x1A6fCc85557BC4fB7B534ed835a03EF056552D52".parse().unwrap();
    pub static ref CONTRACT_PendleLPT_sUSDE_26SEP2024: Address = "0xd1D7D99764f8a52Aff007b7831cc02748b2013b5".parse().unwrap();
    pub static ref CONTRACT_PendleYT_sUSDE_26SEP2024: Address = "0xdc02b77a3986da62C7A78FED73949C9767850809".parse().unwrap();
  }

  #[test]
  fn test_topic0() {
    use ethers_contract::EthEvent as _;
    use crate::rpc::contract::pendle;
    assert_eq!(TOPIC_Mint.to_string(), pendle::i_pendle_market::MintFilter::signature().to_string());
    assert_eq!(TOPIC_UpdateImpliedRate.to_string(), pendle::i_pendle_market::UpdateImpliedRateFilter::signature().to_string());
    assert_eq!(TOPIC_Swap.to_string(), pendle::i_pendle_market::SwapFilter::signature().to_string());
    assert_eq!(TOPIC_Burn.to_string(), pendle::i_pendle_market::BurnFilter::signature().to_string());
    assert_eq!(TOPIC_RedeemRewards.to_string(), pendle::i_pendle_market::RedeemRewardsFilter::signature().to_string());
    assert_eq!(TOPIC_Approval.to_string(), pendle::i_pendle_market::ApprovalFilter::signature().to_string());
    assert_eq!(TOPIC_Transfer.to_string(), pendle::i_pendle_market::TransferFilter::signature().to_string());

    assert_eq!(TOPIC_YT_NewInterestIndex.to_string(), pendle::i_pendle_yt::NewInterestIndexFilter::signature().to_string());
    assert_eq!(TOPIC_YT_Mint.to_string(), pendle::i_pendle_yt::MintFilter::signature().to_string());
    assert_eq!(TOPIC_YT_Burn.to_string(), pendle::i_pendle_yt::BurnFilter::signature().to_string());
    assert_eq!(TOPIC_YT_RedeemInterest.to_string(), pendle::i_pendle_yt::RedeemInterestFilter::signature().to_string());
    assert_eq!(TOPIC_YT_RedeemRewards.to_string(), pendle::i_pendle_yt::RedeemRewardsFilter::signature().to_string());
    assert_eq!(TOPIC_YT_CollectInterestFee.to_string(), pendle::i_pendle_yt::CollectInterestFeeFilter::signature().to_string());
    assert_eq!(TOPIC_Approval.to_string(), pendle::i_pendle_yt::ApprovalFilter::signature().to_string());
    assert_eq!(TOPIC_Transfer.to_string(), pendle::i_pendle_yt::TransferFilter::signature().to_string());
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
  pub scalar: I256,
  pub anchor: I256,
  pub ln_fee_rate: U256,

  pub expiry: Option<u64>,
  pub reward_tokens: Option<Vec<Address>>,
  /// or sy_address
  pub st_address: Option<Address>,
  pub pt_address: Address,
  pub rt_address: Option<Address>,
  pub pt_name: Option<String>,
  pub ut_address: Option<Address>,
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
      scalar: log.get_arg(0)?.as_i256(),
      anchor: log.get_arg(1)?.as_i256(),
      ln_fee_rate: log.get_arg(2)?.as_u256(),
      expiry: None,
      reward_tokens: None,
      st_address: None,
      rt_address: None,
      pt_name: None,
      ut_address: None,
    };
    Ok(result)
  }
}

impl Log_CreateNewMarket {
  pub fn to_df(log_metrics: &[Self]) -> Result<DataFrame> {
    let df = df!{
      "height" => log_metrics.iter().map(|i| i.height).collect::<Vec<_>>(),
      "block_index" => log_metrics.iter().map(|i| i.block_index).collect::<Vec<_>>(),
      "contract" => log_metrics.iter().map(|i| i.contract.to_checksum_hex()).collect::<Vec<_>>(),
      "tx_hash" => log_metrics.iter().map(|i| i.tx_hash.to_hex()).collect::<Vec<_>>(),
      "pt_address" => log_metrics.iter().map(|i| i.pt_address.to_checksum_hex()).collect::<Vec<_>>(),
      "market_address" => log_metrics.iter().map(|i| i.market_address.to_checksum_hex()).collect::<Vec<_>>(),
      "scalar" => log_metrics.iter().map(|i| i.scalar.as_i128() as f64 * 1e-18).collect::<Vec<_>>(),
      "anchor" => log_metrics.iter().map(|i| i.anchor.as_i128() as f64 * 1e-18).collect::<Vec<_>>(),
      "ln_fee_rate" => log_metrics.iter().map(|i| i.ln_fee_rate.as_u128() as f64 * 1e-18).collect::<Vec<_>>(),
      "expiry" => log_metrics.iter().map(|i| i.expiry.map(|i| i as u64)).collect::<Vec<_>>(),
      "reward_tokens" => log_metrics.iter().map(|i|
        i.reward_tokens.as_ref().map(|i| i.into_iter().map(|j| j.to_checksum_hex()).collect::<Series>())
      ).collect::<Vec<_>>(),
      "st_address" => log_metrics.iter().map(|i| i.st_address.map(|i| i.to_checksum_hex())).collect::<Vec<_>>(),
      "rt_address" => log_metrics.iter().map(|i| i.rt_address.map(|i| i.to_checksum_hex())).collect::<Vec<_>>(),
      "pt_name" => log_metrics.iter().map(|i| i.pt_name.clone()).collect::<Vec<_>>(),
      "ut_address" => log_metrics.iter().map(|i| i.ut_address.map(|i| i.to_checksum_hex())).collect::<Vec<_>>(),
    }?;
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
      error!("failed convert market info");
      continue
    };
    let log = match rpc::contract::get_pendle_market_info(client.clone(), log.market_address).await {
      Ok(info) => Log_CreateNewMarket {
        expiry: Some(info.expiry),
        reward_tokens: Some(info.reward_tokens),
        st_address: Some(info.st_address),
        rt_address: Some(info.rt_address),
        pt_name: Some(info.pt_name),
        ut_address: Some(info.ut_address),
        ..log
      },
      Err(e) => {
        warn!(?log.market_address, ?e, "failed to fetch market info");
        log
      },
    };
    result.push(log);
  }
  let df = Log_CreateNewMarket::to_df(&result)?;
  debug!("{}", df.head(None));
  Ok(df)
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Market_ActionType {
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
  pub action: Market_ActionType,
  pub sender: Option<Address>,
  pub to: Option<Address>,
  pub value: Option<i128>,
  pub pt_value: Option<i128>,
  /// aka Sy
  pub st_value: Option<i128>,
  /// in unit st, netSyFee
  pub fee1: Option<u128>,
  /// in unit st, netSyToReserve
  pub fee2: Option<u128>,
  pub rewards: Option<Vec<u128>>,
  pub ln_implied_apy: Option<i128>,
}

impl TryFrom<LogMetric> for Log_Market {
  type Error = anyhow::Error;
  fn try_from(log: LogMetric) -> anyhow::Result<Self> {
    let topic0 = log.topic0().0;
    let action =
    if topic0 == *consts::TOPIC_Swap { Market_ActionType::Swap }
    else if topic0 == *consts::TOPIC_Mint { Market_ActionType::Mint }
    else if topic0 == *consts::TOPIC_Burn { Market_ActionType::Burn }
    else if topic0 == *consts::TOPIC_UpdateImpliedRate { Market_ActionType::Rate }
    else if topic0 == *consts::TOPIC_RedeemRewards { Market_ActionType::Rewards }
    else if topic0 == *consts::TOPIC_Approval { Market_ActionType::Approval }
    else if topic0 == *consts::TOPIC_Transfer { Market_ActionType::Transfer }
    else { bail!("unknown action type, {:?}", topic0) };
    let mut result = Log_Market {
      height: log.height,
      block_index: log.block_index,
      contract: log.contract,
      tx_hash: log.tx_hash.clone(),
      topic0,
      action,
      sender: None,
      to: None,
      value: None,
      pt_value: None,
      st_value: None,
      fee1: None,
      fee2: None,
      rewards: None,
      ln_implied_apy: None,
    };
    use rpc::contract::pendle::i_pendle_market;
    match action {
      // Mint (index_topic_1 address receiver, uint256 netLpMinted, uint256 netSyUsed, uint256 netPtUsed)
      Market_ActionType::Mint => {
        let event = log.decode::<i_pendle_market::MintFilter>()?;
        result.to = Some(event.receiver);
        result.value = Some(event.net_lp_minted.as_u128() as i128);
        result.st_value = Some(event.net_sy_used.as_u128() as i128);
        result.pt_value = Some(event.net_pt_used.as_u128() as i128);
      },
      // Swap (index_topic_1 address caller, index_topic_2 address receiver, int256 netPtOut, int256 netSyOut, uint256 netSyFee, uint256 netSyToReserve)
      Market_ActionType::Swap => {
        let event = log.decode::<i_pendle_market::SwapFilter>()?;
        result.sender = Some(event.caller);
        result.to = Some(event.receiver);
        result.pt_value = Some(event.net_pt_out.as_i128());
        result.st_value = Some(event.net_sy_out.as_i128());
        result.fee1 = Some(event.net_sy_fee.as_u128());
        result.fee2 = Some(event.net_sy_to_reserve.as_u128());
      },
      // UpdateImpliedRate (index_topic_1 uint256 timestamp, uint256 lnLastImpliedRate)
      Market_ActionType::Rate => {
        let event = log.decode::<i_pendle_market::UpdateImpliedRateFilter>()?;
        result.ln_implied_apy = Some(event.ln_last_implied_rate.as_u128() as i128);
      },
      // Burn (index_topic_1 address receiverSy, index_topic_2 address receiverPt, uint256 netLpBurned, uint256 netSyOut, uint256 netPtOut)
      Market_ActionType::Burn => {
        let event = log.decode::<i_pendle_market::BurnFilter>()?;
        result.to = Some(event.receiver_sy);
        result.value = Some(-(event.net_lp_burned.as_u128() as i128));
        result.st_value = Some(-(event.net_sy_out.as_u128() as i128));
        result.pt_value = Some(-(event.net_pt_out.as_u128() as i128));
      },
      // RedeemRewards (index_topic_1 address user, uint256[] rewardsOut)
      Market_ActionType::Rewards => {
        let event = log.decode::<i_pendle_market::RedeemRewardsFilter>()?;
        result.sender = Some(event.user);
        result.rewards = Some(event.rewards_out.iter().map(|i| i.as_u128()).collect());
      },
      // Transfer (index_topic_1 address from, index_topic_2 address to, uint256 value)
      Market_ActionType::Transfer => {
        let event = log.decode::<i_pendle_market::TransferFilter>()?;
        result.sender = Some(event.from);
        result.to = Some(event.to);
      },
      // Approval (index_topic_1 address owner, index_topic_2 address spender, uint256 value)
      Market_ActionType::Approval => {
        let event = log.decode::<i_pendle_market::ApprovalFilter>()?;
        result.sender = Some(event.owner);
        result.to = Some(event.spender);
      },
    }
    Ok(result)
  }
}

impl Log_Market {
  pub fn to_df(log_metrics: &[Self]) -> Result<DataFrame> {
    let df = df!{
      "height" => log_metrics.iter().map(|i| i.height).collect::<Vec<_>>(),
      "block_index" => log_metrics.iter().map(|i| i.block_index).collect::<Vec<_>>(),
      "contract" => log_metrics.iter().map(|i| i.contract.to_checksum_hex()).collect::<Vec<_>>(),
      "tx_hash" => log_metrics.iter().map(|i| i.tx_hash.clone()).collect::<Vec<_>>(),
      "action" => log_metrics.iter().map(|i| format!("{:?}", i.action)).collect::<Vec<_>>(),
      "sender" => log_metrics.iter().map(|i| i.sender.map(|i| i.to_checksum_hex())).collect::<Vec<_>>(),
      "to" => log_metrics.iter().map(|i| i.to.map(|i| i.to_checksum_hex())).collect::<Vec<_>>(),
      "value" => log_metrics.iter().map(|i| i.value.map(|i| i as f64)).collect::<Vec<_>>(),
      "pt_value" => log_metrics.iter().map(|i| i.pt_value.map(|i| i as f64)).collect::<Vec<_>>(),
      "st_value" => log_metrics.iter().map(|i| i.st_value.map(|i| i as f64)).collect::<Vec<_>>(),
      "fee1" => log_metrics.iter().map(|i| i.fee1.map(|i| i as f64)).collect::<Vec<_>>(),
      "fee2" => log_metrics.iter().map(|i| i.fee2.map(|i| i as f64)).collect::<Vec<_>>(),
      "rewards" => log_metrics.iter().map(|i| i.rewards.as_ref().map(|i| i.iter().map(|j| *j as f64).collect::<Series>())).collect::<Vec<_>>(),
      "ln_implied_apy" => log_metrics.iter().map(|i| i.ln_implied_apy.map(|i| i as f64 / 1e18)).collect::<Vec<_>>(),
    }?;
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

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum YT_ActionType {
  Mint,
  Burn,
  NewInterestIndex,
  RedeemInterest,
  RedeemRewards,
  CollectInterestFee,
  Approval,
  Transfer,
}

#[allow(non_camel_case_types)]
pub struct Log_YT {
  pub height: u64,
  pub block_index: u64,
  pub contract: Address,
  pub tx_hash: String,
  pub topic0: H256,
  pub action: YT_ActionType,
  pub sender: Option<Address>,
  pub to: Option<Address>,
  pub rt_value: Option<i128>,
  pub st_value: Option<i128>,
  pub fee: Option<u128>,
  pub rewards: Option<Vec<u128>>,
  pub st_scale_index: Option<u128>,
}

impl TryFrom<LogMetric> for Log_YT {
  type Error = anyhow::Error;
  fn try_from(log: LogMetric) -> anyhow::Result<Self> {
    let topic0 = log.topic0().0;
    let action =
    if topic0 == *consts::TOPIC_YT_Mint { YT_ActionType::Mint }
    else if topic0 == *consts::TOPIC_YT_Burn { YT_ActionType::Burn }
    else if topic0 == *consts::TOPIC_YT_NewInterestIndex { YT_ActionType::NewInterestIndex }
    else if topic0 == *consts::TOPIC_YT_RedeemInterest { YT_ActionType::RedeemInterest }
    else if topic0 == *consts::TOPIC_YT_RedeemRewards { YT_ActionType::RedeemRewards }
    else if topic0 == *consts::TOPIC_YT_CollectInterestFee { YT_ActionType::CollectInterestFee }
    else if topic0 == *consts::TOPIC_Approval { YT_ActionType::Approval }
    else if topic0 == *consts::TOPIC_Transfer { YT_ActionType::Transfer }
    else { bail!("unknown action type, {:?}", topic0) };
    let mut result = Log_YT {
      height: log.height,
      block_index: log.block_index,
      contract: log.contract,
      tx_hash: log.tx_hash.clone(),
      topic0,
      action,
      sender: None,
      to: None,
      rt_value: None,
      st_value: None,
      fee: None,
      rewards: None,
      st_scale_index: None,
    };
    use rpc::contract::pendle::i_pendle_yt;
    match action {
      // Mint (index_topic_1 address caller, index_topic_2 address receiverPT, index_topic_3 address receiverYT, uint256 amountSyToMint, uint256 amountPYOut)
      YT_ActionType::Mint => {
        let event = log.decode::<i_pendle_yt::MintFilter>()?;
        result.sender = Some(event.caller);
        result.to = Some(event.receiver_pt);
        result.st_value = Some(event.amount_sy_to_mint.as_u128() as i128);
        result.rt_value = Some(event.amount_py_out.as_u128() as i128);
      },
      // Burn (index_topic_1 address caller, index_topic_2 address receiver, uint256 amountPYToRedeem, uint256 amountSyOut)
      YT_ActionType::Burn => {
        let event = log.decode::<i_pendle_yt::BurnFilter>()?;
        result.sender = Some(event.caller);
        result.to = Some(event.receiver);
        result.rt_value = Some(-(event.amount_py_to_redeem.as_u128() as i128));
        result.st_value = Some(-(event.amount_sy_out.as_u128() as i128));
      },
      // NewInterestIndex (index_topic_1 uint256 newIndex)
      YT_ActionType::NewInterestIndex => {
        let event = log.decode::<i_pendle_yt::NewInterestIndexFilter>()?;
        result.st_scale_index = Some(event.new_index.as_u128());
      },
      // RedeemInterest (index_topic_1 address user, uint256 interestOut)
      YT_ActionType::RedeemInterest => {
        let event = log.decode::<i_pendle_yt::RedeemInterestFilter>()?;
        result.sender = Some(event.user);
        result.st_value = Some(-(event.interest_out.as_u128() as i128));
      },
      // RedeemRewards (index_topic_1 address user, uint256[] amountRewardsOut)
      YT_ActionType::RedeemRewards => {
        let event = log.decode::<i_pendle_yt::RedeemRewardsFilter>()?;
        result.sender = Some(event.user);
        result.rewards = Some(event.amount_rewards_out.iter().map(|i| i.as_u128()).collect());
      },
      // CollectInterestFee (uint256 amountInterestFee)
      YT_ActionType::CollectInterestFee => {
        let event = log.decode::<i_pendle_yt::CollectInterestFeeFilter>()?;
        result.fee = Some(event.amount_interest_fee.as_u128());
      },
      // Approval (index_topic_1 address owner, index_topic_2 address spender, uint256 value)
      YT_ActionType::Approval => {
        let event = log.decode::<i_pendle_yt::ApprovalFilter>()?;
        result.sender = Some(event.owner);
        result.to = Some(event.spender);
      },
      // Transfer (index_topic_1 address from, index_topic_2 address to, uint256 value)
      YT_ActionType::Transfer => {
        let event = log.decode::<i_pendle_yt::TransferFilter>()?;
        result.sender = Some(event.from);
        result.to = Some(event.to);
      },
    }
    Ok(result)
  }
}

impl Log_YT {
  pub fn to_df(log_metrics: &[Self]) -> Result<DataFrame> {
    let df = df!{
      "height" => log_metrics.iter().map(|i| i.height).collect::<Vec<_>>(),
      "block_index" => log_metrics.iter().map(|i| i.block_index).collect::<Vec<_>>(),
      "contract" => log_metrics.iter().map(|i| i.contract.to_checksum_hex()).collect::<Vec<_>>(),
      "tx_hash" => log_metrics.iter().map(|i| i.tx_hash.clone()).collect::<Vec<_>>(),
      "action" => log_metrics.iter().map(|i| format!("{:?}", i.action)).collect::<Vec<_>>(),
      "sender" => log_metrics.iter().map(|i| i.sender.map(|i| i.to_checksum_hex())).collect::<Vec<_>>(),
      "to" => log_metrics.iter().map(|i| i.to.map(|i| i.to_checksum_hex())).collect::<Vec<_>>(),
      "rt_value" => log_metrics.iter().map(|i| i.rt_value.map(|i| i as f64)).collect::<Vec<_>>(),
      "st_value" => log_metrics.iter().map(|i| i.st_value.map(|i| i as f64)).collect::<Vec<_>>(),
      "fee" => log_metrics.iter().map(|i| i.fee.map(|i| i as f64)).collect::<Vec<_>>(),
      "rewards" => log_metrics.iter().map(|i| i.rewards.as_ref().map(|i| i.iter().map(|i| *i as f64).collect::<Series>())).collect::<Vec<_>>(),
      "st_scale_index" => log_metrics.iter().map(|i| i.st_scale_index.map(|i| i as f64 / 1e18)).collect::<Vec<_>>(),
    }?;
    Ok(df)
  }
}

pub async fn fetch_pendle_yt<P: Middleware>(client: P, height_from: u64, height_to: u64, pair: Address) -> Result<DataFrame>
where P::Error: 'static {
  const PAGE_SIZE: u64 = 2000;
  let logs = rpc::eth::get_logs(client, None, Some(pair), height_from..height_to, PAGE_SIZE).await?;
  debug!(logs.len=?logs.len(), height_from, height_to);
  let logs = logs.into_iter().filter(|i| i.removed != Some(true)).map(LogMetric::from).filter_map(|i| Log_YT::try_from(i).ok()).collect::<Vec<_>>();
  let df = Log_YT::to_df(&logs)?;
  debug!("{}", df.head(None));
  Ok(df)
}
