use std::sync::Arc;

use ethers_core::types::Address;
use ethers_providers::Middleware;

use crate::Result;
pub use pendle::{IPendleMarket, IPendleYield};

pub mod base {
  use ethers_contract::abigen;
  abigen!(
    IERC20,
    r#"[
      function totalSupply() external view returns (uint256)
      function balanceOf(address account) external view returns (uint256)
      function transfer(address recipient, uint256 amount) external returns (bool)
      function allowance(address owner, address spender) external view returns (uint256)
      function approve(address spender, uint256 amount) external returns (bool)
      function transferFrom( address sender, address recipient, uint256 amount) external returns (bool)
      event Transfer(address indexed from, address indexed to, uint256 value)
      event Approval(address indexed owner, address indexed spender, uint256 value)
    ]"#,
  );
}

pub mod pendle {
  use ethers_contract::abigen;
  abigen!(IPendleMarket, "./src/rpc/abi/pendle_mkt.json");
  abigen!(IPendleYield, "./src/rpc/abi/pendle_yield.json");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PendleAssetType {
  Token, Liquidity,
}
impl TryFrom<u8> for PendleAssetType {
  type Error = u8;
  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(Self::Token),
      1 => Ok(Self::Liquidity),
      _ => Err(value),
    }
  }
}

pub struct PendleMarketInfo {
  pub expiry: u64,
  pub reward_tokens: Vec<Address>,
  /// or sy_address
  pub tt_address: Address,
  pub pt_address: Address,
  pub rt_address: Address,
  pub at_type: PendleAssetType,
  pub at_address: Address,
  pub at_decimal: u8,
}

pub async fn get_pendle_market_info<P: Middleware + 'static>(client: Arc<P>, market_address: Address) -> Result<PendleMarketInfo> {
  let market = IPendleMarket::new(market_address, client.clone());
  let expiry = market.expiry().call().await?.as_u64();
  let reward_tokens = market.get_reward_tokens().await?;
  let (tt, pt, rt) = market.read_tokens().await?;
  let tt_contract = IPendleYield::new(tt, client.clone());
  let (at_type, at_address, at_decimal) = tt_contract.asset_info().call().await?;
  Ok(PendleMarketInfo {
    expiry,
    reward_tokens,
    tt_address: tt,
    pt_address: pt,
    rt_address: rt,
    at_type: PendleAssetType::try_from(at_type).map_err(|e| anyhow::format_err!("unknown at_type {}", e))?,
    at_address,
    at_decimal,
  })
}
