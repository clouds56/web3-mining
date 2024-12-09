use std::sync::Arc;

use base::IERC20;
use ethers_core::types::Address;
use ethers_providers::Middleware;

use crate::Result;
pub use pendle::{IPendleMarket, IPendleST, IPendleYT};

pub mod base {
  use ethers_contract::abigen;
  abigen!(
    IERC20,
    r#"[
      function name() external view returns (string)
      function symbol() external view returns (string)
      function decimals() external view returns (uint8)

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

  abigen!(
    IERC4626,
    r#"[
      event Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)
      event Withdraw(address indexed sender, address indexed receiver, address indexed owner, uint256 assets, uint256 shares)

      error COMMENT_sUSDe()
      event RewardsReceived(uint256 amount)
    ]"#,
  );
}

pub mod pendle {
  use ethers_contract::abigen;
  abigen!(IPendleMarket, "./src/rpc/abi/pendle_mkt.json");
  // IPendleYield
  abigen!(IPendleST, "./src/rpc/abi/pendle_sy.json");
  // PendleYieldToken
  abigen!(IPendleYT, "./src/rpc/abi/pendle_yt.json");
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
  pub st_address: Address,
  pub pt_address: Address,
  pub rt_address: Address,
  pub pt_name: String,
  pub ut_address: Address,
  pub rt_reward_tokens: Vec<Address>,
}

pub async fn get_pendle_market_info<P: Middleware + 'static>(client: Arc<P>, market_address: Address) -> Result<PendleMarketInfo> {
  let market = IPendleMarket::new(market_address, client.clone());
  let expiry = market.expiry().call().await?.as_u64();
  let reward_tokens = market.get_reward_tokens().await?;
  let (st, pt, rt) = market.read_tokens().await?;
  let st_contract = IPendleST::new(st, client.clone());
  // this is a confused name in IPendleYield, it means underlying token, like sUSDE of SY-sUSDE
  let ut_address = st_contract.yield_token().call().await?;
  let pt_contract = IERC20::new(pt, client.clone());
  let pt_name = pt_contract.symbol().call().await?;
  let rt_contract = IPendleYT::new(rt, client.clone());
  let rt_reward_tokens = rt_contract.get_reward_tokens().call().await?;
  Ok(PendleMarketInfo {
    expiry,
    reward_tokens,
    st_address: st,
    pt_address: pt,
    pt_name,
    rt_address: rt,
    ut_address,
    rt_reward_tokens,
  })
}
