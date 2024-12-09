use anyhow::{bail, Result};
use ethers_core::types::{Address, U256};
use ethers_providers::Middleware;
use polars::{frame::DataFrame, prelude::NamedFrom as _, series::Series};

use crate::rpc;

use super::{event::LogMetric, ToChecksumHex};

#[allow(non_upper_case_globals)]
pub mod consts {
  use ethers_core::types::{Address, H256};

  lazy_static::lazy_static! {
    /// event Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)
    pub static ref TOPIC_Deposit: H256 = "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7".parse().unwrap();
    /// event Withdraw(address indexed sender, address indexed receiver, address indexed owner, uint256 assets, uint256 shares)
    pub static ref TOPIC_Withdraw: H256 = "0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db".parse().unwrap();

    pub static ref CONTRACT_sUSDe: Address = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497".parse().unwrap();
  }

  #[test]
  fn test_topic0() {
    use ethers_contract::EthEvent as _;
    use crate::rpc::contract::base;
    assert_eq!(base::ierc4626::DepositFilter::signature(), *TOPIC_Deposit);
    assert_eq!(base::ierc4626::WithdrawFilter::signature(), *TOPIC_Withdraw);
  }
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Erc4626_ActionType {
  Deposit, Withdraw,
}

#[allow(non_camel_case_types)]
pub struct Log_Erc4626 {
  pub sender: Address,
  pub receiver: Option<Address>,
  pub owner: Address,
  pub action: Erc4626_ActionType,
  pub assets: U256,
  pub shares: U256,
}

impl TryFrom<LogMetric> for Log_Erc4626 {
  type Error = anyhow::Error;
  fn try_from(log: LogMetric) -> Result<Self> {
    let topic0 = log.topic0().0;
    let action =
    if topic0 == *consts::TOPIC_Deposit { Erc4626_ActionType::Deposit }
    else if topic0 == *consts::TOPIC_Withdraw { Erc4626_ActionType::Withdraw }
    else { bail!("unknown topic0: {:?}", topic0) };
    use rpc::contract::base::ierc4626;
    let result = match action {
      Erc4626_ActionType::Deposit => {
        let event = log.decode::<ierc4626::DepositFilter>()?;
        Log_Erc4626 {
          sender: event.sender,
          receiver: None,
          owner: event.owner,
          action,
          assets: event.assets,
          shares: event.shares,
        }
      }
      Erc4626_ActionType::Withdraw => {
        let event = log.decode::<ierc4626::WithdrawFilter>()?;
        Log_Erc4626 {
          sender: event.sender,
          receiver: Some(event.receiver),
          owner: event.owner,
          action,
          assets: event.assets,
          shares: event.shares,
        }
      },
    };
    Ok(result)
  }
}

impl Log_Erc4626 {
  pub fn to_df(log_metrics: &[Self]) -> Result<DataFrame> {
    let df = DataFrame::new(vec![
      Series::new("sender", log_metrics.iter().map(|x| x.sender.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("receiver", log_metrics.iter().map(|x| x.receiver.map(|x| x.to_checksum_hex())).collect::<Vec<_>>()),
      Series::new("owner", log_metrics.iter().map(|x| x.owner.to_checksum_hex()).collect::<Vec<_>>()),
      Series::new("action", log_metrics.iter().map(|x| format!("{:?}", x.action)).collect::<Vec<_>>()),
      Series::new("assets", log_metrics.iter().map(|x| x.assets.to_string()).collect::<Vec<_>>()),
      Series::new("shares", log_metrics.iter().map(|x| x.shares.to_string()).collect::<Vec<_>>()),
    ])?;
    Ok(df)
  }
}

pub async fn fetch_erc4626<P: Middleware>(client: P, height_from: u64, height_to: u64, token: Address) -> Result<DataFrame>
where P::Error: 'static {
  const PAGE_SIZE: u64 = 2000;
  let logs = rpc::eth::get_logs(client, None, Some(token), height_from..height_to, PAGE_SIZE).await?;
  debug!(logs.len=?logs.len(), height_from, height_to);
  let logs = logs.into_iter().filter(|i| i.removed != Some(true)).map(LogMetric::from).filter_map(|i| Log_Erc4626::try_from(i).ok()).collect::<Vec<_>>();
  let df = Log_Erc4626::to_df(&logs)?;
  debug!("{}", df.head(None));
  Ok(df)
}
