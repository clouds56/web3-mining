use anyhow::Result;
use ethers_core::types::{Block, Transaction};
use ethers_providers::Middleware;
use polars::{frame::DataFrame, lazy::frame::IntoLazy, prelude::NamedFrom as _, series::Series};

use crate::rpc;

#[derive(Debug, Default, Clone)]
pub struct BlockMetric {
  pub height: u64,
  pub timestamp: u64,
  pub tx_count: usize,
  pub total_eth: f64, // eth
  pub gas_used: u64, // ~30M
  pub total_fee: u64, // gwei
  // pub burnt_fee: u64, // gwei
  pub fee_per_gas: u64, // wei
}

impl From<Block<Transaction>> for BlockMetric {
  fn from(block: Block<Transaction>) -> Self {
    let total_fee = block.transactions.iter().map(|i| i.gas_price.unwrap_or_default().as_u128() * i.gas.as_u128()).sum::<u128>();
    BlockMetric {
      height: block.number.unwrap_or_default().as_u64(),
      timestamp: block.timestamp.as_u64(),
      tx_count: block.transactions.len(),
      total_eth: block.transactions.iter().map(|i| i.value.as_u128() as f64 / 1e18).sum(),
      gas_used: block.gas_used.as_u64(),
      total_fee: (total_fee / 1_000_000_000) as u64,
      fee_per_gas: (total_fee / block.gas_used.as_u128().max(1)) as u64,
    }
  }
}

impl BlockMetric {
  pub fn to_df(block_metrics: &[Self]) -> Result<DataFrame> {
    let df = DataFrame::new(vec![
      Series::new("height", block_metrics.iter().map(|i| i.height).collect::<Vec<_>>()),
      Series::new("timestamp", block_metrics.iter().map(|i| i.timestamp).collect::<Vec<_>>()),
      Series::new("tx_count", block_metrics.iter().map(|i| i.tx_count as u32).collect::<Vec<_>>()),
      Series::new("total_eth", block_metrics.iter().map(|i| i.total_eth).collect::<Vec<_>>()),
      Series::new("total_fee", block_metrics.iter().map(|i| i.total_fee).collect::<Vec<_>>()),
      Series::new("gas_used", block_metrics.iter().map(|i| i.gas_used).collect::<Vec<_>>()),
      Series::new("fee_per_gas", block_metrics.iter().map(|i| i.fee_per_gas).collect::<Vec<_>>()),
    ])?;
    Ok(df)
  }
}

// https://stackoverflow.com/questions/73167416/creating-polars-dataframe-from-vecstruct
pub async fn fetch_blocks<P: Middleware>(client: P, height_from: u64, height_to: u64) -> Result<DataFrame> {
  use polars::lazy::dsl::col;
  let block_metrics = rpc::eth::get_blocks(client, height_from..height_to).await?;
  debug!(block_metrics.len=?block_metrics.len(), height_from, height_to);
  let df = BlockMetric::to_df(&block_metrics)?;
  let agg = df.clone().lazy().select([
    col("height").max(),
    col("timestamp").min(),
    col("tx_count").sum(),
    col("total_eth").sum(),
    col("total_fee").sum(),
    col("gas_used").sum(),
    col("fee_per_gas").mean(),
  ]).collect().ok();
  agg.map(|agg| info!("{}", agg));
  debug!("{}", df.head(None));
  Ok(df)
}
