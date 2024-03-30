use anyhow::Result;
use ethers_providers::Middleware;
use polars::{frame::DataFrame, prelude::NamedFrom as _, series::Series};

use crate::rpc;

pub async fn fetch_blocks<P: Middleware>(client: &P, height_from: u64, height_to: u64) -> Result<DataFrame>
where
  P::Error: 'static
{
  let block_metrics = rpc::block::block_metrics(client, height_from..height_to).await?;
  let acc_block = rpc::block::BlockMetric {
    height: block_metrics.iter().map(|i| i.height).reduce(u64::max).unwrap_or_default(),
    timestamp: block_metrics.iter().map(|i| i.timestamp).reduce(u64::max).unwrap_or_default(),
    tx_count: block_metrics.iter().map(|i| i.tx_count).sum::<usize>(),
    total_eth: block_metrics.iter().map(|i| i.total_eth).sum::<f64>(),
    gas_used: block_metrics.iter().map(|i| i.gas_used).sum::<u64>(),
    total_fee: block_metrics.iter().map(|i| i.total_fee).sum::<u64>(),
    fee_per_gas: block_metrics.iter().map(|i| i.fee_per_gas).sum::<u64>() / block_metrics.len() as u64,
  };
  info!(block=?acc_block);
  let df = DataFrame::new(vec![
    Series::new("height", block_metrics.iter().map(|i| i.height).collect::<Vec<_>>()),
    Series::new("timestamp", block_metrics.iter().map(|i| i.timestamp).collect::<Vec<_>>()),
    Series::new("tx_count", block_metrics.iter().map(|i| i.tx_count as u32).collect::<Vec<_>>()),
    Series::new("total_eth", block_metrics.iter().map(|i| i.total_eth).collect::<Vec<_>>()),
    Series::new("total_fee", block_metrics.iter().map(|i| i.total_fee).collect::<Vec<_>>()),
    Series::new("gas_used", block_metrics.iter().map(|i| i.gas_used).collect::<Vec<_>>()),
    Series::new("fee_per_gas", block_metrics.iter().map(|i| i.fee_per_gas).collect::<Vec<_>>()),
  ])?;
  debug!("{}", df.head(None));
  Ok(df)
}
