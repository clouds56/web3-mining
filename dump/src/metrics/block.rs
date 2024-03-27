use std::{ops::Range, sync::Arc};

use anyhow::Result;
use ethers_providers::Middleware;
use futures::{stream, StreamExt as _};
use tokio::sync::Mutex;

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
// https://stackoverflow.com/questions/73167416/creating-polars-dataframe-from-vecstruct

#[tracing::instrument(skip_all)]
pub async fn block_metrics<P: Middleware>(client: P, height_range: Range<u64>) -> Result<Vec<BlockMetric>>
where <P as Middleware>::Error: 'static {
  let result = Arc::new(Mutex::new(vec![BlockMetric::default(); height_range.clone().count()]));
  let height_start = height_range.start;
  stream::iter(height_range).for_each_concurrent(500, |i| {
    let client = &client;
    let result = result.clone();
    async move {
      let block = client.get_block_with_txs(i).await.unwrap().ok_or_else(||anyhow::anyhow!("block not exists {i:?}")).unwrap();
      let total_fee = block.transactions.iter().map(|i| i.gas_price.unwrap_or_default().as_u128() * i.gas.as_u128()).sum::<u128>();
      let target = BlockMetric {
        height: block.number.ok_or_else(||anyhow::anyhow!("block number not exists {i:?}")).unwrap().as_u64(),
        timestamp: block.timestamp.as_u64(),
        tx_count: block.transactions.len(),
        total_eth: block.transactions.iter().map(|i| i.value.as_u128() as f64 / 1e18).sum(),
        gas_used: block.gas_used.as_u64(),
        total_fee: (total_fee / 1_000_000_000) as u64,
        fee_per_gas: (total_fee / block.gas_used.as_u128().max(1)) as u64,
      };
      *result.lock().await.get_mut((i - height_start) as usize).unwrap() = target;
    }
  }).await;
  let result = result.lock().await.clone();
  Ok(result)
}
