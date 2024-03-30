use std::{ops::Range, sync::Arc};

use anyhow::Result;
use ethers_core::types::{Address, Filter, Log, H256};
use ethers_providers::Middleware;
use futures::{stream, StreamExt as _};
use tokio::sync::Mutex;

use crate::metrics::block::BlockMetric;

#[tracing::instrument(level = "debug", skip_all, fields(height_range=format!("{}..{}", height_range.start, height_range.end)))]
pub async fn get_blocks<P: Middleware>(client: P, height_range: Range<u64>) -> Result<Vec<BlockMetric>>
where <P as Middleware>::Error: 'static {
  let result = Arc::new(Mutex::new(vec![BlockMetric::default(); height_range.clone().count()]));
  let height_start = height_range.start;
  stream::iter(height_range).for_each_concurrent(500, |i| {
    let client = &client;
    let result = result.clone();
    async move {
      let mut block = client.get_block_with_txs(i).await.unwrap().ok_or_else(||anyhow::anyhow!("block not exists {i:?}")).unwrap();
      block.number = block.number.or(Some(i.into()));
      let target = block.into();
      *result.lock().await.get_mut((i - height_start) as usize).unwrap() = target;
    }
  }).await;
  let result = result.lock().await.clone();
  Ok(result)
}

#[tracing::instrument(level = "debug", skip_all, fields(height_range=format!("{}..{}", height_range.start, height_range.end)))]
pub async fn get_logs<P: Middleware>(client: P, topic: Option<H256>, address: Option<Address>, height_range: Range<u64>) -> Result<Vec<Log>>
where <P as Middleware>::Error: 'static {
  if height_range.start >= height_range.end {
    return Ok(Vec::new());
  }
  let filter = Filter::new().from_block(height_range.start).to_block(height_range.end.saturating_sub(1));
  let filter = match address {
    Some(address) => filter.address(address),
    _ => filter,
  };
  let filter = match topic {
    Some(topic) => filter.topic0(topic),
    _ => filter,
  };
  info!(?filter);
  const PAGE_SIZE: u64 = 10000;
  let mut start = height_range.start;
  let end = height_range.end;
  let mut result = Vec::new();
  while start < end {
    let to_block = std::cmp::min(start + PAGE_SIZE, end) - 1;
    trace!(start, to_block);
    let logs = client.get_logs(&filter.clone().from_block(start).to_block(to_block)).await?;
    result.extend(logs);
    start += PAGE_SIZE;
  }
  Ok(result)
}
