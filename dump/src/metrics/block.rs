use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use ethers_providers::Middleware;
use futures::{stream, StreamExt as _};

#[tracing::instrument(skip_all)]
pub async fn transaction_count<P: Middleware>(client: P, height: u64) -> Result<Vec<usize>>
where <P as Middleware>::Error: 'static {
  let result = (0..height).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>();
  stream::iter(0..height).for_each_concurrent(500, |i| {
    let client = &client;
    let target = result.get(i as usize).unwrap();
    async move {
      let block = client.get_block(i).await.unwrap().ok_or(anyhow::anyhow!("block not exists {i:?}")).unwrap();
      target.store(block.transactions.len(), std::sync::atomic::Ordering::Release);
    }
  }).await;
  Ok(result.into_iter().map(|i| i.load(std::sync::atomic::Ordering::Acquire)).collect())
}
