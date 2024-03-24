#[macro_use] extern crate tracing;

pub mod metrics;

use std::str::FromStr as _;

use anyhow::Result;
use ethers_providers::{JsonRpcClient, Middleware as _, Provider};

async fn get_block_number<P: JsonRpcClient>(client: &Provider<P>) -> Result<u64> {
  let block_number = client.get_block_number().await?;
  Ok(block_number.as_u64())
}

#[tokio::main]
async fn main() -> Result<()> {
  dotenvy::dotenv().ok();
  tracing_subscriber::fmt::fmt().with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE).init();
  let endpoint = format!("http://{}", std::env::var("RETH_HTTP_RPC").as_deref().unwrap_or("127.0.0.1:8545"));
  let client = Provider::new(ethers_providers::Http::from_str(&endpoint)?);
  println!("hello, current height is {}", get_block_number(&client).await?);
  let block_metrics = metrics::block::block_metrics(client, 100000).await?;
  let acc_block = metrics::block::BlockMetric {
    tx_count: block_metrics.iter().map(|i| i.tx_count).sum::<usize>(),
    total_eth: block_metrics.iter().map(|i| i.total_eth).sum::<f64>(),
    gas_used: block_metrics.iter().map(|i| i.gas_used).sum::<u64>(),
    total_fee: block_metrics.iter().map(|i| i.total_fee).sum::<u64>(),
    fee_per_gas: block_metrics.iter().map(|i| i.fee_per_gas).sum::<u64>() / block_metrics.len() as u64,
  };
  info!(block=?acc_block);
  Ok(())
}
