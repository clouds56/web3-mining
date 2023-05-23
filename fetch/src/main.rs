use primitive_types::{U256, H256, H160};

mod conn;
mod model;
mod rpc;

// TODO: switch to read with confy
static TABLE_EVENTS: &str = "tx_event_test";
static TABLE_TXMSGS: &str = "tx_message_test";

async fn run() -> anyhow::Result<()> {
  let log = model::Event {
    block_number: 0,
    idx_in_block: 0,
    transaction: H256::zero(),
    account: H160::zero(),
    data_len: 0,
    data_prefix_u256: U256::zero(),
    data_prefix_128bytes: vec![],
    topic_num: 0,
    topic0: H256::zero(),
    topic1: H256::zero(),
    topic2: H256::zero(),
    topic3: H256::zero(),
    topic4: H256::zero(),
};
  let config = toml::from_str(std::fs::read_to_string("config.toml")?.as_str())?;
  println!("config: {config:?}");
  let client = conn::clkhs_init_client(&config).await?;
  client.query(include_str!("init_tx_event.sql")).execute().await?;
  client.query(include_str!("init_tx_message.sql")).execute().await?;
  conn::clkhs_insert(&client, TABLE_EVENTS, &[log]).await?;
  Ok(())
}

#[tokio::main]
async fn main() {
  run().await.unwrap();
}
