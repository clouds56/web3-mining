use primitive_types::U256;

mod conn;
mod model;

// TODO: switch to read with confy
static TABLE_EVENTS: &str = "tx_event_test";
static TABLE_TXMSGS: &str = "tx_message_test";

async fn run() -> anyhow::Result<()> {
  let log = model::TxReceiptLog {
    id: 0,
    tx_idx: 0,
    tx_message_hash: U256::zero(),
    address: U256::zero(),
    data_len: 0,
    data_prefix: U256::zero(),
    topic_num: 0,
    topic0: U256::zero(),
    topic1: U256::zero(),
    topic2: U256::zero(),
    topic3: U256::zero(),
    topic4: U256::zero(),
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
