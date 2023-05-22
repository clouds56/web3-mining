use primitive_types::U256;
use serde::{Serialize, Deserialize};
use clickhouse::Row;

mod u256 {
  use primitive_types::U256;
  use serde::{
    de::{Deserialize, Deserializer},
    ser::{Serialize, Serializer},
  };

  pub fn serialize<S: Serializer>(u: &U256, serializer: S) -> Result<S::Ok, S::Error> {
    let mut buf: [u8; 32] = [0; 32];
    u.to_little_endian(&mut buf);
    buf.serialize(serializer)
  }

  pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
  where
    D: Deserializer<'de>,
  {
    let u: [u8; 32] = Deserialize::deserialize(deserializer)?;
    Ok(U256::from_little_endian(&u))
  }
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct TxReceiptLog {
  pub id: u64,
  pub tx_idx: u64,
  #[serde(with = "u256")]
  pub tx_message_hash: U256,
  #[serde(with = "u256")]
  pub address: U256,
  pub data_len: u64,
  #[serde(with = "u256")]
  pub data_prefix: U256,
  pub topic_num: u8,
  #[serde(with = "u256")]
  pub topic0: U256,
  #[serde(with = "u256")]
  pub topic1: U256,
  #[serde(with = "u256")]
  pub topic2: U256,
  #[serde(with = "u256")]
  pub topic3: U256,
  #[serde(with = "u256")]
  pub topic4: U256,
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct TxMessage{
  pub idx: u64,
  pub block_number: u64,
  pub idx_in_block: u64,
  #[serde(with = "u256")]
  pub hash: U256,
  #[serde(with = "u256")]
  pub from: U256, // txsender
  #[serde(with = "u256")]
  pub to: U256,
  pub nonce: u64,
  pub gas_limit: u64,
  pub gas_used: u64, //receipt
  #[serde(with = "u256")]
  pub gas_priority_fee: U256,
  #[serde(with = "u256")]
  pub gas_fee: U256,
  #[serde(with = "u256")]
  pub transfer: U256,
  pub input_len: u64,
  pub input_first_4bytes: u32,
  #[serde(with = "u256")]
  pub input_last_32bytes: U256,
  pub chain_id: Option<u64>,
  pub is_create: bool,
  pub success: bool, // receipt
  pub logs_count: u64, // receipt
}
