use ethers_core::types::H32;
use primitive_types::{U256, H160, H256};
use serde::{Serialize, Deserialize, Deserializer, Serializer};
use clickhouse::Row;
use serde_with::{DeserializeAs, SerializeAs};

macro_rules! serde_bytes {
  ($proxy:ident, $T:ty, $as:ty) => {
    serde_bytes!($proxy, $T, $as, i, <$T>::from(i), <$T>::into(i.clone()));
  };
  ($proxy:ident, $T:ty, $as:ty, $t:ident, $from:expr, $to:expr) => {
    #[allow(non_camel_case_types)]
    struct $proxy;

    impl<'de> DeserializeAs<'de, $T> for $proxy {
      fn deserialize_as<D>(deserializer: D) -> Result<$T, D::Error>
      where
        D: Deserializer<'de>,
      {
        let $t = <$as>::deserialize(deserializer)?;
        Ok($from)
      }
    }
    impl SerializeAs<$T> for $proxy {
      fn serialize_as<S>($t: &$T, serializer: S) -> Result<S::Ok, S::Error>
      where
        S: Serializer,
      {
        let u: $as = $to;
        u.serialize(serializer)
      }
    }
  };
}

serde_bytes!(h160, H160, [u8; 20]);
serde_bytes!(h256, H256, [u8; 32]);
serde_bytes!(u256, U256, [u8; 32], i, U256::from_little_endian(&i), { let mut buf = [0u8; 32]; i.to_little_endian(&mut buf); buf });
// serde_bytes!(impl h160 for primitive_types::H160);
// serde_bytes!(impl h32 for ethers_core::types::H32);

// pub type H160Proxy = AsBytes<H160, [u8; 20]>;
// pub type H256Proxy = AsBytes<H256, [u8; 32]>;

pub type TrieHash = H256; // H256
pub type TxHash = H256; // H256
pub type Topic = H256; // H256
pub type BlockHash = H256; // H256
pub type Address = H160; // H160

pub type BigInt = U256;
pub type GasPrice = U256;

/// A log produced by a transaction.
#[serde_with::serde_as]
#[derive(Debug, Clone, Row, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
  pub block_number: u64,
  pub idx_in_block: u64,
  /// Transaction Hash
  #[serde_as(as = "h256")]
  pub transaction: TxHash,
  #[serde_as(as = "h160")]
  pub account: Address,
  pub data_len: u64,
  #[serde_as(as = "u256")]
  pub data_prefix_u256: U256,
  pub data_prefix_128bytes: Vec<u8>,
  pub topic_num: u8,
  #[serde_as(as = "h256")]
  pub topic0: Topic,
  #[serde_as(as = "h256")]
  pub topic1: Topic,
  #[serde_as(as = "h256")]
  pub topic2: Topic,
  #[serde_as(as = "h256")]
  pub topic3: Topic,
  #[serde_as(as = "h256")]
  pub topic4: Topic,
}

/// Description of a Transaction, pending or in the chain.
#[serde_with::serde_as]
#[derive(Debug, Clone, Row, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Transaction {
  pub block_number: u64,
  pub idx_in_block: u64,
  #[serde_as(as = "h256")]
  pub hash: TxHash,
  /// Sender
  #[serde_as(as = "h160")]
  pub from: Address, // txsender
  /// Recipient (or contract address when creation)
  #[serde_as(as = "h160")]
  pub to: Address,
  #[serde_as(as = "u256")]
  pub nonce: U256,
  pub gas_limit: u64,
  pub gas_used: u64, //receipt
  /// max_priority_fee_per_gas
  #[serde_as(as = "u256")]
  pub gas_tip: GasPrice,
  /// gas_price or max_fee_per_gas
  #[serde_as(as = "u256")]
  pub gas_fee: GasPrice,
  // effective_tip is the actual amount of reward going to miner after considering the max fee cap.
  pub effective_tip: Option<GasPrice>,
  /// Effective gas price
  pub effective_fee: Option<GasPrice>,
  /// Transfered value
  #[serde_as(as = "u256")]
  pub value: BigInt,
  pub input_len: u64,
  pub input_first_4bytes: H32,
  pub input_last_32bytes: [u8; 32],
  // pub chain_id: Option<u64>,
  pub is_create: bool,
  pub success: bool, // receipt
  pub logs_count: u64, // receipt
  /// ECDSA recovery id
  pub v: u8,
  /// ECDSA signature r, 32 bytes
  pub r: BigInt,
  /// ECDSA signature s, 32 bytes
  pub s: BigInt,
  /// Transaction type, Some(1) for AccessList transaction, None for Legacy
  pub has_access_list: bool,
}

/// The block header type returned from GraphQL calls.
#[serde_with::serde_as]
#[derive(Debug, Clone, Row, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Block {
  /// Block number. None if pending.
  pub number: u64,
  #[serde_as(as = "h256")]
  /// Hash of the block
  pub hash: BlockHash,
  #[serde_as(as = "Option<h256>")]
  /// Hash of the parent
  pub parent: Option<BlockHash>,
  /// Nonce
  pub nonce: u64,
  /// State root hash
  #[serde_as(as = "h256")]
  pub state_root: TrieHash,
  /// Transactions root hash
  #[serde_as(as = "h256")]
  pub transactions_root: TrieHash,
  /// Transactions receipts root hash
  #[serde_as(as = "h256")]
  pub receipts_root: TrieHash,
  /// Miner/author's address.
  #[serde_as(as = "h160")]
  pub miner: Address,
  /// Extra data
  // pub extra_data: Bytes,
  /// Gas Limit
  pub gas_limit: u64,
  /// Gas Used
  pub gas_used: u64,
  /// Base fee per unit of gas (if past London)
  #[serde_as(as = "u256")]
  pub base_fee_per_gas: GasPrice,
  /// Base fee per unit of gas (if past London)
  #[serde_as(as = "u256")]
  pub next_base_fee_per_gas: GasPrice,
  /// Timestamp
  pub timestamp: u64,
  /// Logs bloom
  // pub logs_bloom: Bytes,
  /// Mix Hash
  pub mix_hash: TrieHash,
  /// Difficulty
  pub difficulty: BigInt,
  /// Difficulty
  pub total_difficulty: BigInt,
  pub ommer_count: u64,
  /// Hash of the uncles
  #[serde_as(as = "h256")]
  pub ommer_hash: TrieHash,
  pub transaction_count: u64,
}
