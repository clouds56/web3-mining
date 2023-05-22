use diesel::{Insertable, Queryable};
use serde::{Serialize, Deserialize};
use crate::{bytes::{Bytes, Bytea, Byteb}, rpc};
use ethereum_types::{H64, H160, H256, U256};

pub type TrieHash = Byteb<H256>;
pub type TxHash = Byteb<H256>;
pub type Topic = Byteb<H256>;
pub type Nonce = Byteb<H64>;
pub type BlockHash = Byteb<H256>;
pub type Address = Byteb<H160>;

pub type BigInt = Bytea<U256>;
pub type GasPrice = Bytea<U256>;

/// A log produced by a transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Insertable, Queryable)]
#[serde(deny_unknown_fields)]
pub struct Event {
  /// Block Hash
  pub block: BlockHash,
  /// Event Index in Block
  pub block_index: i32,
  /// H160
  pub account: Address,
  /// Topics
  pub topics1: Option<Topic>,
  pub topics2: Option<Topic>,
  pub topics3: Option<Topic>,
  pub topics4: Option<Topic>,
  /// Data
  pub data: Bytes,
  /// Transaction Hash
  #[diesel(column_name = transaction_)]
  pub transaction: TxHash,
  /// index in Transaction
  #[diesel(column_name = index_)]
  pub index: i32,
}

/// Description of a Transaction, pending or in the chain.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Insertable, Queryable)]
#[serde(deny_unknown_fields)]
pub struct Transaction {
  /// Hash
  pub hash: TxHash,
  /// Nonce
  pub nonce: Bytes,
  /// Index within the block.
  #[diesel(column_name = index_)]
  pub index: i32,
  /// Sender
  #[diesel(column_name = from_)]
  pub from: Address,
  /// Recipient (None when contract creation)
  #[diesel(column_name = to_)]
  pub to: Option<Address>,
  /// Transfered value
  pub value: BigInt,
  /// Gas Price
  pub gas_price: Option<GasPrice>,
  /// Block hash. None when pending.
  pub block: Option<BlockHash>,
  /// Max fee per gas
  pub max_fee_per_gas: Option<GasPrice>,
  /// miner bribe
  pub max_priority_fee_per_gas: Option<GasPrice>,
  // EffectiveTip is the actual amount of reward going to miner after considering the max fee cap.
  pub effective_tip: Option<GasPrice>,
  /// Gas amount
  pub gas: i64,
  /// Input data
  pub input: Bytes,
  /// Status: either 1 (success) or 0 (failure).
  pub status: Option<i32>,
  /// Gas used by this transaction alone.
  /// Gas used is `None` if the the client is running in light client mode.
  pub gas_used: Option<i64>,
  /// Cumulative gas used within the block after this was executed.
  pub cumulative_gas_used: Option<i64>,
  /// Effective gas price
  pub effective_gas_price: Option<GasPrice>,
  /// Contract address created, or `None` if not a deployment.
  pub created_contract: Option<Address>,
  /// ECDSA recovery id
  pub v: Option<BigInt>,
  /// ECDSA signature r, 32 bytes
  pub r: Option<BigInt>,
  /// ECDSA signature s, 32 bytes
  pub s: Option<BigInt>,
  /// Events generated within this transaction.
  pub event_count: i32,
  /// Transaction type, Some(1) for AccessList transaction, None for Legacy
  #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
  #[diesel(column_name = type_)]
  pub transaction_type: Option<i32>,
  // Access list
  // #[serde(rename = "accessList", default, skip_serializing_if = "Option::is_none")]
  // pub access_list: Option<AccessList>,
}


/// The block header type returned from GraphQL calls.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Insertable, Queryable)]
#[serde(deny_unknown_fields)]
pub struct Block {
  /// Block number. None if pending.
  pub number: i64,
  /// Hash of the block
  pub hash: BlockHash,
  /// Hash of the parent
  pub parent: Option<BlockHash>,
  /// Nonce
  pub nonce: Nonce,
  /// State root hash
  pub state_root: TrieHash,
  /// Transactions root hash
  pub transactions_root: TrieHash,
  /// Transactions receipts root hash
  pub receipts_root: TrieHash,
  /// Miner/author's address.
  pub miner: Address,
  /// Extra data
  pub extra_data: Bytes,
  /// Gas Limit
  pub gas_limit: i64,
  /// Gas Used
  pub gas_used: i64,
  /// Base fee per unit of gas (if past London)
  pub base_fee_per_gas: Option<GasPrice>,
  /// Base fee per unit of gas (if past London)
  pub next_base_fee_per_gas: Option<GasPrice>,
  /// Timestamp
  #[diesel(column_name = timestamp_)]
  pub timestamp: chrono::NaiveDateTime,
  /// Logs bloom
  pub logs_bloom: Bytes,
  /// Mix Hash
  pub mix_hash: TrieHash,
  /// Difficulty
  pub difficulty: BigInt,
  /// Difficulty
  pub total_difficulty: BigInt,
  #[serde(skip)]
  pub ommers: Vec<BlockHash>,
  /// Hash of the uncles
  pub ommer_hash: TrieHash,
  #[serde(skip)]
  pub transactions: Vec<TxHash>,
}

impl rpc::Blocks {
  pub fn to_db(self) -> (Vec<Block>, Vec<Transaction>, Vec<Event>) {
    let mut blocks = vec![];
    let mut txs = vec![];
    let mut logs = vec![];
    for block in self.blocks {
      let mut log_idx = 0;
      blocks.push(Block {
        number: block.number as _,
        hash: block.hash.into(),
        parent: block.parent.map(|i| i.hash.into()),
        nonce: block.nonce.into(),
        state_root: block.state_root.into(),
        transactions_root: block.transactions_root.into(),
        receipts_root: block.receipts_root.into(),
        miner: block.miner.address.into(),
        extra_data: block.extra_data,
        gas_limit: block.gas_limit as _,
        gas_used: block.gas_used as _,
        base_fee_per_gas: block.base_fee_per_gas.map(Into::into),
        next_base_fee_per_gas: block.next_base_fee_per_gas.map(Into::into),
        timestamp: chrono::NaiveDateTime::from_timestamp(block.timestamp.as_u64() as _, 0),
        logs_bloom: block.logs_bloom,
        mix_hash: block.mix_hash.into(),
        difficulty: block.difficulty.into(),
        total_difficulty: block.total_difficulty.into(),
        ommers: block.ommers.iter().map(|i| i.hash.into()).collect(),
        ommer_hash: block.ommer_hash.into(),
        transactions: block.transactions.iter().map(|i| i.hash.into()).collect(),
      });
      for tx in block.transactions {
        txs.push(Transaction {
            hash: tx.hash.into(),
            nonce: <[u8; 32]>::from(tx.nonce).into(),
            index: tx.index as _,
            from: tx.from.address.into(),
            to: tx.to.map(|i| i.address.into()),
            value: tx.value.into(),
            gas_price: tx.gas_price.map(Into::into),
            block: tx.block_hash.map(|i| i.hash.into()),
            max_fee_per_gas: tx.max_fee_per_gas.map(Into::into),
            max_priority_fee_per_gas: tx.max_priority_fee_per_gas.map(Into::into),
            effective_tip: tx.effective_tip.map(Into::into),
            gas: tx.gas.as_u64() as _,
            input: tx.input,
            status: tx.status.map(|i| i.as_u64() as _),
            gas_used: tx.gas_used.map(|i| i as _),
            cumulative_gas_used: tx.cumulative_gas_used.map(|i| i as _),
            effective_gas_price: tx.effective_gas_price.map(Into::into),
            created_contract: tx.contract.map(|i| i.address.into()),
            v: tx.v.map(|i| U256::from(i.as_u64()).into()),
            r: tx.r.map(Into::into),
            s: tx.s.map(Into::into),
            event_count: tx.logs.len() as _,
            transaction_type: tx.transaction_type.map(|i| i as _),
        });
        for (idx, log) in tx.logs.into_iter().enumerate() {
          logs.push(Event {
            block: block.hash.into(),
            block_index: log_idx,
            account: log.account.address.into(),
            topics1: log.topics.get(0).cloned().map(|i| i.into()),
            topics2: log.topics.get(1).cloned().map(|i| i.into()),
            topics3: log.topics.get(2).cloned().map(|i| i.into()),
            topics4: log.topics.get(3).cloned().map(|i| i.into()),
            data: log.data,
            transaction: tx.hash.into(),
            index: idx as _,
          });
          log_idx += 1;
        }
      }
    }
    (blocks, txs, logs)
  }
}
