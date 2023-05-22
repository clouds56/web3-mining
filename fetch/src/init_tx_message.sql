CREATE TABLE IF NOT EXISTS tx_message_test (
  -- pub idx: u64,
  idx UInt64 NOT NULL,
  -- pub block_number: u64,
  block_number UInt64 NOT NULL,
  -- pub idx_in_block: u64,
  idx_in_block UInt64 NOT NULL,
  -- pub hash: primitive_types::U256,
  hash UInt256 NOT NULL,
  -- pub from: primitive_types::U256, // txsender
  from UInt256 NOT NULL,
  -- pub to: primitive_types::U256,
  to UInt256 NOT NULL,
  -- pub nonce: u64,
  nonce UInt64 NOT NULL,
  -- pub gas_limit: u64,
  gas_limit UInt64 NOT NULL,
  -- pub gas_used: u64, //receipt
  gas_used UInt64 NOT NULL,
  -- pub gas_priority_fee: primitive_types::U256,
  gas_priority_fee UInt256 NOT NULL,
  -- pub gas_fee: primitive_types::U256,
  gas_fee UInt256 NOT NULL,
  -- pub transfer: primitive_types::U256,
  transfer UInt256 NOT NULL,
  -- pub input_len: u64,
  input_len UInt64 NOT NULL,
  -- pub input_first_4bytes: u32,
  input_first_4bytes UInt32 NOT NULL,
  -- pub input_last_32bytes: primitive_types::U256,
  input_last_32bytes UInt256 NOT NULL,
  -- pub chain_id: Option<u64>,
  chain_id UInt64 NULL,
  -- pub is_create: bool,
  is_create bool NOT NULL,
  -- pub success: bool, // receipt
  success bool NOT NULL,
  -- pub logs_count: u64, // receipt
  logs_count UInt64 NOT NULL,

  PRIMARY KEY (idx)
) ENGINE = MergeTree;
