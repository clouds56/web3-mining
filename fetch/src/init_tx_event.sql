CREATE TABLE IF NOT EXISTS tx_event_test (
  -- pub id: u64,
  id UInt64 NOT NULL,
  -- pub tx_idx: u64,
  tx_idx UInt64 NOT NULL,
  -- pub tx_message_hash: primitive_types::U256,
  tx_message_hash UInt256 NOT NULL,

  -- pub address: primitive_types::U256,
  address UInt256 NOT NULL,

  -- pub data_len: u64,
  data_len UInt64 NOT NULL,
  -- pub data_prefix: primitive_types::U256,
  data_prefix UInt256,

  -- pub topic_num: u8,
  topic_num UInt8 NOT NULL,
  -- pub topic0: primitive_types::U256,
  topic0 UInt256,
  -- pub topic1: primitive_types::U256,
  topic1 UInt256,
  -- pub topic2: primitive_types::U256,
  topic2 UInt256,
  -- pub topic3: primitive_types::U256,
  topic3 UInt256,
  -- pub topic4: primitive_types::U256,
  topic4 UInt256,

  PRIMARY KEY(id)
) ENGINE = MergeTree;
