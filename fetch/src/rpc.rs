use ethers_core::types::BlockId;
use ethers_providers::{Provider, Middleware, JsonRpcClient};
use primitive_types::U256;
use futures::future::try_join_all;

use crate::model::{Block, Transaction, Event};

pub async fn get_block<P: JsonRpcClient, B: Into<BlockId>>(provider: Provider<P>, block: B) -> anyhow::Result<()> {
  let block_number = block.into();
  let block = provider.get_block_with_txs(block_number).await?.ok_or(anyhow::anyhow!("block not exists {block_number:?}"))?;
  let receipts = match provider.get_block_receipts(block.number.ok_or(anyhow::anyhow!("block number not available"))?).await {
    Ok(receipts) => receipts,
    _ => {
      let result = try_join_all(block.transactions.iter().map(|i|
        provider.get_transaction_receipt(i.hash)
      )).await?.into_iter().collect::<Option<Vec<_>>>().ok_or_else(|| anyhow::anyhow!("some transaction receipt not found"))?;
      result
    }
  };
  let mut txs = vec![];
  let mut evs = vec![];
  let blk = Block {
    number: block.number.unwrap_or_default().as_u64(),
    hash: block.hash.unwrap_or_default(),
    parent: block.parent_hash.into(),
    nonce: block.nonce.unwrap_or_default().to_low_u64_be(),
    state_root: block.state_root.into(),
    transactions_root: block.transactions_root.into(),
    receipts_root: block.receipts_root.into(),
    miner: block.author.unwrap_or_default(),
    // extra_data: block.extra_data,
    gas_limit: block.gas_limit.as_u64(),
    gas_used: block.gas_used.as_u64(),
    base_fee_per_gas: block.base_fee_per_gas.unwrap_or_default().into(),
    next_base_fee_per_gas: block.next_block_base_fee().unwrap_or_default().into(),
    timestamp: block.timestamp.as_u64(),
    // logs_bloom: block.logs_bloom,
    mix_hash: block.mix_hash.unwrap_or_default().into(),
    difficulty: block.difficulty.into(),
    total_difficulty: block.total_difficulty.unwrap_or_default().into(),
    ommer_count: block.uncles.len() as _,
    ommer_hash: block.uncles_hash.into(),
    transaction_count: block.transactions.len() as _,
  };
  for (tx, receipt) in block.transactions.into_iter().zip(&receipts) {
    txs.push(Transaction {
      block_number: block.number.map(|i| i.as_u64()).unwrap_or_default(),
      idx_in_block: tx.transaction_index.map(|i| i.as_u64()).unwrap_or_default(),
      hash: tx.hash.into(),
      from: tx.from.into(),
      to: tx.to.unwrap_or_else(|| todo!()),
      nonce: tx.nonce.into(),
      gas_limit: tx.gas.as_u64(), // TODO: would this overflow
      gas_used: receipt.gas_used.unwrap_or_default().as_u64(),
      gas_tip: tx.max_priority_fee_per_gas.unwrap_or_default(),
      gas_fee: tx.max_fee_per_gas.or_else(|| tx.gas_price).unwrap_or_default(),
      effective_tip: None,
      effective_fee: receipt.effective_gas_price,
      value: tx.value.into(),
      input_len: tx.input.len() as _,
      input_first_4bytes: {
        let mut i = [0u8; 4];
        i.copy_from_slice(&tx.input[..4]);
        i.into()
      },
      input_last_32bytes: {
        let mut i = [0u8; 32];
        if tx.input.len() >= 32 {
          i.copy_from_slice(&tx.input[tx.input.len()-32..]);
        } else {
          i[32-tx.input.len()..].copy_from_slice(&tx.input);
        }
        i.into()
      },
      // chain_id: todo!(),
      is_create: tx.to.is_none(),
      success: receipt.status.map(|i| i.as_u64() != 0).unwrap_or_else(|| receipt.gas_used != Some(tx.gas)),
      logs_count: receipt.logs.len() as _,
      v: tx.v.as_u64() as _,
      r: tx.r.into(),
      s: tx.s.into(),
      has_access_list: tx.access_list.is_some(),
    })
  }

  for (idx, log) in receipts.into_iter().flat_map(|i| i.logs).into_iter().enumerate() {
    evs.push(Event {
      block_number: blk.number,
      idx_in_block: idx as _,
      account: log.address.into(),
      topic_num: log.topics.len() as _,
      topic0: log.topics.get(0).cloned().unwrap_or_default().into(),
      topic1: log.topics.get(1).cloned().unwrap_or_default().into(),
      topic2: log.topics.get(2).cloned().unwrap_or_default().into(),
      topic3: log.topics.get(3).cloned().unwrap_or_default().into(),
      topic4: log.topics.get(4).cloned().unwrap_or_default().into(),
      data_len: log.data.len() as _,
      data_prefix_u256: U256::from_big_endian(&log.data[..32.min(log.data.len())]),
      data_prefix_128bytes: log.data.into_iter().take(128).collect(),
      // data: log.data,
      transaction: log.transaction_hash.unwrap_or_default().into(),
    });
  }
  Ok(())
}
