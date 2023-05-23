
# %%
def hex_col(s): return f"hex({s}) as {s}" # reinterpretAsFixedString
def int_col(s): return s
def array_u8_col(s): return f"arrayStringConcat(arrayMap(x->hex(x),{s}),'') as {s}"
EV_COLUMN_DEF = [
  ("address", hex_col),
  ("data_len", int_col),
  ("data_prefix32", hex_col),
  ("data_prefix128", array_u8_col),
  ("topic_num", int_col),
  ("topic0", hex_col),
  ("topic1", hex_col),
  ("topic2", hex_col),
  ("topic3", hex_col),
  ("topic4", hex_col),
]
EV_COLUMNS = [v(i) for i, v in EV_COLUMN_DEF]
EV_CREATE_HASH = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9
EV_SWAP_HASH = 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822
EX_BURN_HASH = 0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496
EV_TRANSFER_HASH = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
# %% Show most frequent pools
df = read_spark().option("query", f"select {hex_col('address')}, count(*) as count from tx_log.tx_event where topic0 = toUInt256('{EV_SWAP_HASH}') GROUP BY address ORDER BY count DESC").load()
df.show()

# %%
# https://github.com/Uniswap/v2-core/blob/master/contracts/UniswapV2Factory.sol#L13
# event PairCreated(address indexed token0, address indexed token1, address pair, uint);
df = read_spark().option("query", f"select {','.join(EV_COLUMNS)} from tx_log.tx_event as p0 where p0.topic0 = toUInt256('{EV_CREATE_HASH}') and p0.data_len = 64 and p0.topic_num = 3").load()
df.withColumn('pair', col())

# %%
df.show()
# %%
df.write.options(header=True).mode("overwrite").csv('create_pair')
# %%
df.groupBy('address').count().show()
# %%
df = read_spark().option("query", "select reinterpretAsFixedString(129) as s").load()
df.show()

# %%
