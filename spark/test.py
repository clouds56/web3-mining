# %%
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession

def read_spark(spark:SparkSession=spark):
  return spark.read.format('jdbc').option('driver',driver).option('url',url).option('user',user).option('password',password)

# %%
pgDF = read_spark().option('dbtable', 'system.databases').load()
print("show system databases:", pgDF.show())

# count idx
df = read_spark().option("query", "select count(idx), max(block_number) from tx_log.tx_message ").load()
print('count idx:', df.show())

# %%
def hex_col(s): return f"hex({s}) as {s}" # reinterpretAsFixedString
def int_col(s): return s
def array_u8_col(s): return f"arrayStringConcat(arrayMap(x->hex(x),{s}),'') as {s}"
EV_COLUMN_DEF = [
  ("id", int_col),
  ("tx_idx", int_col),
  ("tx_message_hash", hex_col),
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
EV_MINT_HASH = 0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f
EV_SWAP_HASH = 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822
EV_BURN_HASH = 0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496
EV_TRANSFER_HASH = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
# %% Show most frequent pools
df = read_spark().option("query", f"select {hex_col('address')}, count(*) as count from tx_log.tx_event where topic0 = toUInt256('{EV_SWAP_HASH}') GROUP BY address ORDER BY count DESC").load()
df.show()

# %%
# https://github.com/Uniswap/v2-core/blob/v1.0.1/contracts/UniswapV2Factory.sol#L13
# event PairCreated(address indexed token0, address indexed token1, address pair, uint);
df = read_spark().option("query", f"select {','.join(EV_COLUMNS)} from tx_log.tx_event as p0 where p0.topic0 = toUInt256('{EV_CREATE_HASH}') and p0.data_len = 64 and p0.topic_num = 3").load()
df = df.orderBy("tx_idx", "id").selectExpr(
  "tx_idx as tx_idx", "tx_message_hash as tx_hash", "address as contract",
  "lpad(topic1,40,'0') as token0", "lpad(topic2,40,'0') as token1", "substring(data_prefix128, 25, 40) as pair", "conv(substring(data_prefix128, 65, 64), 16, 10) as length"
)
df.write.options(header=True).mode("overwrite").csv('event_PairCreated')
df.count()

# %%
# https://github.com/Uniswap/v2-core/blob/v1.0.1/contracts/UniswapV2Pair.sol#L49
# event Mint(address indexed sender, uint amount0, uint amount1);
df = read_spark().option("query", f"select {','.join(EV_COLUMNS)} from tx_log.tx_event as p0 where p0.topic0 = toUInt256('{EV_MINT_HASH}') and p0.data_len = 64 and p0.topic_num = 2").load()
df = df.orderBy("tx_idx", "id").selectExpr(
  "tx_idx as tx_idx", "tx_message_hash as tx_hash", "address as contract",
  "lpad(topic1,40,'0') as sender", "conv(substring(data_prefix128,1,64), 16, 10) as amount0", "conv(substring(data_prefix128,65,64), 16, 10) as amount1"
)
df.write.options(header=True).mode("overwrite").csv('event_Mint')
df.count()

# %%
# https://github.com/Uniswap/v2-core/blob/v1.0.1/contracts/UniswapV2Pair.sol#L50
# event Burn(address indexed sender, uint amount0, uint amount1, address indexed to);
df = read_spark().option("query", f"select {','.join(EV_COLUMNS)} from tx_log.tx_event as p0 where p0.topic0 = toUInt256('{EV_BURN_HASH}') and p0.data_len = 64 and p0.topic_num = 3").load()
df = df.orderBy("tx_idx", "id").selectExpr(
  "tx_idx as tx_idx", "tx_message_hash as tx_hash", "address as contract",
  "lpad(topic1,40,'0') as sender", "conv(substring(data_prefix128,1,64), 16, 10) as amount0", "conv(substring(data_prefix128,65,64), 16, 10) as amount1", "lpad(topic2,40,'0') as to"
)
df.write.options(header=True).mode("overwrite").csv('event_Burn')
df.count()

# %%
# https://github.com/Uniswap/v2-core/blob/v1.0.1/contracts/UniswapV2Pair.sol#L51-L58
# event Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to);
df = read_spark().option("query", f"select {','.join(EV_COLUMNS)} from tx_log.tx_event as p0 where p0.topic0 = toUInt256('{EV_SWAP_HASH}') and p0.data_len = 128 and p0.topic_num = 3").load()
df = df.orderBy("tx_idx", "id").selectExpr(
  "tx_idx as tx_idx", "tx_message_hash as tx_hash",
  "address as contract",
  "lpad(topic1,40,'0') as sender",
  "conv(substring(data_prefix128,1,64), 16, 10) as amount0In",
  "conv(substring(data_prefix128,65,64), 16, 10) as amount1In",
  "conv(substring(data_prefix128,129,64), 16, 10) as amount0Out",
  "conv(substring(data_prefix128,193,64), 16, 10) as amount1Out",
  "lpad(topic2,40,'0') as to",
)
df.write.options(header=True).mode("overwrite").csv('event_Swap')
df.count()

# %%
df = spark.read.options(header=True).csv('event_Swap')
df.groupBy('address').count().orderBy('count', ascending=False).show()

# %%
