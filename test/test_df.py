# %%
import os
from typing import List
import itertools
import polars as pl
import matplotlib.pyplot as plt
from pathlib import Path
while not os.path.exists("Cargo.toml"):
  os.chdir("../")
FOLDER_EXPRS = "tauri-app/src-tauri/exprs"
os.makedirs(FOLDER_EXPRS, exist_ok=True)

def load_files(files) -> pl.DataFrame:
  if isinstance(files, str):
    files = Path("data").rglob(files)
  dfa: pl.DataFrame = None
  for file in files:
    df = pl.read_parquet(file)
    if dfa is None:
      dfa = df
    else:
      dfa = dfa.vstack(df)
  return dfa

# %%
def try_int(s: str):
  try:
    return int(s)
  except:
    return None
files = pl.DataFrame({
  'path': Path("data").rglob("*.parquet")
}).with_columns([
  pl.col('path').map_elements(lambda x: x.name.split(".")[0]).alias('prefix'),
  pl.col('path').map_elements(lambda x: try_int(x.name.split(".")[1])).alias('idx'),
  pl.col('path').map_elements(lambda x: str(x)).alias('path'),
]).with_columns([
  pl.col('prefix').map_elements(lambda x: try_int(x.split("_")[-1])).alias('cut'),
]).sort('prefix', 'idx')
datasets = files.group_by('prefix').agg([
  pl.first('cut'),
  pl.max('idx').alias('max'),
  pl.count('idx').alias('count'),
  pl.col('path').alias('paths'),
]).with_columns([
  pl.col('prefix')
    .str.strip_suffix(pl.col('cut').cast(pl.String))
    .str.strip_suffix('_')
    .fill_null(pl.col('prefix'))
    .alias('name')
]).sort('name')
def load_datasets(name: str) -> pl.DataFrame:
  filenames = datasets.filter(pl.col('name') == name)['paths'].explode()
  prefix = "".join([list(x)[0] for x in itertools.takewhile(lambda x: len(x) == 1, map(set, zip(*filenames)))])
  print("load", prefix, list(filenames.str.strip_prefix(prefix)))
  return load_files(filenames)
datasets

# %%
# df = load_files("block_metrics_*.parquet")
df = load_datasets('block_metrics')
df.mean()

# %%
df = load_datasets("uniswap_factory_events")
df.sort('height')['tx_hash'].head().to_list()

# %%
df = load_files("uniswap_pair_old_*.parquet")
if df:
  dfg = df.group_by('topic0').agg([pl.first('tx_hash'), pl.count('height').alias('count')]).sort('count', descending=True)
  list(zip(*[list(dfg[col]) for col in dfg.columns]))

# %%
pairs = (datasets
  .filter(pl.col('name').str.starts_with('uniswap_pair_events'))
  .select(pl.col('name').str.strip_prefix('uniswap_pair_events_').alias('pair'))
  ['pair'])
pairs

# %%
for pair in pairs:
  df = load_datasets("uniswap_pair_events_" + pair)
  print(df.group_by("action").len().sort("len", descending=True))

  df_acc = df.group_by('height').agg(
    (pl.col('value_in').fill_null(0) - pl.col('value_out').fill_null(0)).sum(),
    (pl.col('amount0_in').fill_null(0) - pl.col('amount0_out').fill_null(0)).sum(),
    (pl.col('amount1_in').fill_null(0) - pl.col('amount1_out').fill_null(0)).sum(),
    pl.col('reserve0').fill_null(strategy='forward').last(),
    pl.col('reserve1').fill_null(strategy='forward').last(),
  ).sort('height').with_columns(
    pl.col('value_in').cum_sum().alias('value'),
  )
  df_acc.write_parquet(f"data/uniswap_pair_block_{pair}.parquet")

  # plt.plot(df_acc['height'], df_acc['value0'])
  plt.plot(df_acc['height'], (df_acc['reserve0']*df_acc['reserve1']).sqrt()/df_acc['value'], label=pair)
plt.legend()
plt.show()

# %%
def save_jsonl(filename, exprs: List[pl.Expr]):
  expr_lines = [x.meta.serialize() for x in exprs]
  with open(f"{FOLDER_EXPRS}/{filename}", "w") as f:
    for i in expr_lines:
      print(i, file=f)
save_jsonl("bm.jsonl", [
  pl.col("total_eth").sum().alias("total_eth"),
  pl.col("tx_count").cast(pl.UInt64).sum().alias("tx_count"),
  pl.col("total_fee").mean().alias("total_fee"),
  pl.col("gas_used").mean().alias("gas_used"),
  pl.col("fee_per_gas").mean().alias("fee_per_gas:mean"),
  pl.col("fee_per_gas").median().alias("fee_per_gas:median"),
])
save_jsonl("upair.jsonl", [
  pl.sum("value_in"),
  pl.sum("amount0_in"),
  pl.sum("amount1_in"),
  pl.last("reserve0"),
  pl.last("reserve1"),
  pl.last("value"),
  ((pl.col('reserve0') * pl.col("reserve1")).sqrt() / pl.col("value")).last().alias("scale"),
])

# %%
