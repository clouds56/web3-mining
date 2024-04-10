# %%
import os
import polars as pl
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
df = load_files("block_metrics_*.parquet")
df.mean()

# %%
df = load_files("uniswap_factory_*.parquet")
df.sort('height')['tx_hash'].head().to_list()

# %%
files = Path("data").rglob("uniswap_pair_old_*.parquet")
file = list(files)[0]
df = pl.read_parquet(file)
dfg = df.group_by('topic0').agg([pl.first('tx_hash'), pl.count('height').alias('count')]).sort('count', descending=True)
list(zip(*[list(dfg[col]) for col in dfg.columns]))

# %%
files = Path("data").rglob("uniswap_pair_0x*.parquet")
file = list(files)[0]
df = pl.read_parquet(file)
df.group_by("action").count().sort("count", descending=True)

# %%
df_clean = df.group_by('height').agg(
  (pl.col('value0_in').fill_null(0) - pl.col('value0_out').fill_null(0)).sum(),
  (pl.col('amount0_in').fill_null(0) - pl.col('amount0_out').fill_null(0)).sum(),
  (pl.col('amount1_in').fill_null(0) - pl.col('amount1_out').fill_null(0)).sum(),
  pl.col('reserve0').fill_null(strategy='forward').last(),
  pl.col('reserve1').fill_null(strategy='forward').last(),
).sort('height').with_columns(
  pl.col('value0_in').cum_sum().alias('value0'),
)
df_clean.write_parquet("uniswap_pair_0x0.parquet")

# %%
exprs = [
  pl.col("total_eth").sum().alias("total_eth"),
  pl.col("tx_count").cast(pl.UInt64).sum().alias("tx_count"),
  pl.col("total_fee").mean().alias("total_fee"),
  pl.col("gas_used").mean().alias("gas_used"),
  pl.col("fee_per_gas").mean().alias("fee_per_gas:mean"),
  pl.col("fee_per_gas").median().alias("fee_per_gas:median"),
]
expr_lines = [x.meta.serialize() for x in exprs]
with open(f"{FOLDER_EXPRS}/bm.jsonl", "w") as f:
  for i in expr_lines:
    print(i, file=f)

# %%
import matplotlib.pyplot as plt
# plt.plot(df_clean['height'], df_clean['value0'])
plt.plot(df_clean['height'], (df_clean['reserve0']*df_clean['reserve1']).sqrt()/df_clean['value0'])

# %%
