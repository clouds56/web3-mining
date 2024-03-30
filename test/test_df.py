# %%
import os
import polars as pl
from pathlib import Path
while not os.path.exists("Cargo.toml"):
  os.chdir("../")

# %%
files = Path("data").rglob("block_metrics_*.parquet")
dfa: pl.DataFrame = None
for file in files:
  df = pl.read_parquet(file)
  if dfa is None:
    dfa = df
  else:
    dfa.vstack(df)
df = dfa
df = pl.read_parquet("block_metrics.parquet")
df.mean()

# %%
files = Path("data").rglob("uniswap_pair_*.parquet")
file = list(files)[0]
df = pl.read_parquet(file)
dfg = df.group_by('topic0').agg([pl.first('tx_hash'), pl.count('height').alias('count')]).sort('count', descending=True)
list(zip(*[list(dfg[col]) for col in dfg.columns]))

# %%
