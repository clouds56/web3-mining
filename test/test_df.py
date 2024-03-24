# %%
import os
import polars as pl
while not os.path.exists("Cargo.toml"):
  os.chdir("../")

# %%
df = pl.read_parquet("block_metrics.parquet")
df.mean()

# %%
