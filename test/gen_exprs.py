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

def test_exprs(sample_file, exprs):
  if isinstance(exprs, str):
    with open(f"{FOLDER_EXPRS}/{exprs}", "r") as f:
      expr_lines = f.readlines()
    exprs = [pl.Expr.deserialize(x) for x in expr_lines]
  df = pl.read_parquet(Path('data').rglob(sample_file))
  if not 'timestamp' in df:
    df = df.with_columns([pl.col('height').alias('timestamp') * 15 + 1438269973])
  df = (df.filter(pl.col("timestamp") > 0)
    .with_columns([(pl.col("timestamp") * 1000).cast(pl.Datetime("ms")).cast(pl.Date).alias("_date")])
    .group_by(["_date"]).agg(exprs))
  print(df.head())
def save_jsonl(filename, exprs: "List[pl.Expr]", *, test_file=None):
  expr_lines = [x.meta.serialize() for x in exprs]
  if test_file:
    test_exprs(test_file, exprs)
  with open(f"{FOLDER_EXPRS}/{filename}", "w") as f:
    for i in expr_lines:
      print(i, file=f)

# %%
save_jsonl("bm.jsonl", [
  pl.col("total_eth").sum().alias("total_eth"),
  pl.col("tx_count").cast(pl.UInt64).sum().alias("tx_count"),
  pl.col("total_fee").mean().alias("total_fee"),
  pl.col("gas_used").mean().alias("gas_used"),
  pl.col("fee_per_gas").mean().alias("fee_per_gas:mean"),
  pl.col("fee_per_gas").median().alias("fee_per_gas:median"),
], test_file="block_metrics_*.parquet")
save_jsonl("upair.jsonl", [
  pl.sum("value_in"),
  pl.sum("amount0_in"),
  pl.sum("amount1_in"),
  pl.last("reserve0"),
  pl.last("reserve1"),
  pl.last("value"),
  ((pl.col('reserve0') * pl.col("reserve1")).sqrt() / pl.col("value")).last().alias("scale"),
], test_file="uniswap_pair_block_*.parquet")

# %%
