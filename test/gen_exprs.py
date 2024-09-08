# %%
import io
from common import *
import os
import polars as pl
from pathlib import Path

enter_root_dir()

FOLDER_EXPRS = "tauri-app/src-tauri/exprs"
os.makedirs(FOLDER_EXPRS, exist_ok=True)

def test_exprs(sample_file, exprs: list[pl.Expr] | str):
  if isinstance(exprs, str):
    with open(exprs, "r") as f:
      expr_lines = f.readlines()
    exprs = [pl.Expr.deserialize(io.StringIO(x), format='json') for x in expr_lines]
  df = pl.read_parquet(Path('data').rglob(sample_file))
  if not 'timestamp' in df:
    df = df.with_columns([pl.col('height').alias('timestamp') * 15 + 1438269973])
  df = (df.filter(pl.col("timestamp") > 0)
    .with_columns([(pl.col("timestamp") * 1000).cast(pl.Datetime("ms")).cast(pl.Date).alias("_date")])
    .group_by(["_date"]).agg(exprs))
  print(df.head())
def save_jsonl(filename: str, exprs: "list[pl.Expr]", *, test_file=None):
  expr_lines = [x.meta.serialize(format='json') for x in exprs]
  if test_file:
    test_exprs(test_file, exprs)
  with open(f"{FOLDER_EXPRS}/{filename}", "w") as f:
    for i in expr_lines:
      print(i, file=f)
  if test_file:
    test_exprs(test_file, f"{FOLDER_EXPRS}/{filename}")

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
