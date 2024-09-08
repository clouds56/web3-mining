# %%
from common import *
import polars as pl

enter_root_dir()
ad = all_datasets()

# %%
tokens = pl.read_csv("test/tokens.csv", has_header=False, new_columns=["name", "address"])
pairs = pl.concat([
  tokens[:4].select(name0 = pl.col("name"), address0 = pl.col("address").str.to_lowercase())
    .join(tokens.select(name1 = pl.col("name"), address1 = pl.col("address").str.to_lowercase()), how='cross'),
  tokens.select(name0 = pl.col("name"), address0 = pl.col("address").str.to_lowercase())
    .join(tokens[:4].select(name1 = pl.col("name"), address1 = pl.col("address").str.to_lowercase()), how='cross'),
]).filter(pl.col("address0") != pl.col("address1")).unique()
pairs

# %%
df = load_datasets(ad, "uniswap_factory_events")
df = df.filter(
  pl.col('contract').str.to_lowercase() == '0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f'.lower()
).with_columns(
  address0 = pl.col('token0').str.to_lowercase(),
  address1 = pl.col('token1').str.to_lowercase(),
).join(pairs, on=("address0", "address1")).select(
  ['height', 'contract', 'token0', 'token1', 'name0', 'name1', 'pair']
)
for row in df.rows(named=True):
  print(f"""
[uniswap_pair_events.{row['name0']}_{row['name1']}]
contract = "{row['pair']}"
created = {row['height']}
  """.strip() + '\n')

# %%
df = load_datasets(ad, "uniswap3_factory_events")
df = df.filter(
  pl.col('contract').str.to_lowercase() == '0x1F98431c8aD98523631AE4a59f267346ea31F984'.lower()
).with_columns(
  address0 = pl.col('token0').str.to_lowercase(),
  address1 = pl.col('token1').str.to_lowercase(),
).join(pairs, on=("address0", "address1")).select(
  ['height', 'contract', 'token0', 'token1', 'name0', 'name1', 'pair', 'fee', 'tick_spacing']
)
for row in df.rows(named=True):
  print(f"""
[uniswap3_pair_events.{row['name0']}_{row['name1']}_{row['fee']}_{row['tick_spacing']}]
contract = "{row['pair']}"
created = {row['height']}
  """.strip() + '\n')

# %%
