# %%
from common import *
import polars as pl

enter_root_dir()
ad = all_datasets()

# %%
import tomllib
with open('data/stage.toml', 'rb') as f:
  stage = tomllib.load(f)

def toml_to_df(obj: dict, tag: str):
  data = [{'name': k} | v for k, v in obj.get(tag, {}).items()]
  df = pl.DataFrame(data)
  if len(df) == 0:
    df = pl.DataFrame(schema=[('name', pl.String)])
  return df
stage

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
).with_columns(
  name = pl.col('name0') + "_" + pl.col('name1')
).join(toml_to_df(stage, 'uniswap_pair_events'), on="name", how='anti')
print("# Uniswap V2 Pairs:", len(df))
for row in df.rows(named=True):
  print(f"""
[uniswap_pair_events.{row['name']}]
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
).with_columns(
  name = pl.col('name0') + "_" + pl.col('name1') + "_" +
    pl.col('fee').cast(pl.String) + "_" + pl.col('tick_spacing').cast(pl.String)
).join(toml_to_df(stage, 'uniswap3_pair_events'), on="name", how='anti')
print("# Uniswap V3 Pairs:", len(df))
for row in df.rows(named=True):
  print(f"""
[uniswap3_pair_events.{row['name']}]
contract = "{row['pair']}"
created = {row['height']}
  """.strip() + '\n')

# %%
import math
def trim_pt_name(s: str):
  return "-".join(s.split("-")[1:-1])
df = load_datasets(ad, "pendle2_market_factory_events", with_timestamp=True)
df = df.with_columns(
  time_to_expiry = (pl.col('expiry') - pl.col('timestamp')) / 86400.0,
).with_columns(
  scalar = pl.col('scalar') * 365. / pl.col('time_to_expiry'),
).with_columns(
  max_rate = pl.col('anchor') + math.log(9) / pl.col('scalar'),
  min_rate = pl.col('anchor') - math.log(9) / pl.col('scalar'),
).with_columns(
  min_apy = pl.col('min_rate') ** (365 / pl.col('time_to_expiry')),
  expected_apy = pl.col('anchor') ** (365 / pl.col('time_to_expiry')),
  max_apy = pl.col('max_rate') ** (365 / pl.col('time_to_expiry')),
).with_columns(
  token_str = pl.col('pt_name').map_elements(trim_pt_name, return_dtype=pl.String),
  expiry_str = pl.from_epoch(pl.col('expiry')).cast(pl.Date).cast(pl.String).str.replace_all('-', ''),
  min_apy_str = ((pl.col('min_apy') - 1) * 1000).round().cast(pl.Int64).cast(pl.String),
  max_apy_str = ((pl.col('max_apy') - 1) * 1000).round().cast(pl.Int64).cast(pl.String),
  fee_rate_str = (pl.col('fee_rate') * 10000).round().cast(pl.Int64).cast(pl.String),
).with_columns(
  name = pl.col('token_str') + "_" + pl.col('expiry_str') + "_" + pl.col('min_apy_str') + "_" + pl.col('max_apy_str') + "_" + pl.col('fee_rate_str'),
).drop(['token_str', 'expiry_str', 'min_apy_str', 'max_apy_str', 'fee_rate_str']).filter(
  pl.col('name').is_not_null()
).join(toml_to_df(stage, 'pendle2_market_events'), on="name", how='anti')
print("# Pendle V2 Markets:", len(df))
for row in df.rows(named=True):
  print(f"""
[pendle2_market_events.{row['name'].replace('+', 'p')}]
contract = "{row['contract']}"
created = {row['height']}
  """.strip() + '\n')

# %%
