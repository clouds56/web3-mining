# %%
from common import *
from typing import List
import polars as pl
import matplotlib.pyplot as plt
enter_root_dir()

# %%
ad = all_datasets()

# %%
# df = load_files("block_metrics_*.parquet")
df = load_datasets(ad, 'block_metrics')
df.mean()

# %%
df = load_datasets(ad, "uniswap_factory_events")
df.sort('height')['tx_hash'].head().to_list()

# %%
df = load_files("uniswap_pair_old_*.parquet")
if df:
  dfg = df.group_by('topic0').agg([pl.first('tx_hash'), pl.count('height').alias('count')]).sort('count', descending=True)
  list(zip(*[list(dfg[col]) for col in dfg.columns]))

# %%
pairs = (ad
  .filter(pl.col('name').str.starts_with('uniswap_pair_events'))
  .select(pl.col('name').str.strip_prefix('uniswap_pair_events_').alias('pair'))
  ['pair'])
pairs

# %%
for pair in pairs:
  df = load_datasets(ad, "uniswap_pair_events_" + pair)
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
df = load_datasets(ad, 'uniswap3_factory_events')
df.filter(
  (df['contract'] == '0x1F98431c8aD98523631AE4a59f267346ea31F984')
  & (df['token0'] == '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599')
  & (df['token1'] == '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
)
df.group_by(['fee', 'tick_spacing']).count().sort('count')
df.filter(df['fee'] == 1)['pair'].to_list()

# %%
df = load_datasets(ad, 'uniswap3_pair_events_wbtc_weth')
df[0]['tx_hash'].to_list()
df.group_by('action').count()
# %%
plt.plot(df['height'], (df['price']/1e5)**-2)
# %%
plt.plot(df['height'], (df['value'] * (df['tick_upper'] - df['tick_lower'])).cum_sum())
# %%
plt.plot(df['height'], -df['fee1'].cum_sum().fill_null(strategy="forward"))
# %%
