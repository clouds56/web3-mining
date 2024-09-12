# %%
from _common import *
import polars as pl
import matplotlib.pyplot as plt

ad = setup_datasets()

import altair as alt
alt.themes.register('custom', lambda: {
  "config": {
    "view": { "continuousWidth": 500, "continuousHeight": 300 },
    "scale": { "zero": False },
    "axisY": { "format": "e" },
  }
})
alt.themes.enable('custom')

# %%
# df = load_files("block_metrics_*.parquet")
df = load_datasets(ad, 'block_metrics')
df.mean()

# %%
df = load_datasets(ad, "uniswap_factory_events")
df.sort('height')['tx_hash'].head().to_list()

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

  df_acc = df.with_columns(
    reserve0 = pl.col('reserve0').fill_null(strategy='forward'),
    reserve1 = pl.col('reserve1').fill_null(strategy='forward'),
  ).group_by('height').agg(
    (pl.col('value_in').fill_null(0) - pl.col('value_out').fill_null(0)).sum(),
    (pl.col('amount0_in').fill_null(0) - pl.col('amount0_out').fill_null(0)).sum(),
    (pl.col('amount1_in').fill_null(0) - pl.col('amount1_out').fill_null(0)).sum(),
    pl.col('reserve0').last(),
    pl.col('reserve1').last(),
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
plotting(df, [
  (df['price']/1e5)**-2,
  (df['value'] * (df['tick_upper'] - df['tick_lower'])).cum_sum(),
  -df['fee1'].cum_sum().fill_null(strategy="forward"),
], time_column='height')
# plt.plot(df['height'], (df['price']/1e5)**-2)
# plt.plot(df['height'], (df['value'] * (df['tick_upper'] - df['tick_lower'])).cum_sum())
# plt.plot(df['height'], -df['fee1'].cum_sum().fill_null(strategy="forward"))

# %%
df_plot = df.with_columns(
  price = (pl.col('price') / 1e5) ** -2,
  value = (pl.col('value') * (pl.col('tick_upper') - pl.col('tick_lower'))).cum_sum(),
  fee1 = -pl.col('fee1').cum_sum().fill_null(strategy="forward"),
).plot
(
  df_plot.line(x='height', y='price') &
  df_plot.line(x='height', y='value') &
  df_plot.line(x='height', y='fee1')
).configure_view(continuousHeight=100)

# %%
markets = (ad
  .filter(pl.col('name').str.starts_with('pendle2_market_events'))
  .select(pl.col('name').str.strip_prefix('pendle2_market_events_'))
  ['name'])
markets = pl.DataFrame([x for x in [parse_market_name(i) for i in markets] if x is not None])
markets

# %%
for market in markets.filter(pl.col('name').str.contains('weETH_20240627'))[:1].rows(named=True):
  df = load_datasets(ad, f"pendle2_market_events_{market['name']}", with_timestamp=True)
  print(df.group_by("action").len().sort("len", descending=True))
  df_acc = df.with_columns(
    pl.col('value').fill_null(0),
    pl.col('pt_value').fill_null(0),
    pl.col('st_value').fill_null(0),
    pl.col('fee1').fill_null(0),
    pl.col('fee2').fill_null(0),
    pl.col('ln_implied_apy').forward_fill(),
  ).group_by('height').agg(
    pl.col('value').sum(),
    pl.col('pt_value').sum(),
    pl.col('st_value').sum(),
    pl.col('fee1').sum(),
    pl.col('fee2').sum(),
    pl.col('rewards').drop_nulls().count(),
    pl.col('ln_implied_apy').last(),
  ).sort('height').with_columns(
    pl.col('value').cum_sum().alias('value'),
    pl.col('pt_value').cum_sum().alias('pt_value'),
    pl.col('st_value').cum_sum().alias('st_value'),
    pl.col('fee1').cum_sum().alias('fee1'),
    pl.col('fee2').cum_sum().alias('fee2'),
    (pl.col('ln_implied_apy').exp() - 1).alias('implied_apy'),
  ).drop('ln_implied_apy')
  df_acc.write_parquet(f"data/pendle2_market_block_{market['name']}.parquet")
# %%
# df.select(
#   # pl.concat_list(df['rewards'].drop_nulls()).arr.sum()
#   pl.col('rewards').drop_nulls().cast(pl.Array(pl.Float64, shape=1)).arr.sum()
# )
# %%
