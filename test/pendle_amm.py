# %%
from _common import *
from _models.pendle import *
from _models.swap import *
import polars as pl
import numpy as np
import matplotlib.pyplot as plt

pair = "usdc_weth"
ad = setup_datasets()

# %%
df = load_datasets(ad, f"uniswap_pair_block_{pair}", with_timestamp=True)
df = df.with_columns(
  fee_index = (df['reserve0'] * df['reserve1']).sqrt() / df['value'],
  price = df['reserve0'] / df['reserve1'],
).sort('height')
plotting(df, 'price', 'fee_index')

# %%
from polars._typing import IntoExpr as pl_IntoExpr
def diff_with_window(df: pl.DataFrame, *columns: str, window = 7 * 86400, **named_exprs: pl_IntoExpr):
  last_index = df.select(index = pl.max_horizontal(pl.col('timestamp').search_sorted(pl.col('timestamp') - window, 'left'), 1) - 1)['index']
  if isinstance(columns[0], list):
    columns = columns[0]
  new_columns = [f"{i}.diff()" for i in columns]
  column_exprs = [
    (df[i] - df[last_index][i]).alias(j) for i, j in zip(columns, new_columns)
  ]
  df = df.with_columns(
    *column_exprs
  ).with_columns(
    **named_exprs
  ).drop(new_columns, strict=True)
  return df
df = diff_with_window(df.with_columns(fee_index_log = pl.col('fee_index').log()), ['fee_index_log', 'timestamp'], window=7 * 86400,
  rate = (1 + pl.col('fee_index_log.diff()')) ** (1 / pl.col('timestamp.diff()')) - 1,
).with_columns(
  sigma = df['price'].rolling_std(40000) ** 2,
  apy = (1 + pl.col("rate")) ** (365 * 86400) - 1,
)
plotting(df, 'rate', 'sigma')

# %%
def test_amm(amm: PTT, df: pl.DataFrame):
  result = np.zeros((len(df), 6))
  df = df.with_columns(timedelta = pl.col('timestamp') - df[0, 'timestamp'])
  for i, row in enumerate(df.rows(named=True)):
    amm.set_time(row['timedelta'])
    (pt, tt) = amm.rate_to_position(row['rate'])
    _, delta_tt = amm.set_position(pt, tt)
    fee = abs(delta_tt) / (amm.price() * pt + tt) * amm.FEE_RATE
    result[i, :] = pt, tt, amm.k, amm.price(), amm.t, fee
  result[0, -1] = 0
  return df.with_columns(
    pt = result[:, 0],
    tt = result[:, 1],
    ptt_k = result[:, 2],
    ptt_price = result[:, 3],
    ptt_time = result[:, 4],
    ptt_fee = result[:, 5],
  ).with_columns(
    ptt_fee_cumsum = (1 + pl.col("ptt_fee")).cum_prod(),
    ptt_tv = pl.col('pt') * pl.col('ptt_price') + pl.col('tt'),
  )

# %%
import numpy as np
rate = np.random.rand(100)
# rate = np.abs(((rate - 0.5) / 10).cumsum() + 0.5)
price = np.random.rand(100) * 0.2 + 2.4
df_rand = pl.DataFrame().with_columns(
  height = np.arange(len(rate)) * 10,
  price = price,
  apy = rate,
  rate = (1 + rate) ** (1 / (365 * 86400)) - 1,
).with_columns(
  timestamp = pl.col('height') * 15 + 10_000_000,
)

start_time = np.datetime64('2022-12-01', 's').astype(np.int64)
window = 90 * 86400
df_test = df.filter((df['timestamp'] > start_time) & (df['timestamp'] < start_time + window)).select("height", "timestamp", "price", "rate", "apy")
plt.plot(rate)

# %%
amm = Yield(1000, 1000)
amm.price_to_position(0.81)

# %%
amm = Yield(1000, 1000)
test_amm(amm, df_test)

# %%
pendle_init = Pendle.coeff_ac(0, 1.0, total_time=90/365)
amm = Pendle(1000, 1000, A=pendle_init[0], C=pendle_init[1])
amm.price_to_position(0.98)

# %%
amm = Pendle(1000, 1000, A=pendle_init[0], C=pendle_init[1])
test_amm(amm, df_rand)

# %%
pendle_init = Pendle.coeff_ac(0, 0.25, total_time=90/365)
amm = Pendle(1000, 1000, A=pendle_init[0], C=pendle_init[1])
df_test = test_amm(amm, df_test).with_columns(
  ptt_expected_apy = pl.col('ptt_k') ** (365 * 86400 / (pl.col('ptt_time') * amm.total_time)) - 1,
)
df_test

# %%
def test_samm(samm: TS, df: pl.DataFrame):
  result = np.zeros((len(df), 5))
  for i, row in enumerate(df.rows(named=True)):
    (xt, yt) = samm.price_to_position(row['price'])
    delta_xt, _ = samm.set_position(xt, yt)
    feerate = abs(delta_xt) * samm.fee_rate / (xt + yt * samm.price())
    result[i, :] = xt, yt, samm.k, samm.price(), feerate
  result[0, -1] = 0
  return df.with_columns(
    xt = result[:, 0],
    yt = result[:, 1],
    ts_k = result[:, 2],
    ts_price = result[:, 3],
    ts_fee = result[:, 4],
  ).with_columns(
    ts_fee_cumsum = (1 + pl.col('ts_fee')).cum_prod(),
    ts_tv = pl.col("xt") + pl.col("yt") * pl.col("price"),
  )

# %%
samm = UniswapV2(1000, 1000)
test_samm(samm, df_rand)

# %%
samm = UniswapV2(1000, 1000, price_gap = 0)
df_result = test_samm(samm, df)
plotting(df_result, 'ts_fee_cumsum', 'fee_index', samey=True)

# %%
samm = UniswapV3(1000, 1000, idx=180)
samm.price()

# samm = UniswapV3(1000, 1000, idx=18)
# test_samm(samm, df_rand)

# %%
samm = UniswapV3(1000, 1000, idx=-400)
df_result = test_samm(samm, df)
plotting(df_result, 'ts_fee_cumsum')

# %%
samm = TickSwap(1000, 1000, idx=18)
test_samm(samm, df_rand)

# %%
samm = TickSwap(1000, 1000, idx=-400)
df_result = test_samm(samm, df)
plotting(df_result, 'ts_fee_cumsum')

# %%
start_time = np.datetime64('2023-01-01', 's').astype(np.int64)
before_window = 7 * 86400
after_window = 90 * 86400
avg_price = df.filter((df['timestamp'] > start_time - before_window) & (df['timestamp'] < start_time)).select('price').mean().item()
df_test = df.filter((df['timestamp'] > start_time) & (df['timestamp'] < start_time + after_window))

samm = TickSwap(1000, 1000, idx=TickSwap.price_to_tick(avg_price), price_gap=0)
samm.price_gap = 0
df_test = test_samm(samm, df_test).with_columns(
  ts_fee_index_log = pl.col('ts_fee_cumsum').log(),
)
df_test1 = test_samm(UniswapV2(1000, 1000, price_gap=0), df_test).with_columns(
  ts_fee_index_log = pl.col('ts_fee_cumsum').log(),
)
plotting(df_test, 'price', 'ts_fee_cumsum')
plotting(df_test1, 'price', 'ts_fee_cumsum', 'fee_index')
avg_price

# %%
df_test = diff_with_window(df_test, ['timestamp', 'ts_fee_index_log'],
  ts_fee_rate = (1 + pl.col('ts_fee_index_log.diff()')) ** (1 / pl.col('timestamp.diff()')) - 1,
).with_columns(
  v2_fee_rate = diff_with_window(df_test1, ['timestamp', 'ts_fee_index_log'],
    ts_fee_rate = (1 + pl.col('ts_fee_index_log.diff()')) ** (1 / pl.col('timestamp.diff()')) - 1,
  )['ts_fee_rate']
)
plotting(df_test, 'price', 'ts_fee_cumsum', 'ts_fee_rate', 'v2_fee_rate', 'rate')

# %%
fee_rates = [0.001, 0.003, 0.01, 0.03, 0.1]
df_test_result = [test_samm(TickSwap(1000, 1000, idx=TickSwap.price_to_tick(avg_price), fee_rate=fee), df_test) for fee in fee_rates]

# %%
fig, axs = plt.subplots(2, 1)
set_axes_locator(axs)
for df_result, fee in zip(df_test_result, fee_rates):
  axs[0].plot(df_test['datetime'], df_result['ts_fee_cumsum'], label=f"fee {fee*100}%")
axs[0].legend()
axs[1].plot(df_test['datetime'], df_test['price'])
avg_price

# %%
df_test_result[-1].filter(pl.col('ts_fee')>0)

# %%
set_axes_locator(plt.gca())
plt.axhline(samm.center_price() * 1.1)
plt.axhline(df_test[0, 'price'])
plt.axhline(samm.center_price() / 1.1)
for row in df_test_result[-1].filter(pl.col('ts_fee')>0).rows(named=True):
  plt.axvline(row['datetime'], color='red')
plt.plot(df_test['datetime'], df_test['price'])

# %%
market_name = "weETH_20240627_35_500_3"
market_info = parse_market_name(market_name)
df = load_datasets(ad, f'pendle2_market_block_{market_name}', with_timestamp=True)
plotting(df, 'value', 'pt_value', 'st_value', 'fee1', 'fee2', 'implied_apy')

# %%
tte = int(market_info.expiry - df['timestamp'].min())
A, C = Pendle.coeff_ac(market_info.min_apy, market_info.max_apy, total_time=tte / (365*86400))

# %%
# %%
df = df.with_columns(
  price = 2 - ((1 + pl.col('implied_apy')) ** ((market_info.expiry - pl.col('timestamp')) / (365*86400))),
)
plotting(df, 'price')

# %%
plt.plot(df['price'])
plt.plot(df['implied_apy'])

# %%
