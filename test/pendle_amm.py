# %%
from typing import Literal, TypeAlias
from common import *
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import math
enter_root_dir()
pair = "usdc_weth"
ad = all_datasets()

# %%
dfb = load_datasets(ad, f"block_metrics")

# %%
df = load_datasets(ad, f"uniswap_pair_block_{pair}").join(
  dfb.select('height', 'timestamp'), on='height',
)
df = df.with_columns(
  datetime = pl.from_epoch(df['timestamp'], time_unit='s'),
  fee_index = (df['reserve0'] * df['reserve1']).sqrt() / df['value'],
  price = df['reserve0'] / df['reserve1'],
).sort('height')
plotting(df, 'price', 'fee_index')

# %%
window = 7 * 86400
last_index = df.select(index = pl.max_horizontal(pl.col('timestamp').search_sorted(pl.col('timestamp') - window, 'left'), 1) - 1)['index']
df = df.with_columns(
  rate = (1 + (df['fee_index'] - df[last_index]['fee_index'])) ** (1 / (df['timestamp'] - df[last_index]['timestamp'])) - 1,
  sigma = df['price'].rolling_std(40000) ** 2,
).with_columns(
  apy = (1 + pl.col("rate")) ** (365 * 86400) - 1,
)
plotting(df, 'rate', 'sigma')

# %%
TOKEN_TYPE: TypeAlias = Literal["PT"] | Literal["T"]
class PTT:
  FEE_RATE = 0.001
  def __init__(self, pt: int, tt: int, *, total_time = 90 * 86400) -> None:
    self._rate = 0
    self.t = 1
    self.total_time = total_time + 1
    self.TT = tt
    self.PT = pt
    self.step_time(1)

  def set_position(self, pt: int, tt: int) -> tuple[int, int]:
    delta = (pt - self.PT, tt - self.TT)
    self.PT = pt
    self.TT = tt
    return delta

  def update_rate(self):
    self._rate = 1 / self.price() ** (1 / (self.t * self.total_time)) - 1

  def update_k(self):
    pass

  def trade(self, value: int, type: TOKEN_TYPE):
    raise NotImplementedError

  def price(self) -> float:
    raise NotImplementedError

  def price_to_position(self, price: float) -> tuple[int, int]:
    """
    return (PT, TT)
    """
    raise NotImplementedError

  def rate_to_position(self, rate: float) -> tuple[int, int]:
    """
    return (PT, TT)
    """
    price = 1 / (1 + rate) ** (self.t * self.total_time)
    return self.price_to_position(price)

  def set_value(self, value: int, type: TOKEN_TYPE):
    if type == "PT":
      delta = self.PT - value
      self.PT = value
    else:
      delta = self.TT - value
      self.TT = value
    return delta

  def step_time(self, time: int):
    self.update_rate()
    self.t -= time / self.total_time
    if self.t < 0:
      self.t = 0
    self.update_k()

  def set_time(self, time: int):
    self.update_rate()
    time += 1
    if time <= 0: time = 1
    if time > self.total_time: time = self.total_time
    self.t = 1 - time / self.total_time
    self.update_k()

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
class Yield(PTT):
  """
  x^(1-t) + y^(1-t) = k
  """
  def __init__(self, pt: int, tt: int) -> None:
    super().__init__(pt, tt)

  def price(self):
    """
    p = (x / y) ^ t
    """
    return (self.TT / self.PT) ** self.t

  def price_to_position(self, price: float) -> tuple[int, int]:
    """
    x = ((k p^(1/t))/(p^(1/t) + p))^(1/(1 - t))
    """
    new_TT = ((self.k * price ** (1 / self.t)) / (price ** (1 / self.t) + price)) ** (1 / (1 - self.t))
    new_PT = (self.k - new_TT ** (1 - self.t)) ** (1 / (1 - self.t))
    return new_PT, new_TT

  def trade(self, value: int, type: TOKEN_TYPE):
    if type == "PT":
      self.PT += value
      new_TT = (self.k - self.PT ** (1 - self.t)) ** (1 / (1 - self.t))
      return self.set_value(new_TT, "T")
    else:
      self.TT += value
      new_PT = (self.k - self.TT ** (1 - self.t)) ** (1 / (1 - self.t))
      return self.set_value(new_PT, "PT")

  def update_k(self):
    self.k = self.PT ** (1 - self.t) + self.TT ** (1 - self.t)

amm = Yield(1000, 1000)
amm.price_to_position(0.81)

# %%
amm = Yield(1000, 1000)
test_amm(amm, df_test)

# %%
import math
class Pendle(PTT):
  TRADE_STEPS = 10
  """
  1 / p == t * ln(y/x) / A + k
  """
  def __init__(self, pt: int, tt: int, *, A: float, C: float) -> None:
    self.A = A
    self.k = C
    super().__init__(pt, tt)

  def coeff_ac(lower: float, upper: float, expected: float | None = None, *, total_time: int | float):
    """
    min = 0, max, expacted -> A, C
    """
    # here price means 1/price
    lower_price = (1 + lower) ** total_time
    upper_price = (1 + upper) ** total_time
    if expected is None:
      expected_price = (upper_price + lower_price) / 2
    else:
      expected_price = (1 + expected) ** total_time
    C = expected_price
    A = math.log(9) / max(upper_price - expected_price, expected_price - lower_price)
    return A, C

  def update_k(self):
    self.k = (1 + self._rate) ** (self.t * self.total_time) - self.t * math.log(self.PT / self.TT) / self.A

  def price(self) -> float:
    return 1 / (self.t * math.log(self.PT / self.TT) / self.A + self.k)

  def price_to_position(self, price: float) -> tuple[int, int]:
    """
    y1 / x1 = ratio = exp((1 / p - k) * A / t)
    (y1 - y0) / (x0 - x1) = 1 / price

    y1 = x1 * ratio
    (price * ratio + 1) * x1  = y0 * price + x0
    """
    def step(PT, TT, price):
      ratio = math.exp((1 / price - self.k) * self.A / self.t)
      new_TT = (PT * price + TT) / (price * ratio + 1)
      new_PT = new_TT * ratio
      return (new_PT, new_TT)
    p = old_price = self.price()
    tmp_PT, tmp_TT = self.PT, self.TT
    for i in range(self.TRADE_STEPS):
      p = (i+1) * (price - old_price) / self.TRADE_STEPS + old_price
      tmp_PT, tmp_TT = step(tmp_PT, tmp_TT, p)
    return tmp_PT, tmp_TT


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
class TS:
  TICK_BASE = 1.05
  FEE_RATE = 0.003
  def __init__(self, xt: int, yt: int, idx: int) -> None:
    self.k = 0
    self.XT = xt
    self.YT = yt
    self.idx = idx
    self.left_limit = self.TICK_BASE ** self.idx
    self.right_limit = self.TICK_BASE ** (self.idx + 1)
    self.update_k()

  @classmethod
  def price_to_tick(cls, price: float) -> int:
    return math.floor(math.log(price, cls.TICK_BASE))

  def price(self) -> float:
    raise NotImplementedError

  def price_to_position(self, price: float) -> tuple[int, int]:
    raise NotImplementedError

  def update_k(self):
    pass

  def target_price(self, price: float) -> float:
    old_price = self.price()
    if price / (1 + self.FEE_RATE) > old_price: return price / (1 + self.FEE_RATE)
    if price * (1 + self.FEE_RATE) < old_price: return price * (1 + self.FEE_RATE)
    return old_price

  def set_position(self, xt: int, yt: int) -> tuple[int, int]:
    delta = (xt - self.XT, xt - self.YT)
    self.XT = xt
    self.YT = yt
    return delta

def test_samm(samm: TS, df: pl.DataFrame):
  result = np.zeros((len(df), 5))
  for i, row in enumerate(df.rows(named=True)):
    (xt, yt) = samm.price_to_position(row['price'])
    delta_xt, _ = samm.set_position(xt, yt)
    feerate = abs(delta_xt) * samm.FEE_RATE / (xt + yt * samm.price())
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
class UniswapV2(TS):
  TICK_BASE = float('inf')
  def __init__(self, xt: int, yt: int) -> None:
    super().__init__(xt, yt, 0)
    self.left_limit = 0
    self.right_limit = self.TICK_BASE

  def update_k(self):
    self.k = math.sqrt(self.XT * self.YT)

  def price(self) -> float:
    return self.XT / self.YT

  def price_to_position(self, price: float) -> tuple[int, int]:
    price = self.target_price(price)
    new_XT = self.k * math.sqrt(price)
    new_YT = self.k / math.sqrt(price)
    return new_XT, new_YT

samm = UniswapV2(1000, 1000)
test_samm(samm, df_rand)

# %%
samm = UniswapV2(1000, 1000)
df_result = test_samm(samm, df)
plt.plot(df_result['ts_fee_cumsum'])

# %%
class UniswapV3(TS):
  def __init__(self, xt: int, yt: int, idx: int) -> None:
    super().__init__(xt, yt, idx=idx)

  def update_k(self):
    """
    (x + k√p_left)(y + k/√p_right) = k^2
    """
    a = math.sqrt(self.left_limit / self.right_limit) - 1
    b = self.YT * math.sqrt(self.left_limit) + self.XT / math.sqrt(self.right_limit)
    c = self.XT * self.YT
    self.k = (- b - math.sqrt(b**2 - 4 * a * c)) / (2 * a)

  def price(self) -> float:
    """
    p = - dx/dy == (y + k/√p_right) / (x + k√p_left)
      == 1 / (k/y + 1/√p_right) ** 2
      == (x/k + √p_left) ** 2
    """
    return (self.XT / self.k + math.sqrt(self.left_limit)) ** 2

  def price_to_position(self, price: float) -> tuple[int, int]:
    """
    x = k(√p - √p_left)
    y = k(1/√p - 1/√p_right)
    """
    price = self.target_price(price)
    price = clamp(price, self.left_limit, self.right_limit)
    new_XT = self.k * (math.sqrt(price) - math.sqrt(self.left_limit))
    new_YT = self.k * (1 / math.sqrt(price) - 1 / math.sqrt(self.right_limit))
    return new_XT, new_YT

samm = UniswapV3(1000, 1000, 180)
samm.price()

# %%
samm = UniswapV3(1000, 1000, 18)
test_samm(samm, df_rand)

# %%
samm = UniswapV3(1000, 1000, -400)
df_result = test_samm(samm, df)
plt.plot(df_result['ts_fee_cumsum'])

# %%
class TickSwap(TS):
  def __init__(self, xt: int, yt: int, idx: int) -> None:
    super().__init__(xt, yt, idx=idx)

  def price(self) -> float:
    # if self.XT == 0: return self.center_price() / (1 + self.FEE_RATE)
    # if self.YT == 0: return self.center_price() * (1 + self.FEE_RATE)
    return self.center_price()

  def center_price(self) -> float:
    return (self.left_limit * self.right_limit) ** 0.5

  def update_k(self):
    self.k = self.XT + self.YT * self.center_price()

  def price_to_position(self, price: float) -> tuple[int, int]:
    center_price = self.center_price()
    price = self.target_price(price)
    if price > center_price:
      return (self.k, 0)
    if price < center_price:
      return (0, self.k / center_price)
    return self.XT, self.YT

class TickSwapFee1(TickSwap): FEE_RATE = 0.001
class TickSwapFee3(TickSwap): FEE_RATE = 0.003
class TickSwapFee10(TickSwap): FEE_RATE = 0.01
class TickSwapFee30(TickSwap): FEE_RATE = 0.03
class TickSwapFee100(TickSwap): FEE_RATE = 0.1

# %%
samm = TickSwap(1000, 1000, 18)
test_samm(samm, df_rand)

# %%
samm = TickSwap(1000, 1000, -400)
df_result = test_samm(samm, df)
plt.plot(df_result['ts_fee_cumsum'])

# %%
start_time = np.datetime64('2022-08-01', 's').astype(np.int64)
before_window = 7 * 86400
after_window = 90 * 86400
avg_price = df.filter((df['timestamp'] > start_time - before_window) & (df['timestamp'] < start_time)).select('price').mean().item()
samm = TickSwap(1000, 1000, idx=TickSwap.price_to_tick(avg_price))
df_test = df.filter((df['timestamp'] > start_time) & (df['timestamp'] < start_time + after_window))
df_test = test_samm(samm, df_test)
plotting(df_test, 'price', 'ts_fee_cumsum')
avg_price

# %%
models = [TickSwapFee1, TickSwapFee3, TickSwapFee10, TickSwapFee30, TickSwapFee100]
df_test_result = [test_samm(model(1000, 1000, idx=TickSwap.price_to_tick(avg_price)), df_test) for model in models]

# %%
fig, axs = plt.subplots(2, 1)
set_axes_locator(axs)
for df_result, model in zip(df_test_result, models):
  axs[0].plot(df_test['datetime'], df_result['ts_fee_cumsum'], label=f"fee {model.FEE_RATE*100}%")
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
