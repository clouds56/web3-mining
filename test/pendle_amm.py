# %%
from typing import Literal, TypeAlias
from common import *
import polars as pl
enter_root_dir()
pair = "usdc_weth"
ad = all_datasets()

# %%
df = load_datasets(ad, f"uniswap_pair_block_{pair}")
df = df.with_columns(
  fee_index = (df['reserve0'] * df['reserve1']).sqrt() / df['value'],
  price = df['reserve0'] / df['reserve1'],
).sort('height')

# %%
import matplotlib.pyplot as plt
plt.plot(df['fee_index'])

# %%
window = 7 * 86400 / 15
last_index = df.select(index = pl.max_horizontal(df['height'].search_sorted(df['height'] - window, 'left'), 1) - 1)['index']
df = df.with_columns(
  rate = (df['fee_index'] - df[last_index]['fee_index']) / (df['height'] - df[last_index]['height']) * window,
  sigma = df['price'].fill_null(strategy='forward').rolling_std(40000) ** 2,
)

# %%
plt.plot(df['fee_avg'])
# %%
plt.plot(df['sigma'])

# %%
TOKEN_TYPE: TypeAlias = Literal["PT"] | Literal["T"]
class PTT:
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
import numpy as np
rate = np.random.rand(100)
df = pl.DataFrame().with_columns(
  height = np.arange(len(rate)) * 15,
  rate = (1 + rate) ** (1 / (365 * 86400)) - 1,
)
df
# %%
amm = Yield(1000, 1000)
result = np.zeros((len(rate), 5))
for i, row in enumerate(df.rows(named=True)):
  amm.set_time(row['height'] * 15)
  (pt, tt) = amm.rate_to_position(row['rate'])
  amm.set_position(pt, tt)
  result[i, :] = pt, tt, amm.k, amm.price(), amm.t
df = df.with_columns(
  pt = result[:, 0],
  tt = result[:, 1],
  k = result[:, 2],
  price = result[:, 3],
  time = result[:, 4],
)
df
# %%
import math
class Pendle(PTT):
  """
  1 / p == t * ln(y/x) / A + k
  """
  def __init__(self, pt: int, tt: int, *, A: float, C: float) -> None:
    self.A = A
    self.k = C
    super().__init__(pt, tt)

  def coeff_ac(lower: float, upper: float, expected: float, *, total_time: int | float):
    """
    min = 0, max, expacted -> A, C
    """
    # here price means 1/price
    lower_price = (1 + lower) ** total_time
    upper_price = (1 + upper) ** total_time
    expected_price = (1 + expected) ** total_time
    C = expected_price
    A = math.log(9) / max(upper_price - expected_price, expected_price - lower_price)
    return A, C

  def update_k(self):
    self.k = 1 / (1 + self._rate) ** (self.t * self.total_time) - self.t * math.log(self.PT / self.TT) / self.A

  def price(self) -> float:
    return self.t * math.log(self.PT / self.TT) / self.A + self.k

  def price_to_position(self, price: float) -> tuple[int, int]:
    """
    y1 / x1 = ratio = exp((1 / p - k) * A / t)
    (y1 - y0) / (x0 - x1) = 1 / price

    y1 = x1 * ratio
    (price * ratio + 1) * x1  = y0 * price + x0
    """
    ratio = math.exp((1 / price - self.k) * self.A / self.t)
    print(ratio)
    new_TT = (self.PT * price + self.TT) / (price * ratio + 1)
    new_PT = new_TT * ratio
    return new_PT, new_TT

def natrual_expected(lower, upper, *, total_time):
  # here price means 1/price
  upper_price = (1+upper) ** total_time
  lower_price = (1+lower) ** total_time
  expected_price = (upper_price + lower_price) / 2
  return expected_price ** (1 / total_time) - 1

pendle_init = Pendle.coeff_ac(0, 1.0, 0.435, total_time=90/365)
amm = Pendle(1000, 1000, A=pendle_init[0], C=pendle_init[1])
amm.price_to_position(0.84)

# %%
amm = Pendle(1000, 1000, A=pendle_init[0], C=pendle_init[1])
result = np.zeros((len(rate), 5))
for i, row in enumerate(df.rows(named=True)):
  amm.set_time(row['height'] * 15)
  (pt, tt) = amm.rate_to_position(row['rate'])
  amm.set_position(pt, tt)
  result[i, :] = pt, tt, amm.k, amm.price(), amm.t
df = df.with_columns(
  pt = result[:, 0],
  tt = result[:, 1],
  k = result[:, 2],
  price = result[:, 3],
  time = result[:, 4],
)
df

# %%
