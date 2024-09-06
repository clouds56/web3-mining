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

  def update_k(self):
    pass

  def trade(self, value: int, type: TOKEN_TYPE):
    raise NotImplementedError

  def price(self):
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
    price = 1 / rate ** (self.t * self.total_time)
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
    self.t += time / self.total_time
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
class Pendle(PTT):
  def __init__(self, pt: int, tt: int) -> None:
    super().__init__(pt, tt)
