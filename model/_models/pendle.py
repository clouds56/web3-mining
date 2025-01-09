from typing import Literal, TypeAlias

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

# %%
import math
class Pendle(PTT):
  TRADE_STEPS = 10
  """
  in the context, y means PT, x means TT (aka ST)
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
