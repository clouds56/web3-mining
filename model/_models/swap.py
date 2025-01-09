
import math

from _common import clamp

class TS:
  TICK_BASE = 1.05
  FEE_RATE = 0.003
  def __init__(self, xt: int, yt: int, *, idx: int, fee_rate: float | None = None, price_gap: float | None = None) -> None:
    if fee_rate is None: fee_rate = self.FEE_RATE
    if price_gap is None: price_gap = fee_rate
    self.k = 0
    self.XT = xt
    self.YT = yt
    self.idx = idx
    self.left_limit = self.TICK_BASE ** self.idx
    self.right_limit = self.TICK_BASE ** (self.idx + 1)
    self.price_gap = price_gap
    self.fee_rate = fee_rate
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
    if self.price_gap == 0: return price
    if price / (1 + self.price_gap) > old_price: return price / (1 + self.price_gap)
    if price * (1 + self.price_gap) < old_price: return price * (1 + self.price_gap)
    return old_price

  def set_position(self, xt: int, yt: int) -> tuple[int, int]:
    delta = (xt - self.XT, xt - self.YT)
    self.XT = xt
    self.YT = yt
    return delta

class UniswapV2(TS):
  TICK_BASE = float('inf')
  def __init__(self, xt: int, yt: int, **kwargs) -> None:
    super().__init__(xt, yt, idx=0, **kwargs)
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


class UniswapV3(TS):
  def __init__(self, xt: int, yt: int, *, idx: int, **kwargs) -> None:
    super().__init__(xt, yt, idx=idx, **kwargs)

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

class TickSwap(TS):
  def __init__(self, xt: int, yt: int, *, idx: int, **kwargs) -> None:
    super().__init__(xt, yt, idx=idx, **kwargs)

  def price(self) -> float:
    # if self.XT == 0: return self.center_price() / (1 + self.price_gap)
    # if self.YT == 0: return self.center_price() * (1 + self.price_gap)
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
