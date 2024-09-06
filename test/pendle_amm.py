# %%
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
