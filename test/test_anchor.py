# %%
from common import *
import polars as pl
import math

enter_root_dir()
ad = all_datasets()

df = load_datasets(ad, "pendle2_market_factory_events", with_timestamp=True).with_columns(
  delta = (pl.col('expiry') - pl.col('timestamp')) / 86400.0,
)
df

# %%
apy_info = [
  ('PT-sUSDE-26SEP2024', 8.6, 71.8),
  ('PT-weETH-26SEP2024', 3.2, 72.0),
  ('PT-rsETH-26SEP2024', 3.1, 71.7),
  ('PT-pufETH-26SEP2024', 3.1, 71.7),

  ('PT-sUSDE-31OCT2024', 6.4, 41.9),

  ('PT-sUSDE-26DEC2024', 5.8, 41.1),
  ('PT-weETHs-26DEC2024', 0.4, 20.4),
  ('PT-rsETH-26DEC2024', 2.3, 15.4),
  ('PT-pufETH-26DEC2024', 2.3, 12.4),

  # ('PT-stETH-25DEC2025', 2.0, 9.5),
]
df_apy = pl.DataFrame(dict(
  pt_name = [x[0] for x in apy_info],
  min_apy = [x[1] for x in apy_info],
  max_apy = [x[2] for x in apy_info],
)).with_columns(
  min_apy = pl.col('min_apy') / 100 + 1,
  max_apy = pl.col('max_apy') / 100 + 1,
)
df_apy.join(df.select('pt_name', 'delta'), on='pt_name', how='left').with_columns(
  min_rate = pl.col('min_apy') ** (pl.col('delta') / 365.),
  max_rate = pl.col('max_apy') ** (pl.col('delta') / 365.),
).with_columns(
  expected_rate = (pl.col('min_rate') + pl.col('max_rate')) / 2,
).with_columns(
  scalar0 = math.log(9) / ((pl.col('max_rate') - pl.col('min_rate')) / 2),
  scalar1 = math.log(9) / pl.max_horizontal(pl.col('max_rate') - pl.col('expected_rate'), pl.col('expected_rate') - 1),
).with_columns(
  scalar0_apy = pl.col('scalar0') / 365 * pl.col('delta'),
  scalar1_apy = pl.col('scalar1') / 365 * pl.col('delta'),
).join(df.select('pt_name', 'anchor', 'scalar'), on='pt_name', how='left').with_columns(
  test_a = (pl.col('anchor') - 1) / (pl.col('expected_rate') - 1),
  test_c0 = pl.col('scalar0_apy') / pl.col('scalar'),
  test_c1 = pl.col('scalar1_apy') / pl.col('scalar'),
)

# %%
df.with_columns(
  scalar = pl.col('scalar') * 365 / pl.col('delta'),
).with_columns(
  max_rate = pl.col('anchor') + math.log(9) / pl.col('scalar'),
  min_rate = pl.col('anchor') - math.log(9) / pl.col('scalar'),
  expected_apy = pl.col('anchor'),
).with_columns(
  min_apy = pl.col('min_rate') ** (365 / pl.col('delta')),
  expected_apy = pl.col('expected_apy') ** (365 / pl.col('delta')),
  max_apy = pl.col('max_rate') ** (365 / pl.col('delta')),
).filter(pl.col('pt_name').str.contains('sUSDE'))

# %%
