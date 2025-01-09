# %%
from dataclasses import dataclass
import itertools
from pathlib import Path
import polars as pl

def setup_datasets(*, alt_renderer = "png", datasets_path = None):
  import os
  while not os.path.exists("Cargo.toml"):
    os.chdir("../")

  import altair as alt
  # import vegafusion_jupyter
  # vf.enable(row_limit=1_000_000)
  # vegafusion_jupyter.enable()
  alt.data_transformers.enable('vegafusion')
  alt.renderers.enable(alt_renderer)
  alt.themes.register('custom', lambda: {
    "config": {
      "view": { "continuousWidth": 500, "continuousHeight": 300 },
      "scale": { "zero": False },
      "axisY": { "format": "e" },
    }
  })
  alt.themes.enable('custom')
  # alt.Chart().configure_scale(zero=False)
  # plt.renderers.enable('mimetype')

  if datasets_path == False:
    return None
  return all_datasets(datasets_path)

def load_files(files) -> pl.DataFrame:
  if isinstance(files, str):
    files = Path("data").rglob(files)
  dfa: pl.DataFrame = None
  for file in files:
    df = pl.read_parquet(file)
    if dfa is None:
      dfa = df
    else:
      dfa = dfa.vstack(df)
  return dfa

# %%
def all_datasets(path = None):
  if path is None:
    path = Path("data").rglob("*.parquet")
  files = pl.DataFrame({
    'path': path
  }).with_columns([
    pl.col('path').map_elements(lambda x: x.name.split(".")[0], return_dtype=pl.String).alias('prefix'),
    pl.col('path').map_elements(lambda x: try_int(x.name.split(".")[1]), return_dtype=pl.Int64).alias('idx'),
    pl.col('path').map_elements(lambda x: f"{x}", return_dtype=pl.String).alias('path'),
  ]).with_columns([
    pl.col('prefix').map_elements(lambda x: try_int(x.split("_")[-1]), return_dtype=pl.Int64).alias('cut'),
  ]).sort('prefix', 'idx')
  datasets = files.group_by('prefix').agg([
    pl.first('cut'),
    pl.max('idx').alias('max'),
    pl.count('idx').alias('count'),
    pl.col('path').alias('paths'),
  ]).with_columns([
    pl.col('prefix')
      .str.strip_suffix(pl.col('cut').cast(pl.String))
      .str.strip_suffix('_')
      .fill_null(pl.col('prefix'))
      .alias('name')
  ]).sort('name')
  return datasets
def load_datasets(ad: pl.DataFrame, name: str, *, with_timestamp = False) -> pl.DataFrame:
  import sys
  filenames = ad.filter((pl.col('name') == name) | (pl.col('prefix') == name))['paths'].explode()
  prefix = "".join([list(x)[0] for x in itertools.takewhile(lambda x: len(x) == 1, map(set, zip(*filenames)))])
  print("load", prefix, list(filenames.str.strip_prefix(prefix)), file=sys.stderr)
  df = load_files(filenames)
  if with_timestamp:
    dfb = load_datasets(ad, f"block_metrics")
    df = df.join(
      dfb.select('height', 'timestamp'), on='height', how='left'
    ).with_columns(
      datetime = pl.from_epoch(pl.col('timestamp'), time_unit='s'),
    )
  return df

# %%
import numpy as np
# import matplotlib.pyplot as plt
import matplotlib.axes, matplotlib.dates, matplotlib.ticker
from polars._typing import IntoExpr as pl_IntoExpr
def set_axes_locator(ax: matplotlib.axes.Axes | np.ndarray[matplotlib.axes.Axes], locator: matplotlib.ticker.Locator | None = None):
  if locator is None:
    locator = matplotlib.dates.AutoDateLocator()
  if isinstance(ax, np.ndarray):
    for a in ax.flat:
      a.xaxis.set_major_locator(locator)
      a.xaxis.set_major_formatter(matplotlib.dates.ConciseDateFormatter(locator))
  else:
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(matplotlib.dates.ConciseDateFormatter(locator))

def plotting(df: pl.DataFrame, *columns: pl_IntoExpr, time_column: str = 'datetime',
  twinx: bool | None = None, samey: bool = False,
):
  if isinstance(columns[0], list):
    columns = columns[0]
  if twinx is None or samey:
    twinx = len(columns) == 2
  if samey:
    twinx = True

  df = df.select([
    time_column,
    *columns,
  ])
  if len(df) > 100_000:
    df = df.sample(50_000)
  column_names = df.columns
  df_plot = df.plot
  result = None
  for i, column in enumerate(column_names[1:]):
    line = df_plot.line(x=time_column, y=column)
    if result is None:
      result = line
    elif samey or twinx:
      result += line
    else:
      result &= line
  if twinx and not samey:
    result = result.resolve_scale(y='independent')
  elif len(columns) != 1 and not samey and not twinx:
    result = result.configure_view(continuousHeight=100)
  return result

# %% pure functions
def try_int(s: str):
  try:
    return int(s)
  except:
    return None

def clamp(x, lower, upper):
  return min(max(x, lower), upper)

def parse_yyyymmdd(date: str):
  date = date[:-4] + '-' + date[-4:-2] + '-' + date[-2:]
  return np.datetime64(date, 's')

# %%
@dataclass
class MarketInfo:
  name: str
  underlying: str
  expiry: int
  min_apy: float
  max_apy: float
  fee_rate: float
def parse_market_name(name: str) -> MarketInfo | None:
  parts = name.rsplit('_', 5)
  if len(parts) < 5:
    return None
  return MarketInfo(
    name=name,
    underlying=parts[0],
    expiry=int(parse_yyyymmdd(parts[1]).astype('datetime64[s]').astype(int)),
    min_apy=int(parts[2]) / 1e3,
    max_apy=int(parts[3]) / 1e3,
    fee_rate=int(parts[4]) / 1e4,
  )
