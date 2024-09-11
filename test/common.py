# %%
import itertools
from pathlib import Path
import polars as pl


def enter_root_dir():
  import os
  while not os.path.exists("Cargo.toml"):
    os.chdir("../")

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
def try_int(s: str):
  try:
    return int(s)
  except:
    return None
def all_datasets(path = None):
  if path is None:
    path = Path("data").rglob("*.parquet")
  files = pl.DataFrame({
    'path': path
  }).with_columns([
    pl.col('path').map_elements(lambda x: x.name.split(".")[0], return_dtype=pl.String).alias('prefix'),
    pl.col('path').map_elements(lambda x: try_int(x.name.split(".")[1]), return_dtype=pl.Int64).alias('idx'),
    pl.col('path').map_elements(lambda x: str(x), return_dtype=pl.String).alias('path'),
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
import matplotlib.pyplot as plt
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

def plotting(df: pl.DataFrame, *columns: pl_IntoExpr, time_column: str = 'datetime', twinx: bool | None = None):
  if twinx is None:
    twinx = len(columns) == 2
  if twinx:
    fig, ax = plt.subplots()
    if time_column != 'height':
      set_axes_locator(ax)
    axs = [ax, *[ax.twinx() for _ in range(len(columns) - 1)]]
  else:
    fig, axs = plt.subplots(len(columns), 1)
    if time_column != 'height':
      set_axes_locator(axs)
  lines = []
  for i, (column, ax) in enumerate(zip(columns, axs)):
    if isinstance(column, str):
      label = column
      column = df[label]
    elif isinstance(column, pl.Expr):
      label = column.name
      column = df.select(column)
    elif isinstance(column, pl.Series):
      label = column.name
    else:
      label = "line_" + i
    if twinx and i > 0:
      ax._get_lines = axs[0]._get_lines
      ax.spines["right"].set_position(("axes", 0.8 + 0.2 * i))
      ax.set_ylabel(label, color=p.get_color())
      ax.tick_params(axis='y', colors=p.get_color())
    p, = ax.plot(df[time_column], column, label=label)
    lines.append(p)
  if twinx:
    axs[0].legend(lines, [l.get_label() for l in lines])
  return fig

# %% pure functions
def clamp(x, lower, upper):
  return min(max(x, lower), upper)
