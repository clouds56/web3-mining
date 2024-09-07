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
def load_datasets(ad: pl.DataFrame, name: str) -> pl.DataFrame:
  filenames = ad.filter((pl.col('name') == name) | (pl.col('prefix') == name))['paths'].explode()
  prefix = "".join([list(x)[0] for x in itertools.takewhile(lambda x: len(x) == 1, map(set, zip(*filenames)))])
  print("load", prefix, list(filenames.str.strip_prefix(prefix)))
  return load_files(filenames)

# %%
import matplotlib.axes, matplotlib.dates, matplotlib.ticker
def set_axes_locator(ax: matplotlib.axes.Axes, locator: matplotlib.ticker.Locator):
  ax.xaxis.set_major_locator(locator)
  ax.xaxis.set_major_formatter(matplotlib.dates.ConciseDateFormatter(locator))

# %% pure functions
def clamp(x, lower, upper):
  return min(max(x, lower), upper)
