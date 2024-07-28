# %%
import os
from typing import List
import itertools
import polars as pl
import matplotlib.pyplot as plt
from pathlib import Path

import csv
# 打开CSV文件
tokens = []
token_addr = []
pair_names = []
pair_addr = []
with open('tokens.csv', newline='') as csvfile:
    # 创建一个CSV阅读器对象
    csvreader = csv.reader(csvfile)
    # 逐行读取数据
    for row in csvreader:
        #print(row)
        tokens.append(row[0])
        token_addr.append(row[1])
def get_token(taddr):
    for t in range(len(token_addr)):
        if token_addr[t] == taddr:
            return tokens[t]
    print("BadException")
    raise ValueError("BadException")
#print(tokens, token_addr)

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
files = pl.DataFrame({
  'path': Path("data").rglob("*.parquet")
}).with_columns([
  pl.col('path').map_elements(lambda x: x.name.split(".")[0]).alias('prefix'),
  pl.col('path').map_elements(lambda x: try_int(x.name.split(".")[1])).alias('idx'),
  pl.col('path').map_elements(lambda x: str(x)).alias('path'),
]).with_columns([
  pl.col('prefix').map_elements(lambda x: try_int(x.split("_")[-1])).alias('cut'),
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
def load_datasets(name: str) -> pl.DataFrame:
  filenames = datasets.filter(pl.col('name') == name)['paths'].explode()
  prefix = "".join([list(x)[0] for x in itertools.takewhile(lambda x: len(x) == 1, map(set, zip(*filenames)))])
  print("load", prefix, list(filenames.str.strip_prefix(prefix)))
  return load_files(filenames)
datasets

# %%
# df = load_files("block_metrics_*.parquet")
df = load_datasets('block_metrics')
df.mean()

# %%
df = load_datasets("uniswap_factory_events")
df.sort('height')['tx_hash'].head().to_list()
#print(df)
find_uniswap_pair_v2 = True
if find_uniswap_pair_v2:
    for x in df.rows(): #print(x[4:7])
        if x[2] != "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f":
            continue
        for y in token_addr[:4]:
            #print(x[4],y)
            if x[4].lower()==y:
                for z in token_addr[4:]:
                    if x[5]==z:
                        pair_names.append(get_token(y)+"_"+get_token(z))
                        pair_addr.append(x[6])
                        print(get_token(y)+"_"+get_token(z),"@@@", x[4:7])
                        import time
                        time.sleep(4)
    
            elif x[5].lower()==y:
                for z in token_addr[4:]:
                    if x[4]==z:
                        pair_names.append(get_token(z)+"_"+get_token(y))
                        pair_addr.append(x[6])
                        print(get_token(z)+"_"+get_token(y),"@@@", x[4:7])
                        import time
                        time.sleep(4)
        #break
    print(pair_names,len(pair_names), 198, "uniswap")
    print(pair_addr,len(pair_addr), 199, "uniswap")
    assert(len(pair_addr)==len(pair_names))
    for i in range(len(pair_names)):
        print('[uniswap_pair_events.{}]\ncontract = "{}"\ncreated = 10000000\n'.format(pair_names[i], pair_addr[i]))

# decode pair address from uniswap3_factory_events
df = load_datasets("uniswap3_factory_events")
df.sort('height')['tx_hash'].head().to_list()
#print((df.columns))
find_uniswap_pair_v3 = True
if find_uniswap_pair_v3:
    pair_names = []
    pair_addr = []
    for x in df.rows(): #print(x[4:7])
        if x[2] != "0x1F98431c8aD98523631AE4a59f267346ea31F984":
            continue
        for y in token_addr[:4]:
            #print(x[4],y)
            if x[4].lower()==y.lower():
                for z in token_addr[:]:
                    if x[5].lower()==z.lower() and y.lower()!=z.lower():
                        if in_pair_set3(get_token(y), get_token(z), x[7], x[8]):
                            print("p1", in_pair_set3(get_token(y), get_token(z), x[7], x[8]), get_token(y)+"_"+get_token(z))
                            continue
                        pair_name = get_token(y)+"_"+get_token(z)+"_"+str(x[7])+"_"+str(x[8])
                        pairs_set.add(pair_name)
                        pair_names.append(pair_name)
                        pair_addr.append(x[6])
                        print("b1", pair_name, "@@@", x[4:9])
    
            elif x[5].lower()==y.lower():
                for z in token_addr[:]:
                    if x[4].lower()==z.lower() and y.lower()!=z.lower():
                        if in_pair_set3(get_token(y), get_token(z), x[7], x[8]):
                            print("p2", in_pair_set3(get_token(y), get_token(z), x[7], x[8]), get_token(y)+"_"+get_token(z))
                            continue
                        pair_name = get_token(z)+"_"+get_token(y)+"_"+str(x[7])+"_"+str(x[8])
                        pairs_set.add(pair_name)
                        pair_names.append(pair_name)
                        pair_addr.append(x[6])
                        print("b2", pair_name, "@@@", x[4:9])
        #break
    print(pair_names,len(pair_names), 298, "uniswap3")
    print(pair_addr,len(pair_addr), 299, "uniswap3")
    assert(len(pair_addr)==len(pair_names))
    for i in range(len(pair_names)):
        print('[uniswap3_pair_events.{}]\ncontract = "{}"\ncreated = 10000000\n'.format(pair_names[i], pair_addr[i]))

# %%
df = load_files("uniswap_pair_old_*.parquet")
if df:
  dfg = df.group_by('topic0').agg([pl.first('tx_hash'), pl.count('height').alias('count')]).sort('count', descending=True)
  list(zip(*[list(dfg[col]) for col in dfg.columns]))

# %%
pairs = (datasets
  .filter(pl.col('name').str.starts_with('uniswap_pair_events'))
  .select(pl.col('name').str.strip_prefix('uniswap_pair_events_').alias('pair'))
  ['pair'])
pairs

# %%
for pair in pairs:
  df = load_datasets("uniswap_pair_events_" + pair)
  print(df.group_by("action").len().sort("len", descending=True))

  df_acc = df.group_by('height').agg(
    (pl.col('value_in').fill_null(0) - pl.col('value_out').fill_null(0)).sum(),
    (pl.col('amount0_in').fill_null(0) - pl.col('amount0_out').fill_null(0)).sum(),
    (pl.col('amount1_in').fill_null(0) - pl.col('amount1_out').fill_null(0)).sum(),
    pl.col('reserve0').fill_null(strategy='forward').last(),
    pl.col('reserve1').fill_null(strategy='forward').last(),
  ).sort('height').with_columns(
    pl.col('value_in').cum_sum().alias('value'),
  )
  df_acc.write_parquet(f"data/uniswap_pair_block_{pair}.parquet")

  # plt.plot(df_acc['height'], df_acc['value0'])
  plt.plot(df_acc['height'], (df_acc['reserve0']*df_acc['reserve1']).sqrt()/df_acc['value'], label=pair)
plt.legend()
plt.show()

# %%
df = load_datasets('uniswap3_pair_events_wbtc_weth')
df[0]['tx_hash'].to_list()
df.group_by('action').count()
# %%
plt.plot(df['height'], (df['price']/1e5)**-2)
# %%
plt.plot(df['height'], (df['value'] * (df['tick_upper'] - df['tick_lower'])).cum_sum())
# %%
plt.plot(df['height'], -df['fee1'].cum_sum().fill_null(strategy="forward"))
# %%
