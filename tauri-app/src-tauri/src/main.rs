// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

#[macro_use] extern crate tracing;

use std::{collections::BTreeMap, fmt::Display, path::PathBuf};

use polars::{chunked_array::ops::SortOptions, frame::DataFrame, lazy::frame::{IntoLazy, LazyFrame}, sql::sql_expr};
use tauri::State;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Debug)]
struct Config {
  data_dir: PathBuf,
}

#[derive(serde::Serialize)]
pub struct Error(String);
impl<T: std::fmt::Debug + Display> From<T> for Error {
  fn from(value: T) -> Self {
    error!(?value, %value, "error");
    Error(value.to_string())
  }
}
pub type Result<T, E=Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Dataset {
  pub name: String,
  pub collection: Vec<(usize, String)>,
  pub max: usize,
}
impl Dataset {
  pub fn new(name: String, mut collection: Vec<(usize, String)>) -> Self {
    collection.sort();
    let max = collection.last().map(|i| i.0).unwrap_or_default();
    Self { name, collection, max }
  }
}

fn split_name<'a>(name: &'a str, ext: &str) -> Option<(&'a str, usize)> {
  let mut split = name.rsplitn(3, '.');
  if split.next()? != ext { return None }
  let n =  split.next()?.parse().ok()?;
  let b = split.next()?;
  assert_eq!(split.next(), None);
  Some((b, n))
}

// Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
#[tauri::command(rename_all = "snake_case")]
#[instrument(level="info", fields(data_dir=%config.data_dir.display(), ok=config.data_dir.is_dir()))]
async fn list_data_names(config: State<'_, Config>) -> Result<Vec<Dataset>> {
  let mut map = BTreeMap::<_, Vec<_>>::new();
  for i in std::fs::read_dir(&config.data_dir)? {
    let i = i?;
    debug!(?i, ok=i.file_type()?.is_dir());
    let filename = i.file_name().to_string_lossy().to_string();
    if let Some((name, idx)) = split_name(&filename, "parquet") {
      map.entry(name.to_string()).or_default().push((idx, filename));
      continue;
    }
  }
  Ok(map
    .into_iter()
    .map(|(name, collection)| Dataset::new(name, collection))
    .collect())
}

fn get_data_info(config: &Config, name: &str) -> Result<Dataset> {
  let mut collection = Vec::new();
  for i in std::fs::read_dir(&config.data_dir)? {
    let i = i?;
    let filename = i.file_name().to_string_lossy().to_string();
    if !filename.starts_with(&name) {
      continue
    }

    if let Some((n, idx)) = split_name(&filename, "parquet") {
      if n != name { continue }
      collection.push((idx, filename));
    }
  }
  Ok(Dataset::new(name.to_string(), collection))
}

#[derive(serde::Serialize)]
struct Data {
  name: String,
  data: BTreeMap<String, Vec<Option<f64>>>,
  time: Vec<Option<i64>>,
}
#[tauri::command(rename_all = "snake_case")]
#[instrument(level="info", fields(data_dir=%config.data_dir.display(), ok=config.data_dir.is_dir()))]
async fn get_data(config: State<'_, Config>, name: String) -> Result<Data> {
  use polars::{datatypes::*, lazy::dsl::*};
  use std::ops::Mul;
  let info = get_data_info(&config, &name)?;
  if info.collection.is_empty() {
    return Err("nothing in collection".into())
  }
  let df_s = info.collection.iter().map(|(_, f)| {
    let path = config.data_dir.join(&f);
    LazyFrame::scan_parquet(&path, Default::default())?.collect()
  }).collect::<Result<Vec<_>, _>>()?;
  let mut df = None::<DataFrame>;
  for b in df_s {
    df = Some(match df {
      Some(a) => a.vstack(&b)?,
      None => b
    })
  }
  let df = df.unwrap().lazy();
  info!(df=%df.clone().limit(10).collect()?.head(None));
  // let select_exprs = [
  //   []
  // ];
  let df = df
    .filter(col("timestamp").gt(lit(0)))
    .with_column(col("timestamp").mul(lit(1000)).cast(DataType::Datetime(TimeUnit::Milliseconds, None)).cast(DataType::Date).alias("_date"))
    .group_by([col("_date")]).agg([
      // https://github.com/pola-rs/polars/blob/cd1994b63e32191640cca80da4fd420af0650378/crates/polars-sql/src/sql_expr.rs#L935
      // sql_expr("sum(total_eth)")?.alias("total_eth"),
      col("total_eth").sum().alias("total_eth_old"),
      // sql_expr("sum(tx_count as u64)")?.alias("tx_count"),
      col("tx_count").cast(DataType::UInt64).sum().alias("tx_cound_old"),
      // sql_expr("mean(total_fee)")?.alias("total_fee"),
      col("total_fee").mean().alias("total_fee_old"),
      // sql_expr("mean(gas_used)")?.alias("gas_used"),
      col("gas_used").mean().alias("gas_used_old"),
      // sql_expr("mean(fee_per_gas)")?.alias("fee_per_gas:mean"),
      col("fee_per_gas").mean().alias("fee_per_gas_old:mean"),
      // sql_expr("median(fee_per_gas)")?.alias("fee_per_gas:median"),
      col("fee_per_gas").median().alias("fee_per_gas_old:median"),
    ]);
  info!(agg=%df.clone().limit(10).collect()?.head(None));
  let df = df
    .with_column(col("_date").cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
    .sort("_date", SortOptions::default())
    .collect()?;
  let mut result = BTreeMap::new();
  for i in df.get_columns() {
    if i.name() == "_data" {
      continue;
    }
    result.insert(i.name().to_string(), i.cast(&DataType::Float64)?.f64()?.to_vec());
  }
  let time = df.column("_date").unwrap().datetime()?.to_vec();
  Ok(Data {
    name,
    data: result,
    time,
  })
}

fn main() {
  tracing_subscriber::fmt::fmt()
    .with_span_events(FmtSpan::CLOSE)
    .init();
  let config = Config {
    data_dir: "data".into(),
  };
  // if std::env::current_dir().unwrap().to_string_lossy() == std::env::var("CARGO_MANIFEST_DIR").unwrap() {
  //   std::env::set_current_dir(std::env::var("CARGO_WORKSPACE_DIR").unwrap()).unwrap();
  // }
  std::env::set_current_dir("../..").ok();
  info!(?config, pwd=%std::env::current_dir().unwrap().display());
  tauri::Builder::default()
    .manage(config)
    .invoke_handler(tauri::generate_handler![
      get_data,
      list_data_names,
    ])
    .run(tauri::generate_context!())
    .expect("error while running tauri application");
}
