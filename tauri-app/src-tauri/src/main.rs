// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

#[macro_use] extern crate tracing;

use std::{collections::BTreeMap, fmt::Display, path::PathBuf};

use polars::{chunked_array::ops::SortOptions, lazy::frame::LazyFrame};
use tauri::State;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Debug)]
struct Config {
  data_dir: PathBuf,
}

#[derive(serde::Serialize)]
pub struct Error(String);
impl<T: Display> From<T> for Error {
  fn from(value: T) -> Self {
    Error(value.to_string())
  }
}
pub type Result<T, E=Error> = std::result::Result<T, E>;

// Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
#[tauri::command(rename_all = "snake_case")]
#[instrument(level="info", fields(data_dir=%config.data_dir.display(), ok=config.data_dir.is_dir()))]
async fn list_data_names(config: State<'_, Config>) -> Result<Vec<String>> {
  let mut result = Vec::new();
  for i in std::fs::read_dir(&config.data_dir)? {
    let i = i?;
    debug!(?i, ok=i.file_type()?.is_dir());
    result.push(i.file_name().to_string_lossy().to_string())
  }
  Ok(result)
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
  let path = config.data_dir.join(&name);
  let df = LazyFrame::scan_parquet(&path, Default::default())?;
  info!(df=%df.clone().limit(10).collect()?.head(None));
  let df = df
    .filter(col("timestamp").gt(lit(0)))
    .with_column(col("timestamp").mul(lit(1000)).cast(DataType::Datetime(TimeUnit::Milliseconds, None)).cast(DataType::Date).alias("_date"))
    .group_by([col("_date")]).agg([
      col("total_eth").sum(),
      col("tx_count").cast(DataType::UInt64).sum(),
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
