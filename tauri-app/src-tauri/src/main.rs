// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

#[macro_use] extern crate tracing;

use std::{collections::{BTreeMap, HashMap}, fmt::Display, path::PathBuf, sync::{Arc, Mutex}};

use polars::{frame::DataFrame, lazy::frame::{IntoLazy, LazyFrame}, prelude::SortMultipleOptions};
use polars_plan::dsl::Expr;
use tauri::State;
use tracing_subscriber::fmt::format::FmtSpan;

struct ExprLoader {
  inner: Arc<Mutex<HashMap<String, Vec<Expr>>>>,
}

impl ExprLoader {
  fn new() -> Self {
    Self {
      inner: Arc::new(Mutex::new(HashMap::new())),
    }
  }

  fn insert(&self, name: String, exprs: Vec<Expr>) {
    self.inner.lock().unwrap().insert(name, exprs);
  }

  fn get(&self, name: &str) -> Option<Vec<Expr>> {
    self.inner.lock().unwrap().get(name).cloned()
  }
}

impl std::fmt::Debug for ExprLoader {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self.inner.try_lock() {
      Ok(inner) => {
        let mut f = f.debug_struct("ExprLoader");
        for (k, v) in inner.iter() {
          f.field(k, &v.len());
        }
        f.finish()
      }
      Err(_) => f.debug_tuple("ExprLoader").field(&"locked").finish(),
    }
  }
}

#[derive(Debug)]
struct Config {
  data_dir: PathBuf,
  exprs: ExprLoader,
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
    } else if filename.ends_with(".parquet") {
      map.entry(filename.to_string()).or_default().push((0, filename));
    }
  }
  Ok(map
    .into_iter()
    .map(|(name, collection)| Dataset::new(name, collection))
    .collect())
}

fn get_data_info(config: &Config, name: &str) -> Result<Dataset> {
  let mut collection = Vec::new();
  if name.ends_with(".parquet") {
    collection.push((0, name.to_string()));
    return Ok(Dataset::new(name.to_string(), collection))
  }
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

fn exprs_from_str(s: &str) -> Result<Vec<polars_plan::dsl::Expr>> {
  Ok(s.lines().map(|i| serde_json::from_str(i)).collect::<Result<Vec<_>, _>>()?)
}

fn infer_expr_from_schema(schema: &polars::prelude::Schema) -> Result<Vec<Expr>> {
  use polars::prelude::DataType;
  use polars::lazy::dsl::*;
  let mut exprs = Vec::new();
  for i in schema.iter_fields() {
    match i.data_type() {
      DataType::Int64 => exprs.push(col(i.name())),
      DataType::Float64 => {
        exprs.push(col(i.name()).mean().alias(&format!("{}:mean", &i.name)));
      }
      _ => {},
    }
    exprs.push(col(i.name()).fill_null_with_strategy(polars::chunked_array::ops::FillNullStrategy::Forward(None)).last())
  }
  Ok(exprs)
}

#[instrument(level="debug", skip_all, fields(name=%name))]
fn load_exprs(config: &Config, name: &str, schema: Option<&polars::prelude::Schema>) -> Result<Vec<Expr>> {
  let exprs = match config.exprs.get(name) {
    Some(exprs) => exprs,
    None => {
      let exprs = match name {
        _ if name.starts_with("block_metrics_") => exprs_from_str(include_str!("../exprs/bm.jsonl"))?,
        _ if name.starts_with("uniswap_pair_block_") => exprs_from_str(include_str!("../exprs/upair.jsonl"))?,
        _ => if let Some(schema) = schema {
          infer_expr_from_schema(schema)?
        } else {
          return Err("no schema to infer expr".into())
        }
      };
      config.exprs.insert(name.to_string(), exprs.clone());
      exprs
    }
  };

  // TODO: filter invalid exprs here
  info!(?exprs);
  Ok(exprs)
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
  use std::ops::*;
  let info = get_data_info(&config, &name)?;
  if info.collection.is_empty() {
    return Err("nothing in collection".into())
  }
  let df_s = info.collection.iter().map(|(_, f)| {
    let path = config.data_dir.join(&f);
    LazyFrame::scan_parquet(&path, Default::default())?.collect()
  }).collect::<Result<Vec<_>, _>>()?;
  let mut df = None::<DataFrame>;
  let mut schema = None;
  let mut exprs = None;
  for b in df_s {
    df = Some(match df {
      Some(a) => a.vstack(&b)?,
      None => {
        schema = Some(b.schema());
        exprs = Some(load_exprs(&config, &name, Some(&b.schema()))?);
        b
      }
    })
  }
  let df = df.unwrap().lazy();
  info!(df=%df.clone().limit(10).collect()?.head(None));

  let schema = schema.ok_or("no schema")?;
  let exprs = exprs.ok_or("no exprs")?;

  let df = if schema.get("timestamp").is_none() {
    // TODO: cache from block_metrics
    df.with_column(col("height").mul(lit(15)).add(lit(1438269973)).alias("timestamp"))
  } else {
    df
  };
  let df = df
    .filter(col("timestamp").gt(lit(0)))
    .with_column(col("timestamp").mul(lit(1000)).cast(DataType::Datetime(TimeUnit::Milliseconds, None)).cast(DataType::Date).alias("_date"))
    .group_by([col("_date")]).agg(exprs);
  info!(agg=%df.clone().limit(10).collect()?.head(None));
  let df = df
    .with_column(col("_date").cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
    .sort(["_date"], SortMultipleOptions::default())
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
    exprs: ExprLoader::new(),
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
