// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

#[macro_use] extern crate tracing;

use std::{fmt::Display, path::PathBuf};

use tauri::State;

#[derive(Debug)]
struct Config {
  data_dir: PathBuf,
}

#[derive(serde::Serialize)]
pub struct Error;
impl<T: Display> From<T> for Error {
  fn from(value: T) -> Self {
    Error
  }
}
pub type Result<T, E=Error> = std::result::Result<T, E>;

// Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
#[tauri::command(rename_all = "snake_case")]
async fn list_data_names(config: State<'_, Config>) -> Result<Vec<String>> {
  info!(data_dir=%config.data_dir.display(), ok=config.data_dir.is_dir());
  let mut result = Vec::new();
  for i in std::fs::read_dir(&config.data_dir)? {
    let i = i?;
    info!(?i, ok=i.file_type()?.is_dir());
    result.push(i.file_name().to_string_lossy().to_string())
  }
  Ok(result)
}

fn main() {
  tracing_subscriber::fmt::init();
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
    .invoke_handler(tauri::generate_handler![list_data_names])
    .run(tauri::generate_context!())
    .expect("error while running tauri application");
}
