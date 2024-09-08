use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
  pub data_dir: PathBuf,
  pub endpoint: String,
  pub block_length: u64,
  pub cut: u64,
}

const DEFAULT_CUT: u64 = 1000000;
/// A cut means 0..CUT, CUT..2*CUT, etc.
/// which means block number 10000 is in a new file.
/// just like what reth do.
pub const fn next_cut(i: u64, cut: u64) -> u64 {
  i / cut * cut + cut
}

impl Config {
  pub fn default() -> Self {
    Self {
      data_dir: "data".to_string().into(),
      endpoint: "http://localhost:8545".to_string(),
      block_length: 0,
      cut: DEFAULT_CUT,
    }
  }

  pub fn from_env() -> Self {
    Self {
      data_dir: std::env::var("DATA_DIR").unwrap_or_else(|_| "data".to_string()).into(),
      endpoint: format!("http://{}", std::env::var("RETH_HTTP_RPC").as_deref().unwrap_or("127.0.0.1:8545")),
      block_length: 0,
      cut: DEFAULT_CUT,
    }
  }
}
