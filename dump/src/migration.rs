use std::path::Path;

use anyhow::Result;
use indexmap::IndexMap;

use crate::{save_stage, DatasetName, PairStage, Stage};

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct StageMigration {
  /// uniswap_factory => uniswap_factory_events
  pub uniswap_factory: Option<u64>,
  /// uniswap_pair => uniswap_pair_events
  pub uniswap_pair: Option<IndexMap<String, PairStage>>,
}

fn rename_prefix<P: AsRef<Path>>(data_dir: P, from: &str, to: &str) -> Result<usize> {
  let data_dir = data_dir.as_ref();
  let mut count = 0;
  for i in std::fs::read_dir(data_dir)? {
    let i = i?;
    let filename = i.file_name().to_string_lossy().to_string();

    if filename.starts_with(from) {
      debug!(filename, "checking");
      if !filename.ends_with(".parquet") {
        info!(filename, "not a parquet file");
        continue
      }
      if filename.starts_with(to) {
        info!(filename, "skip already renamed");
        continue
      }
    } else {
      continue
    }
    let new_filename = filename.replacen(from, to, 1);

    // check if dataset name
    let old = DatasetName::from_string(&filename);
    let new = DatasetName::from_string(&new_filename);
    if let (Some((old, old_part)), Some((new, new_part))) = (old, new) {
      assert_eq!(old_part, new_part);
      assert_eq!(old.name, from.strip_suffix('_').unwrap_or(from));
      assert_eq!(new.name, to.strip_suffix('_').unwrap_or(from));
      assert_eq!(old.cut, new.cut);
      assert_eq!(old.idx, new.idx);
    } else {
      warn!(filename, new_filename, "not a dataset name");
      continue;
    }

    info!(filename, new_filename, "rename");
    std::fs::rename(data_dir.join(&filename), data_dir.join(&new_filename))?;
    count += 1;
  }
  if count == 0 {
    return Err(anyhow::anyhow!("no file renamed"));
  }
  Ok(count)
}

pub fn migrate<P: AsRef<Path>>(data_dir: P, mut stage: Stage, migration: StageMigration) -> Result<Stage> {
  let data_dir = data_dir.as_ref();
  if let Some(uniswap_factory) = migration.uniswap_factory {
    if uniswap_factory > stage.uniswap_factory_events {
      rename_prefix(data_dir, "uniswap_factory_", "uniswap_factory_events_")?;
    }
    stage.uniswap_factory_events = uniswap_factory;
    save_stage(data_dir, &stage)?;
  }
  if let Some(uniswap_pair) = migration.uniswap_pair {
    for (name, pair) in uniswap_pair {
      if pair.checkpoint.load(std::sync::atomic::Ordering::SeqCst)
        > stage.uniswap_pair_events.get(&name).map(|p| p.checkpoint.load(std::sync::atomic::Ordering::SeqCst)).unwrap_or(0) {
        rename_prefix(data_dir, &format!("uniswap_pair_{name}_"), &format!("uniswap_pair_events_{name}_"))?;
      }
      stage.uniswap_pair_events.entry(name).or_insert(pair);
      save_stage(data_dir, &stage)?;
    }
  }
  Ok(stage)
}
