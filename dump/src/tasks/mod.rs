pub mod uniswap;

use std::{future::Future, path::Path, sync::{atomic::AtomicU64, Arc}};

use polars::{frame::DataFrame, io::SerReader as _, prelude::{ParquetReader, ParquetWriter}};

use crate::{config::{next_cut, Config}, Result};

macro_rules! is_break {
  ($expr:expr) => {
    if !$expr { anyhow::bail!("break"); }
  };
}

pub struct RunConfig<'a, Fn: Executor> {
  pub data_dir: &'a Path,
  pub checkpoint: Arc<AtomicU64>,
  pub start: u64,
  pub end: u64,
  pub cut: u64,
  pub name: &'a str,
  pub executor: &'a Fn,
}

impl<'a, Fn: Executor> RunConfig<'a, Fn> {
  pub fn new(config: &'a Config, checkpoint: Arc<AtomicU64>, name: &'a str, executor: &'a Fn) -> Self {
    Self {
      data_dir: &config.data_dir,
      start: checkpoint.load(std::sync::atomic::Ordering::SeqCst),
      end: config.block_length,
      cut: config.cut,
      checkpoint,
      name,
      executor
    }
  }

  pub async fn run(self, mut tracker: impl EventListener<RunEvent>) -> Result<()> {
    let config = self;
    let mut start = config.start;
    let end = config.end;
    let cut = config.cut;
    is_break!(tracker.on_event(RunEvent { start, checkpoint: start, len: 0, cut, end }));
    while start < end {
      let checkpoint = next_cut(start, cut).min(end);
      info!(config.start, config.end, config.name, "running for {}..{}", start, checkpoint);
      if start < checkpoint {
        let tmp_filename = config.data_dir.join(format!("{}_{}.{}.parquet.tmp", config.name, cut, start/cut));
        // metrics::block::fetch_blocks(client, start, checkpoint).await?;
        let mut df = config.executor.run(start, checkpoint).await?;
        if start % cut != 0 {
          let old_file = std::fs::File::open(tmp_filename.with_extension(""))?;
          let old_df = ParquetReader::new(old_file).finish()?;
          df = old_df.vstack(&df)?;
        }
        let file = std::fs::File::create(&tmp_filename)?;
        ParquetWriter::new(file).finish(&mut df)?;
        is_break!(tracker.on_event(RunEvent { start, checkpoint, len: df.shape().0 as u64, cut, end }));
        std::fs::rename(&tmp_filename, tmp_filename.with_extension(""))?;
      }
      start = checkpoint;
      config.checkpoint.store(start, std::sync::atomic::Ordering::SeqCst);
    }
    Ok(())
  }
}

pub trait Executor {
  fn run(&self, start: u64, end: u64) -> impl Future<Output = Result<DataFrame>>;
}
#[allow(refining_impl_trait)]
impl<Fut: Future<Output = Result<DataFrame>>, F: Fn(u64, u64) -> Fut > Executor for F {
  fn run(&self, start: u64, end: u64) -> Fut {
    self(start, end)
  }
}

pub trait EventListener<E> {
  fn on_event(&mut self, event: E) -> bool;
}
impl<E> EventListener<E> for () {
  fn on_event(&mut self, _: E) -> bool {true}
}
impl<F: FnMut(E), E> EventListener<E> for F {
  fn on_event(&mut self, event: E) -> bool {
    self(event);
    true
  }
}

#[allow(unused)]
pub struct RunEvent {
  pub start: u64,
  pub checkpoint: u64,
  pub len: u64,
  pub cut: u64,
  pub end: u64,
}
