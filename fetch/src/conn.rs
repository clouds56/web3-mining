use clickhouse::{Client, Row, error::Result, inserter::Inserter};
use std::time::Duration;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Password(String);

impl std::fmt::Debug for Password {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_tuple("Password").field(&self.0.len()).finish()
  }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
  pub url: String,
  pub username: String,
  pub password: Password,
  pub db_name: String,
}

pub async fn clkhs_init_client(config: &Config) -> Result<Client> {
  let client = Client::default()
    .with_url(&config.url)
    .with_user(&config.username)
    .with_password(&config.password.0);
  client.query(format!("CREATE DATABASE IF NOT EXISTS {}", config.db_name).as_str()).execute().await?;

  Ok(client.with_database(&config.db_name))
}

// pub async fn clkhs_select_id(config: &Config) -> Result<u64> {
//   let client = clkhs_init_client(config).await?;
//   let id = client
//     .query(format!("SELECT MAX(id) FROM tx_log.{}", TABLE_EVENTS).as_str())
//     .fetch_one::<u64>()
//     .await?;

//   println!("id() = {id}");
//   Ok(id)
// }

pub async fn clkhs_insert<T: Row + serde::Serialize>(client: &Client, table: &str, rows: &[T]) -> Result<()> {
  let mut inserter: Inserter<T> = client.inserter(table)?
    .with_max_entries(100_000_000)
    .with_period(Some(Duration::from_secs(10)))
    .with_timeouts(Some(Duration::from_secs(3)), Some(Duration::from_secs(3)));

  for i in 0..rows.len(){
    inserter.write(&rows[i]).await?;
    if i % 10_000 == 1{
      println!("commit {table}: {i} total:{}", rows.len());
      inserter.commit().await?;
    }
  }
  inserter.end().await?;

  Ok(())
}
