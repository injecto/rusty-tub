#![cfg(feature = "clickhouse")]

use rusty_tub::clickhouse::ClickHouse;

#[tokio::test]
async fn clickhouse_integration() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("rusty-tub=trace")
        .try_init();

    let db_prefix = "clickhouse_integration";
    let clickhouse = ClickHouse::new(db_prefix).await;
    let client = clickhouse.client();

    let actual_database = client
        .query("SELECT currentDatabase()")
        .fetch_one::<String>()
        .await
        .expect("currentDatabase selected");
    assert!(actual_database.starts_with(db_prefix));
}
