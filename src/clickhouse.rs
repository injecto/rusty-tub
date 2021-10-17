#![cfg(feature = "clickhouse")]

use std::{
    env,
    time::{Duration, SystemTime},
};

use clickhouse::{sql::Identifier, Client};
use tracing::debug;

use crate::{up, Container};

pub struct ClickHouse {
    _container: Container,
    url: String,
    db: String,
    client: Client,
}

static PORT: u16 = 8123;
static IMAGE_ENV: &str = "CLICKHOUSE_IMAGE";
static IMAGE_DEFAULT: &str = "yandex/clickhouse-server:21.8";

impl ClickHouse {
    /// # Panics
    ///
    /// Panics in case of any trouble while prepare container
    pub async fn new(db_prefix: &str) -> Self {
        let image = env::var(IMAGE_ENV)
            .ok()
            .unwrap_or_else(|| IMAGE_DEFAULT.to_owned());
        let container = up(&image, &[PORT]).await.expect("continer up");
        container
            .wait_for_port_listening(PORT, Duration::from_secs(5))
            .await
            .expect("wait_for_port_listening");

        let ch_ip = container.ip().await.expect("got ip of container");
        let url = format!("http://{}:{}", ch_ip, PORT);
        let db = format!(
            "{}_{}",
            db_prefix,
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("legal system time")
                .as_secs()
        );
        debug!(?db, ?url, "using database");

        let client = Client::default().with_url(&url);
        client
            .query("CREATE DATABASE ?")
            .bind(Identifier(&db))
            .execute()
            .await
            .expect("database created");

        container.exec(vec!["bash", "-c", &format!(r#"for f in /docker-entrypoint-initdb.d/*.sql; do clickhouse-client -n -d {} < "$f"; done"#, db)]).await.expect("database migrated");

        ClickHouse {
            _container: container,
            client: client.with_database(&db),
            url,
            db,
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn db(&self) -> &str {
        &self.db
    }
}
