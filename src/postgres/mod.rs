mod metadata_store;
mod notification_store;

use crate::JobSchedulerError;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use tokio_postgres::{Config, NoTls};
use tracing::error;

pub use metadata_store::PostgresMetadataStore;
pub use notification_store::PostgresNotificationStore;

#[derive(Clone)]
pub enum PostgresStore {
    Created(String),
    Inited(Pool),
}

impl PostgresStore {
    pub fn inited(&self) -> bool {
        matches!(self, PostgresStore::Inited(_))
    }
}

impl Default for PostgresStore {
    fn default() -> Self {
        let url = std::env::var("POSTGRES_URL")
            .map(Some)
            .unwrap_or_default()
            .unwrap_or_else(|| {
                let db_host =
                    std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string());
                let port = std::env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string());
                let dbname =
                    std::env::var("POSTGRES_DB").unwrap_or_else(|_| "postgres".to_string());
                let username =
                    std::env::var("POSTGRES_USERNAME").unwrap_or_else(|_| "postgres".to_string());
                let password = std::env::var("POSTGRES_PASSWORD")
                    .map(Some)
                    .unwrap_or_default();
                let application_name = std::env::var("POSTGRES_APP_NAME")
                    .map(Some)
                    .unwrap_or_default();

                "".to_string()
                    + "host="
                    + &*db_host
                    + " port="
                    + &*port
                    + " dbname="
                    + &*dbname
                    + " user="
                    + &*username
                    + &*match password {
                        Some(password) => " password=".to_string() + &*password,
                        None => "".to_string(),
                    }
                    + &*match application_name {
                        Some(application_name) => {
                            " application_name=".to_string() + &*application_name
                        }
                        None => "".to_string(),
                    }
            });
        Self::Created(url)
    }
}

impl PostgresStore {
    pub fn init(
        self,
    ) -> Pin<Box<dyn Future<Output = Result<PostgresStore, JobSchedulerError>> + Send>> {
        Box::pin(async move {
            match self {
                PostgresStore::Created(url) => {
                    #[cfg(feature = "postgres-openssl")]
                    let tls = postgres_openssl::TlsConnector;
                    #[cfg(feature = "postgres-native-tls")]
                    let tls = postgres_native_tls::TlsConnector;
                    #[cfg(not(any(
                        feature = "postgres-native-tls",
                        feature = "postgres-openssl"
                    )))]
                    let tls = NoTls;

                    let config = Config::from_str(&url).map_err(|e| {
                        error!("Error parsing postgres config {:?}", e);
                        JobSchedulerError::CantInit
                    })?;
                    let manager = Manager::from_config(
                        config,
                        tls,
                        ManagerConfig {
                            recycling_method: RecyclingMethod::Verified,
                        },
                    );
                    let pool = Pool::builder(manager).max_size(4).build().map_err(|e| {
                        error!("Error creating postgres pool {:?}", e);
                        JobSchedulerError::CantInit
                    })?;

                    let client = pool.get().await.map_err(|e| {
                        error!("Error connecting to postgres {:?}", e);
                        JobSchedulerError::CantInit
                    })?;
                    drop(client);

                    Ok(PostgresStore::Inited(pool))
                }
                PostgresStore::Inited(pool) => Ok(PostgresStore::Inited(pool)),
            }
        })
    }
}
