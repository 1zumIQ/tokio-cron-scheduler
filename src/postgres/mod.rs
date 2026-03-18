mod metadata_store;
mod notification_store;

use crate::JobSchedulerError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info, warn};

pub use metadata_store::PostgresMetadataStore;
pub use notification_store::PostgresNotificationStore;

#[derive(Clone)]
pub enum PostgresStore {
    Created(String),
    Inited(Arc<RwLock<Client>>),
}

impl PostgresStore {
    pub fn inited(&self) -> bool {
        matches!(self, PostgresStore::Inited(_))
    }
}

#[cfg(test)]
mod test {
    use super::PostgresStore;
    use std::time::Duration;

    #[test]
    fn reconnect_backoff_caps() {
        assert_eq!(PostgresStore::retry_backoff(0), Duration::from_secs(1));
        assert_eq!(PostgresStore::retry_backoff(1), Duration::from_secs(2));
        assert_eq!(PostgresStore::retry_backoff(2), Duration::from_secs(4));
        assert_eq!(PostgresStore::retry_backoff(3), Duration::from_secs(8));
        assert_eq!(PostgresStore::retry_backoff(4), Duration::from_secs(16));
        assert_eq!(PostgresStore::retry_backoff(10), Duration::from_secs(16));
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
    async fn connect(
        url: &str,
    ) -> Result<
        (
            Client,
            Pin<Box<dyn Future<Output = Result<(), tokio_postgres::Error>> + Send>>,
        ),
        tokio_postgres::Error,
    > {
        #[cfg(feature = "postgres-openssl")]
        let tls = postgres_openssl::TlsConnector;
        #[cfg(feature = "postgres-native-tls")]
        let tls = postgres_native_tls::TlsConnector;
        #[cfg(not(any(feature = "postgres-native-tls", feature = "postgres-openssl")))]
        let tls = NoTls;

        let (client, connection) = tokio_postgres::connect(url, tls).await?;
        Ok((client, Box::pin(connection)))
    }

    fn retry_backoff(attempt: u32) -> Duration {
        let secs = 2_u64.saturating_pow(attempt.min(4));
        Duration::from_secs(secs)
    }

    fn spawn_connection_task(
        url: String,
        client_ref: Arc<RwLock<Client>>,
        mut connection: Pin<Box<dyn Future<Output = Result<(), tokio_postgres::Error>> + Send>>,
    ) {
        tokio::spawn(async move {
            let mut attempt = 0_u32;
            loop {
                match connection.await {
                    Ok(_) => {
                        warn!("Postgres connection task ended unexpectedly");
                    }
                    Err(e) => {
                        error!("Error with Postgres Connection {:?}", e);
                    }
                }

                let sleep_duration = Self::retry_backoff(attempt);
                warn!(
                    ?sleep_duration,
                    "Trying to reconnect to Postgres after connection loss"
                );
                tokio::time::sleep(sleep_duration).await;

                connection = loop {
                    match Self::connect(&url).await {
                        Ok((client, next_connection)) => {
                            let mut writer = client_ref.write().await;
                            *writer = client;
                            attempt = 0;
                            info!("Reconnected to Postgres");
                            break next_connection;
                        }
                        Err(e) => {
                            attempt = attempt.saturating_add(1);
                            error!("Error reconnecting to postgres {:?}", e);
                            let sleep_duration = Self::retry_backoff(attempt);
                            tokio::time::sleep(sleep_duration).await;
                        }
                    }
                };
            }
        });
    }

    pub fn init(
        self,
    ) -> Pin<Box<dyn Future<Output = Result<PostgresStore, JobSchedulerError>> + Send>> {
        Box::pin(async move {
            match self {
                PostgresStore::Created(url) => {
                    let connect = Self::connect(&url).await;
                    if let Err(e) = connect {
                        error!("Error connecting to postgres {:?}", e);
                        return Err(JobSchedulerError::CantInit);
                    }
                    let (client, connection) = connect.unwrap();
                    let client_ref = Arc::new(RwLock::new(client));
                    Self::spawn_connection_task(url, client_ref.clone(), connection);
                    Ok(PostgresStore::Inited(client_ref))
                }
                PostgresStore::Inited(client) => Ok(PostgresStore::Inited(client)),
            }
        })
    }
}
