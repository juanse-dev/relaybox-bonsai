use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub bind: SocketAddr,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let database_url = std::env::var("RELAYBOX_DATABASE_URL")
            .unwrap_or_else(|_| "sqlite://relaybox.db".to_string());
        if !database_url.starts_with("sqlite://") {
            return Err(format!(
                "RELAYBOX_DATABASE_URL must use the sqlite:// scheme, got: {database_url:?}"
            ));
        }

        let bind = std::env::var("RELAYBOX_BIND").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
        let bind = bind
            .parse::<SocketAddr>()
            .map_err(|_| format!("RELAYBOX_BIND is not a valid host:port address: {bind:?}"))?;

        Ok(Self { database_url, bind })
    }
}
