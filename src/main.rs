use anyhow::{anyhow, Result};
use relaybox::{app, config};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let config = config::Config::from_env().map_err(|e| anyhow!("config: {e}"))?;
    let app = app::App::create(&config.database_url).await?;

    info!(bind = %config.bind, "relaybox listening");
    app.run(config.bind).await
}
