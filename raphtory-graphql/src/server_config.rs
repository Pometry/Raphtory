use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct LoggingConfig {
    pub(crate) log_level: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AppConfig {
    pub(crate) logging: LoggingConfig,
}

pub fn load_config() -> Result<AppConfig, ConfigError> {
    let settings = Config::builder()
        .add_source(File::with_name("config"))
        .add_source(Environment::with_prefix("APP"))
        .build()?;

    settings.try_deserialize::<AppConfig>()
}
