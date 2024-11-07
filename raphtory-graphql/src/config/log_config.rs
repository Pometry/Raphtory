use serde::Deserialize;
use tracing_subscriber::EnvFilter;

pub const DEFAULT_LOG_LEVEL: &'static str = "INFO";

#[derive(Debug, Deserialize, PartialEq, Clone, serde::Serialize)]
pub struct LoggingConfig {
    pub log_level: String,
}

impl LoggingConfig {
    pub fn get_log_env(&self) -> EnvFilter {
        raphtory_api::core::utils::logging::get_log_env(self.log_level.clone())
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            log_level: DEFAULT_LOG_LEVEL.to_owned(),
        }
    }
}
