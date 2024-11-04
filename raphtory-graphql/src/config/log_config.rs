use serde::Deserialize;
use tracing_subscriber::EnvFilter;

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
            log_level: "INFO".to_string(),
        }
    }
}
