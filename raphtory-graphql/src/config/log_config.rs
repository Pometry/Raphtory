use serde::Deserialize;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct LoggingConfig {
    pub log_level: String,
}

impl LoggingConfig {
    pub fn get_log_env(&self) -> EnvFilter {
        EnvFilter::new(format!(
            "raphtory-graphql={},raphtory={},raphtory-api={},",
            self.log_level, self.log_level, self.log_level
        ))
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            log_level: "INFO".to_string(),
        }
    }
}
