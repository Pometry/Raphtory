use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, PartialEq)]
pub(crate) struct LoggingConfig {
    pub log_level: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        LoggingConfig {
            log_level: "INFO".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct CacheConfig {
    pub capacity: u64,
    pub tti_seconds: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            capacity: 30,
            tti_seconds: 900,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub(crate) struct AppConfig {
    pub(crate) logging: LoggingConfig,
    pub(crate) cache: CacheConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            logging: Default::default(),
            cache: Default::default(),
        }
    }
}

// Order of precedence of config loading: config args >> config path >> config default
// Note: Since config args takes precedence over config path, ensure not to provide config args when starting server from a compile rust instance.
// This would cause configs from config paths to be ignored. The reason it has been implemented so is to avoid having to pass all the configs as
// args from the python instance i.e., being able to provide configs from config path as default configs and yet give precedence to config args.
pub fn load_config(
    cache_config: Option<CacheConfig>,
    config_path: Option<&Path>,
) -> Result<AppConfig, ConfigError> {
    let mut config_builder = Config::builder();
    if let Some(config_path) = config_path {
        config_builder = config_builder.add_source(File::from(config_path));
    }
    let settings = config_builder.build()?;

    // Load default configs
    let mut loaded_config = AppConfig::default();

    // Override with provided configs from config file if any
    if let Some(log_level) = settings.get::<String>("logging.log_level").ok() {
        loaded_config.logging.log_level = log_level;
    }
    if let Some(capacity) = settings.get::<u64>("cache.capacity").ok() {
        loaded_config.cache.capacity = capacity;
    }
    if let Some(tti_seconds) = settings.get::<u64>("cache.tti_seconds").ok() {
        loaded_config.cache.tti_seconds = tti_seconds;
    }

    // Override with provided cache configs if any
    if let Some(cache_config) = cache_config {
        loaded_config.cache = cache_config;
    }

    Ok(loaded_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_load_config_from_toml() {
        // Prepare a test TOML configuration file
        let config_toml = r#"
            [logging]
            log_level = "DEBUG"
            
            [cache]
            tti_seconds = 1000
        "#;
        let config_path = Path::new("test_config.toml");
        fs::write(config_path, config_toml).unwrap();

        // Load config using the test TOML file
        let result = load_config(None, Some(config_path));
        let expected_config = AppConfig {
            logging: LoggingConfig {
                log_level: "DEBUG".to_string(),
            },
            cache: CacheConfig {
                capacity: 30,
                tti_seconds: 1000,
            },
        };

        assert_eq!(result.unwrap(), expected_config);

        // Cleanup: delete the test TOML file
        fs::remove_file(config_path).unwrap();
    }

    #[test]
    fn test_load_config_with_custom_cache() {
        // Prepare a custom cache configuration
        let custom_cache = CacheConfig {
            capacity: 50,
            tti_seconds: 1200,
        };

        // Load config with custom cache configuration
        let result = load_config(Some(custom_cache), None);
        let expected_config = AppConfig {
            logging: LoggingConfig {
                log_level: "INFO".to_string(),
            }, // Default logging level
            cache: CacheConfig {
                capacity: 50,
                tti_seconds: 1200,
            },
        };

        assert_eq!(result.unwrap(), expected_config);
    }
}
