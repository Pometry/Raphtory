use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct LoggingConfig {
    pub log_level: String,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct CacheConfig {
    pub capacity: u64,
    pub tti_seconds: u64,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct AuthConfig {
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub tenant_id: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct AppConfig {
    pub logging: LoggingConfig,
    pub cache: CacheConfig,
    pub auth: AuthConfig,
}

pub struct AppConfigBuilder {
    logging: LoggingConfig,
    cache: CacheConfig,
    auth: AuthConfig,
}

impl AppConfigBuilder {
    pub fn new() -> Self {
        Self {
            logging: LoggingConfig {
                log_level: "INFO".to_string(),
            },
            cache: CacheConfig {
                capacity: 30,
                tti_seconds: 900,
            },
            auth: AuthConfig {
                client_id: None,
                client_secret: None,
                tenant_id: None,
            },
        }
    }

    pub fn from(config: AppConfig) -> Self {
        Self {
            logging: config.logging,
            cache: config.cache,
            auth: config.auth,
        }
    }

    pub fn with_log_level(mut self, log_level: String) -> Self {
        self.logging.log_level = log_level;
        self
    }

    pub fn with_cache_capacity(mut self, cache_capacity: u64) -> Self {
        self.cache.capacity = cache_capacity;
        self
    }

    pub fn with_cache_tti_seconds(mut self, tti_seconds: u64) -> Self {
        self.cache.tti_seconds = tti_seconds;
        self
    }

    pub fn with_auth_client_id(mut self, client_id: String) -> Self {
        self.auth.client_id = Some(client_id);
        self
    }

    pub fn with_auth_client_secret(mut self, client_secret: String) -> Self {
        self.auth.client_secret = Some(client_secret);
        self
    }

    pub fn with_auth_tenant_id(mut self, tenant_id: String) -> Self {
        self.auth.tenant_id = Some(tenant_id);
        self
    }

    pub fn build(self) -> AppConfig {
        AppConfig {
            logging: self.logging,
            cache: self.cache,
            auth: self.auth,
        }
    }
}

// Order of precedence of config loading: config args >> config path >> config default
// Note: Since config args takes precedence over config path, ensure not to provide config args when starting server from a compile rust instance.
// This would cause configs from config paths to be ignored. The reason it has been implemented so is to avoid having to pass all the configs as
// args from the python instance i.e., being able to provide configs from config path as default configs and yet give precedence to config args.
pub fn load_config(
    app_config: Option<AppConfig>,
    config_path: Option<PathBuf>,
) -> Result<AppConfig, ConfigError> {
    let mut settings_config_builder = Config::builder();
    if let Some(config_path) = config_path {
        settings_config_builder = settings_config_builder.add_source(File::from(config_path));
    }
    let settings = settings_config_builder.build()?;

    let mut app_config_builder = if let Some(app_config) = app_config {
        AppConfigBuilder::from(app_config)
    } else {
        AppConfigBuilder::new()
    };

    // Override with provided configs from config file if any
    if let Some(log_level) = settings.get::<String>("logging.log_level").ok() {
        app_config_builder = app_config_builder.with_log_level(log_level);
    }
    if let Some(cache_capacity) = settings.get::<u64>("cache.capacity").ok() {
        app_config_builder = app_config_builder.with_cache_capacity(cache_capacity);
    }
    if let Some(cache_tti_seconds) = settings.get::<u64>("cache.tti_seconds").ok() {
        app_config_builder = app_config_builder.with_cache_tti_seconds(cache_tti_seconds);
    }
    if let Some(client_id) = settings.get::<String>("auth.client_id").ok() {
        app_config_builder = app_config_builder.with_auth_client_id(client_id);
    }
    if let Some(client_secret) = settings.get::<String>("auth.client_secret").ok() {
        app_config_builder = app_config_builder.with_auth_client_secret(client_secret);
    }
    if let Some(tenant_id) = settings.get::<String>("auth.tenant_id").ok() {
        app_config_builder = app_config_builder.with_auth_tenant_id(tenant_id);
    }

    Ok(app_config_builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_load_config_from_toml() {
        let config_toml = r#"
            [logging]
            log_level = "DEBUG"
            
            [cache]
            tti_seconds = 1000
        "#;
        let config_path = PathBuf::from("test_config.toml");
        fs::write(&config_path, config_toml).unwrap();

        let result = load_config(None, Some(config_path.clone()));
        let expected_config = AppConfigBuilder::new()
            .with_log_level("DEBUG".to_string())
            .with_cache_capacity(30)
            .with_cache_tti_seconds(1000)
            .build();

        assert_eq!(result.unwrap(), expected_config);

        // Cleanup: delete the test TOML file
        fs::remove_file(config_path).unwrap();
    }

    #[test]
    fn test_load_config_with_custom_cache() {
        let app_config = AppConfigBuilder::new()
            .with_cache_capacity(50)
            .with_cache_tti_seconds(1200)
            .build();

        let result = load_config(Some(app_config.clone()), None);

        assert_eq!(result.unwrap(), app_config);
    }

    #[test]
    fn test_load_config_with_custom_auth() {
        let app_config = AppConfigBuilder::new()
            .with_auth_client_id("custom_client_id".to_string())
            .with_auth_client_secret("custom_client_secret".to_string())
            .with_auth_tenant_id("custom_tenant_id".to_string())
            .build();

        let result = load_config(Some(app_config.clone()), None);

        assert_eq!(result.unwrap(), app_config);
    }
}
