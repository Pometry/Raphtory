use super::auth_config::{AuthConfig, PublicKeyError, PUBLIC_KEY_DECODING_ERR_MSG};
use crate::config::{
    cache_config::CacheConfig, index_config::IndexConfig, log_config::LoggingConfig,
    otlp_config::TracingConfig,
};
use config::{Config, ConfigError, File};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Deserialize, PartialEq, Clone, Serialize)]
pub struct AppConfig {
    pub logging: LoggingConfig,
    pub cache: CacheConfig,
    pub tracing: TracingConfig,
    pub auth: AuthConfig,
    pub index: IndexConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            logging: LoggingConfig::default(),
            cache: CacheConfig::default(),
            tracing: TracingConfig::default(),
            auth: AuthConfig::default(),
            index: IndexConfig::default(),
        }
    }
}

pub struct AppConfigBuilder {
    logging: LoggingConfig,
    cache: CacheConfig,
    tracing: TracingConfig,
    auth: AuthConfig,
    index: IndexConfig,
}

impl From<AppConfig> for AppConfigBuilder {
    fn from(config: AppConfig) -> Self {
        Self {
            logging: config.logging,
            cache: config.cache,
            tracing: config.tracing,
            auth: config.auth,
            index: config.index,
        }
    }
}

impl AppConfigBuilder {
    pub fn new() -> Self {
        AppConfig::default().into()
    }

    pub fn with_log_level(mut self, log_level: String) -> Self {
        self.logging.log_level = log_level;
        self
    }

    pub fn with_tracing(mut self, tracing: bool) -> Self {
        self.tracing.tracing_enabled = tracing;
        self
    }

    pub fn with_otlp_agent_host(mut self, otlp_agent_host: String) -> Self {
        self.tracing.otlp_agent_host = otlp_agent_host;
        self
    }

    pub fn with_otlp_agent_port(mut self, otlp_agent_port: String) -> Self {
        self.tracing.otlp_agent_port = otlp_agent_port;
        self
    }

    pub fn with_otlp_tracing_service_name(mut self, otlp_tracing_service_name: String) -> Self {
        self.tracing.otlp_tracing_service_name = otlp_tracing_service_name;
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

    pub fn with_auth_public_key(
        mut self,
        public_key: Option<String>,
    ) -> Result<Self, PublicKeyError> {
        if let Some(public_key) = public_key {
            self.auth.public_key = Some(public_key.try_into()?);
        }
        Ok(self)
    }

    pub fn with_auth_enabled_for_reads(mut self, enabled_for_reads: bool) -> Self {
        self.auth.enabled_for_reads = enabled_for_reads;
        self
    }

    pub fn with_create_index(mut self, create_index: bool) -> Self {
        self.index.create_index = create_index;
        self
    }

    pub fn build(self) -> AppConfig {
        AppConfig {
            logging: self.logging,
            cache: self.cache,
            tracing: self.tracing,
            auth: self.auth,
            index: self.index,
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
    if let Some(tracing) = settings.get::<bool>("tracing.tracing_enabled").ok() {
        app_config_builder = app_config_builder.with_tracing(tracing);
    }

    if let Some(otlp_agent_host) = settings.get::<String>("tracing.otlp_agent_host").ok() {
        app_config_builder = app_config_builder.with_otlp_agent_host(otlp_agent_host);
    }

    if let Some(otlp_agent_port) = settings.get::<String>("tracing.otlp_agent_port").ok() {
        app_config_builder = app_config_builder.with_otlp_agent_port(otlp_agent_port);
    }

    if let Some(otlp_tracing_service_name) = settings
        .get::<String>("tracing.otlp_tracing_service_name")
        .ok()
    {
        app_config_builder =
            app_config_builder.with_otlp_tracing_service_name(otlp_tracing_service_name);
    }

    if let Some(cache_capacity) = settings.get::<u64>("cache.capacity").ok() {
        app_config_builder = app_config_builder.with_cache_capacity(cache_capacity);
    }
    if let Some(cache_tti_seconds) = settings.get::<u64>("cache.tti_seconds").ok() {
        app_config_builder = app_config_builder.with_cache_tti_seconds(cache_tti_seconds);
    }

    if let Ok(public_key) = settings.get::<Option<String>>("auth.public_key") {
        app_config_builder = app_config_builder
            .with_auth_public_key(public_key)
            .map_err(|_| ConfigError::Message(PUBLIC_KEY_DECODING_ERR_MSG.to_owned()))?;
    }
    if let Ok(enabled_for_reads) = settings.get::<bool>("auth.enabled_for_reads") {
        app_config_builder = app_config_builder.with_auth_enabled_for_reads(enabled_for_reads);
    }

    Ok(app_config_builder.build())
}
