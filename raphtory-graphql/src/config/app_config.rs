use super::auth_config::{AuthConfig, PublicKeyError, PUBLIC_KEY_DECODING_ERR_MSG};
#[cfg(feature = "search")]
use crate::config::index_config::IndexConfig;
use crate::{
    config::{
        cache_config::CacheConfig,
        concurrency_config::ConcurrencyConfig,
        log_config::LoggingConfig,
        otlp_config::{TracingConfig, TracingLevel, TracingProtocol},
        schema_config::SchemaConfig,
    },
    server::ServerError,
};
use clap::{
    parser::{RawValues, ValueSource},
    ArgMatches,
};
use config::{Config, ConfigError, File, Map, Source, Value, ValueKind};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
};

#[derive(Debug, Default, Deserialize, PartialEq, Clone, Serialize)]
pub struct AppConfig {
    pub logging: LoggingConfig,
    pub cache: CacheConfig,
    pub tracing: TracingConfig,
    pub auth: AuthConfig,
    pub concurrency: ConcurrencyConfig,
    pub schema: SchemaConfig,
    pub public_dir: Option<PathBuf>,
    #[cfg(feature = "search")]
    pub index: IndexConfig,
}

pub struct AppConfigBuilder {
    logging: LoggingConfig,
    cache: CacheConfig,
    tracing: TracingConfig,
    auth: AuthConfig,
    concurrency: ConcurrencyConfig,
    schema: SchemaConfig,
    public_dir: Option<PathBuf>,
    #[cfg(feature = "search")]
    index: IndexConfig,
}

impl From<AppConfig> for AppConfigBuilder {
    fn from(config: AppConfig) -> Self {
        Self {
            logging: config.logging,
            cache: config.cache,
            tracing: config.tracing,
            auth: config.auth,
            concurrency: config.concurrency,
            schema: config.schema,
            public_dir: config.public_dir,
            #[cfg(feature = "search")]
            index: config.index,
        }
    }
}

impl Default for AppConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl AppConfigBuilder {
    pub fn new() -> Self {
        AppConfig::default().into()
    }

    pub fn load_from_path(mut self, config_path: impl AsRef<Path>) -> Result<Self, ServerError> {
        let mut settings = Config::builder()
            .add_source(File::from(config_path.as_ref()))
            .build()?;

        // Override with provided configs from config file if any
        if let Ok(log_level) = settings.get::<String>("logging.log_level") {
            self = self.with_log_level(log_level);
        }

        if let Ok(tracing) = settings.get::<bool>("tracing.tracing_enabled") {
            self = self.with_tracing(tracing);
        }

        if let Ok(tracing_level) = settings.get::<TracingLevel>("tracing.tracing_level") {
            self = self.with_tracing_level(tracing_level);
        }

        if let Ok(otlp_agent_host) = settings.get::<String>("tracing.otlp_agent_host") {
            self = self.with_otlp_agent_host(otlp_agent_host);
        }

        if let Ok(otlp_agent_port) = settings.get::<String>("tracing.otlp_agent_port") {
            self = self.with_otlp_agent_port(otlp_agent_port);
        }

        if let Ok(otlp_tracing_service_name) =
            settings.get::<String>("tracing.otlp_tracing_service_name")
        {
            self = self.with_otlp_tracing_service_name(otlp_tracing_service_name);
        }

        if let Ok(cache_capacity) = settings.get::<u64>("cache.capacity") {
            self = self.with_cache_capacity(cache_capacity);
        }

        if let Ok(public_key) = settings.get::<Option<String>>("auth.public_key") {
            self = self.with_auth_public_key(public_key)?;
        }
        if let Ok(require_auth_for_reads) = settings.get::<bool>("auth.require_auth_for_reads") {
            self = self.with_require_auth_for_reads(require_auth_for_reads);
        }

        if let Ok(heavy_query_limit) =
            settings.get::<Option<usize>>("concurrency.heavy_query_limit")
        {
            self = self.with_heavy_query_limit(heavy_query_limit);
        }
        if let Ok(exclusive_writes) = settings.get::<bool>("concurrency.exclusive_writes") {
            self = self.with_exclusive_writes(exclusive_writes);
        }
        if let Ok(disable_batching) = settings.get::<bool>("concurrency.disable_batching") {
            self = self.with_disable_batching(disable_batching);
        }
        if let Ok(max_batch_size) = settings.get::<Option<usize>>("concurrency.max_batch_size") {
            self = self.with_max_batch_size(max_batch_size);
        }
        if let Ok(disable_lists) = settings.get::<bool>("concurrency.disable_lists") {
            self = self.with_disable_lists(disable_lists);
        }
        if let Ok(max_page_size) = settings.get::<Option<usize>>("concurrency.max_page_size") {
            self = self.with_max_page_size(max_page_size);
        }

        if let Ok(max_query_depth) = settings.get::<Option<usize>>("schema.max_query_depth") {
            self = self.with_max_query_depth(max_query_depth);
        }
        if let Ok(max_query_complexity) =
            settings.get::<Option<usize>>("schema.max_query_complexity")
        {
            self = self.with_max_query_complexity(max_query_complexity);
        }
        if let Ok(max_recursive_depth) = settings.get::<Option<usize>>("schema.max_recursive_depth")
        {
            self = self.with_max_recursive_depth(max_recursive_depth);
        }
        if let Ok(max_directives_per_field) =
            settings.get::<Option<usize>>("schema.max_directives_per_field")
        {
            self = self.with_max_directives_per_field(max_directives_per_field);
        }
        if let Ok(disable_introspection) = settings.get::<bool>("schema.disable_introspection") {
            self = self.with_disable_introspection(disable_introspection);
        }

        if let Ok(public_dir) = settings.get::<Option<PathBuf>>("public_dir") {
            self = self.with_public_dir(public_dir);
        }

        #[cfg(feature = "search")]
        if let Ok(create_index) = settings.get::<bool>("index.create_index") {
            self = self.with_create_index(create_index);
        }
        Ok(self)
    }

    pub fn with_log_level(mut self, log_level: String) -> Self {
        self.logging.log_level = log_level;
        self
    }

    pub fn with_tracing(mut self, tracing: bool) -> Self {
        self.tracing.tracing_enabled = tracing;
        self
    }

    pub fn with_tracing_level(mut self, tracing_level: TracingLevel) -> Self {
        self.tracing.tracing_level = tracing_level;
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

    pub fn with_otlp_transport_protocol(mut self, otlp_protocol: TracingProtocol) -> Self {
        self.tracing.otlp_transport_protocol = otlp_protocol;
        self
    }

    pub fn with_otlp_transport_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.tracing.otlp_transport_headers = headers;
        self
    }

    pub fn with_cache_capacity(mut self, cache_capacity: u64) -> Self {
        self.cache.capacity = cache_capacity;
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

    pub fn with_require_auth_for_reads(mut self, require_auth_for_reads: bool) -> Self {
        self.auth.require_auth_for_reads = require_auth_for_reads;
        self
    }

    pub fn with_heavy_query_limit(mut self, heavy_query_limit: Option<usize>) -> Self {
        self.concurrency.heavy_query_limit = heavy_query_limit;
        self
    }

    pub fn with_exclusive_writes(mut self, exclusive_writes: bool) -> Self {
        self.concurrency.exclusive_writes = exclusive_writes;
        self
    }

    pub fn with_disable_batching(mut self, disable_batching: bool) -> Self {
        self.concurrency.disable_batching = disable_batching;
        self
    }

    pub fn with_max_batch_size(mut self, max_batch_size: Option<usize>) -> Self {
        self.concurrency.max_batch_size = max_batch_size;
        self
    }

    pub fn with_disable_lists(mut self, disable_lists: bool) -> Self {
        self.concurrency.disable_lists = disable_lists;
        self
    }

    pub fn with_max_page_size(mut self, max_page_size: Option<usize>) -> Self {
        self.concurrency.max_page_size = max_page_size;
        self
    }

    pub fn with_max_query_depth(mut self, max_query_depth: Option<usize>) -> Self {
        self.schema.max_query_depth = max_query_depth;
        self
    }

    pub fn with_max_query_complexity(mut self, max_query_complexity: Option<usize>) -> Self {
        self.schema.max_query_complexity = max_query_complexity;
        self
    }

    pub fn with_max_recursive_depth(mut self, max_recursive_depth: Option<usize>) -> Self {
        self.schema.max_recursive_depth = max_recursive_depth;
        self
    }

    pub fn with_max_directives_per_field(
        mut self,
        max_directives_per_field: Option<usize>,
    ) -> Self {
        self.schema.max_directives_per_field = max_directives_per_field;
        self
    }

    pub fn with_disable_introspection(mut self, disable_introspection: bool) -> Self {
        self.schema.disable_introspection = disable_introspection;
        self
    }

    pub fn with_public_dir(mut self, public_dir: Option<PathBuf>) -> Self {
        self.public_dir = public_dir;
        self
    }

    #[cfg(feature = "search")]
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
            concurrency: self.concurrency,
            schema: self.schema,
            public_dir: self.public_dir,
            #[cfg(feature = "search")]
            index: self.index,
        }
    }
}
