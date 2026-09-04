use super::auth_config::{AuthConfig, AuthConfigFieldName, PublicKeyError};
use crate::{
    cli::ConfigArgs,
    config::{
        cache_config::{CacheConfig, CacheConfigFieldName},
        concurrency_config::{ConcurrencyConfig, ConcurrencyConfigFieldName},
        log_config::{LoggingConfig, LoggingConfigFieldName},
        otlp_config::{TracingConfig, TracingConfigFieldName, TracingLevel, TracingProtocol},
        parquet_config::{ParquetConfig, ParquetConfigFieldName},
        schema_config::{SchemaConfig, SchemaConfigFieldName},
    },
    plugin::server::extension::{ArgExtensions, BoxedExtension, ServerExtensionImpl},
    server::ServerError,
};
use config::{Config, ConfigError, File};
use field_types::FieldName;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    error::Error,
    fmt::Display,
    io,
    path::{Path, PathBuf},
};

#[derive(Debug, Default, Deserialize, PartialEq, Clone, Serialize, FieldName)]
pub struct AppConfig {
    pub logging: LoggingConfig,
    pub cache: CacheConfig,
    pub tracing: TracingConfig,
    pub auth: AuthConfig,
    pub concurrency: ConcurrencyConfig,
    pub schema: SchemaConfig,
    pub parquet: ParquetConfig,
    pub public_dir: Option<PathBuf>,
    #[serde(flatten)]
    pub extensions: ArgExtensions,
}

impl AppConfig {
    /// The config with each extension replaced by its full schema (every field, not only the set ones).
    pub fn config_schema_json(&self) -> Result<serde_json::Value, ServerError> {
        let mut value = serde_json::to_value(self).map_err(ServerError::config_error)?;
        if let serde_json::Value::Object(map) = &mut value {
            for ext in self.extensions.iter() {
                map.insert(ext.name().to_string(), ext.config_schema()?);
            }
        }
        Ok(value)
    }
}

pub struct AppConfigBuilder {
    config: AppConfig,
}

impl From<AppConfig> for AppConfigBuilder {
    fn from(config: AppConfig) -> Self {
        Self { config }
    }
}

impl Default for AppConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn invalid_path(path: impl IntoIterator<Item: Display>) -> ServerError {
    let path = path.into_iter().join(".");
    ServerError::ConfigError(ConfigError::Message(format!(
        "Invalid configuration field '{path}'"
    )))
}

fn invalid_value(path: impl IntoIterator<Item: Display>, err: impl Error) -> ServerError {
    let path = path.into_iter().join(".");
    ServerError::ConfigError(ConfigError::Message(format!(
        "Invalid configuration value for field '{path}': {err}"
    )))
}

fn as_boxed_external<E: Error + Send + Sync + 'static>(error: E) -> ServerError {
    ServerError::ConfigError(ConfigError::Foreign(Box::new(error)))
}

impl AppConfigBuilder {
    pub fn new() -> Self {
        AppConfig::default().into()
    }

    pub fn new_from_args(server_args: ConfigArgs) -> Result<Self, ServerError> {
        let mut builder = Self::new();
        // initialise extensions with parsed command line arguments
        builder.config.extensions = server_args.extensions;

        if let Some(config_file) = server_args.config_file.clone() {
            builder.load_from_path(config_file)?;
        };
        if let Some(cache_capacity) = server_args.cache_capacity {
            builder.with_cache_capacity(cache_capacity);
        }
        if let Some(log_level) = server_args.log_level.clone() {
            builder.with_log_level(log_level);
        }
        if let Some(tracing) = server_args.tracing {
            builder.with_tracing(tracing);
        }
        if let Some(tracing_level) = server_args.tracing_level.clone() {
            builder.with_tracing_level(tracing_level);
        }
        if let Some(otlp_agent_host) = server_args.otlp_agent_host.clone() {
            builder.with_otlp_agent_host(Some(otlp_agent_host));
        }
        if let Some(otlp_tracing_service_name) = server_args.otlp_tracing_service_name.clone() {
            builder.with_otlp_tracing_service_name(otlp_tracing_service_name);
        }
        if let Some(otlp_transport_protocol) = server_args.otlp_transport_protocol.clone() {
            builder.with_otlp_transport_protocol(otlp_transport_protocol);
        }
        if let Some(otlp_transport_headers) = server_args.otlp_transport_headers.clone() {
            builder.with_otlp_transport_headers(otlp_transport_headers);
        }
        if let Some(otlp_transport_certificate) = server_args.otlp_transport_certificate.clone() {
            builder.with_otlp_transport_certificate(Some(otlp_transport_certificate));
        }
        if let Some(auth_public_key) = server_args.auth_public_key.clone() {
            builder
                .with_auth_public_key(Some(auth_public_key))
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        }
        if let Some(public_dir) = server_args.public_dir.clone() {
            builder.with_public_dir(Some(public_dir));
        }
        if let Some(require_auth_for_reads) = server_args.require_auth_for_reads {
            builder.with_require_auth_for_reads(require_auth_for_reads);
        }
        if let Some(heavy_query_limit) = server_args.heavy_query_limit {
            builder.with_heavy_query_limit(Some(heavy_query_limit));
        }
        if let Some(exclusive_writes) = server_args.exclusive_writes {
            builder.with_exclusive_writes(exclusive_writes);
        }
        if let Some(disable_batching) = server_args.disable_batching {
            builder.with_disable_batching(disable_batching);
        }
        if let Some(max_batch_size) = server_args.max_batch_size {
            builder.with_max_batch_size(Some(max_batch_size));
        }
        if let Some(disable_lists) = server_args.disable_lists {
            builder.with_disable_lists(disable_lists);
        }
        if let Some(max_page_size) = server_args.max_page_size {
            builder.with_max_page_size(Some(max_page_size));
        }
        if let Some(max_query_depth) = server_args.max_query_depth {
            builder.with_max_query_depth(Some(max_query_depth));
        }
        if let Some(max_query_complexity) = server_args.max_query_complexity {
            builder.with_max_query_complexity(Some(max_query_complexity));
        }
        if let Some(max_recursive_depth) = server_args.max_recursive_depth {
            builder.with_max_recursive_depth(Some(max_recursive_depth));
        }
        if let Some(max_directives_per_field) = server_args.max_directives_per_field {
            builder.with_max_directives_per_field(Some(max_directives_per_field));
        }
        if let Some(disable_introspection) = server_args.disable_introspection {
            builder.with_disable_introspection(disable_introspection);
        }
        Ok(builder)
    }

    pub fn update_from_json(&mut self, value: serde_json::Value) -> Result<&mut Self, ServerError> {
        let map = value
            .as_object()
            .ok_or_else(|| ConfigError::Message(format!("Invalid config: {value}")))?;

        for (path, value) in map {
            // A key naming no built-in section names a server extension, whose settings sit at
            // the top level alongside the built-ins. An unregistered name still errors, exactly as
            // an unknown section did.
            //
            // `extensions` is deliberately not treated as a section here: the field is
            // `#[serde(flatten)]`ed, so a config file has no such key either, and routing it
            // through the same lookup keeps this path and the file path in agreement.
            let field = AppConfigFieldName::by_name(path)
                .filter(|f| !matches!(f, AppConfigFieldName::Extensions));
            let Some(field) = field else {
                // A name that is neither a section nor a registered extension is simply an
                // invalid field. Only once it is known to be an extension do we hand the value
                // over and let its own error through — that error names the offending inner
                // field, which reporting the section name here would hide.
                if !crate::plugin::server::is_registered(path) {
                    return Err(invalid_path([path]));
                }
                let mut one = serde_json::Map::new();
                one.insert(path.clone(), value.clone());
                self.config
                    .extensions
                    .update_from_json(&serde_json::Value::Object(one))?;
                continue;
            };
            match field {
                AppConfigFieldName::Logging => {
                    let map = value.as_object().ok_or_else(|| {
                        ConfigError::Message(format!("Invalid logging config: {value}"))
                    })?;
                    for (sub_path, value) in map {
                        match LoggingConfigFieldName::by_name(sub_path)
                            .ok_or_else(|| invalid_path([path, sub_path]))?
                        {
                            LoggingConfigFieldName::LogLevel => {
                                self.with_log_level(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                        }
                    }
                }
                AppConfigFieldName::Cache => {
                    let map = value.as_object().ok_or_else(|| {
                        ConfigError::Message(format!("Invalid cache config: {value}"))
                    })?;
                    for (sub_path, value) in map {
                        match CacheConfigFieldName::by_name(sub_path)
                            .ok_or_else(|| invalid_path([path, sub_path]))?
                        {
                            CacheConfigFieldName::Capacity => {
                                self.with_cache_capacity(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            CacheConfigFieldName::ReadOnly => {
                                self.with_cache_read_only(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                        }
                    }
                }
                AppConfigFieldName::Tracing => {
                    let map = value.as_object().ok_or_else(|| {
                        ConfigError::Message(format!("Invalid tracing config: {value}"))
                    })?;
                    for (sub_path, value) in map {
                        match TracingConfigFieldName::by_name(sub_path)
                            .ok_or_else(|| invalid_path([path, sub_path]))?
                        {
                            TracingConfigFieldName::Enabled => {
                                self.with_tracing(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            TracingConfigFieldName::Level => {
                                self.with_tracing_level(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            TracingConfigFieldName::AgentHost => {
                                self.with_otlp_agent_host(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            TracingConfigFieldName::ServiceName => {
                                self.with_otlp_tracing_service_name(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            TracingConfigFieldName::TransportProtocol => {
                                self.with_otlp_transport_protocol(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            TracingConfigFieldName::TransportHeaders => {
                                self.with_otlp_transport_headers(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            TracingConfigFieldName::TransportCertificate => {
                                self.with_otlp_transport_certificate(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                        }
                    }
                }
                AppConfigFieldName::Auth => {
                    let map = value.as_object().ok_or_else(|| {
                        ConfigError::Message(format!("Invalid auth config: {value}"))
                    })?;
                    for (sub_path, value) in map {
                        match AuthConfigFieldName::by_name(sub_path)
                            .ok_or_else(|| invalid_path([path, sub_path]))?
                        {
                            AuthConfigFieldName::PublicKey => {
                                self.with_auth_public_key(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                )?;
                            }
                            AuthConfigFieldName::RequireAuthForReads => {
                                self.with_require_auth_for_reads(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            AuthConfigFieldName::Audience => {
                                self.with_auth_audience(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            AuthConfigFieldName::Issuer => {
                                self.with_auth_issuer(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            AuthConfigFieldName::RoleClaim => {
                                self.with_auth_role_claim(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                        }
                    }
                }
                AppConfigFieldName::Concurrency => {
                    let map = value.as_object().ok_or_else(|| {
                        ConfigError::Message(format!("Invalid concurrency config: {value}"))
                    })?;
                    for (sub_path, value) in map {
                        match ConcurrencyConfigFieldName::by_name(sub_path)
                            .ok_or_else(|| invalid_path([path, sub_path]))?
                        {
                            ConcurrencyConfigFieldName::HeavyQueryLimit => {
                                self.with_heavy_query_limit(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            ConcurrencyConfigFieldName::ExclusiveWrites => {
                                self.with_exclusive_writes(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            ConcurrencyConfigFieldName::DisableBatching => {
                                self.with_disable_batching(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            ConcurrencyConfigFieldName::MaxBatchSize => {
                                self.with_max_batch_size(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            ConcurrencyConfigFieldName::DisableLists => {
                                self.with_disable_lists(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            ConcurrencyConfigFieldName::MaxConcurrentLoads => {
                                self.with_max_concurrent_loads(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            ConcurrencyConfigFieldName::MaxPageSize => {
                                self.with_max_page_size(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                        }
                    }
                }
                AppConfigFieldName::Schema => {
                    let map = value.as_object().ok_or_else(|| {
                        ConfigError::Message(format!("Invalid schema config: {value}"))
                    })?;
                    for (sub_path, value) in map {
                        match SchemaConfigFieldName::by_name(sub_path)
                            .ok_or_else(|| invalid_path([path, sub_path]))?
                        {
                            SchemaConfigFieldName::MaxQueryDepth => {
                                self.with_max_query_depth(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            SchemaConfigFieldName::MaxQueryComplexity => {
                                self.with_max_query_complexity(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            SchemaConfigFieldName::MaxRecursiveDepth => {
                                self.with_max_recursive_depth(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            SchemaConfigFieldName::MaxDirectivesPerField => {
                                self.with_max_directives_per_field(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            SchemaConfigFieldName::DisableIntrospection => {
                                self.with_disable_introspection(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                            SchemaConfigFieldName::DisableUi => {
                                self.with_disable_ui(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                        }
                    }
                }
                AppConfigFieldName::Parquet => {
                    let map = value.as_object().ok_or_else(|| {
                        ConfigError::Message(format!("Invalid parquet config: {value}"))
                    })?;
                    for (sub_path, value) in map {
                        match ParquetConfigFieldName::by_name(sub_path)
                            .ok_or_else(|| invalid_path([path, sub_path]))?
                        {
                            ParquetConfigFieldName::AllowedPaths => {
                                self.with_allowed_parquet_paths(
                                    Deserialize::deserialize(value)
                                        .map_err(|e| invalid_value([path, sub_path], e))?,
                                );
                            }
                        }
                    }
                }
                AppConfigFieldName::PublicDir => {
                    self.with_public_dir(
                        Deserialize::deserialize(value).map_err(|e| invalid_value([path], e))?,
                    );
                }
                AppConfigFieldName::Extensions => unreachable!("filtered out above"),
            }
        }

        Ok(self)
    }

    pub fn load_from_path(
        &mut self,
        config_path: impl AsRef<Path>,
    ) -> Result<&mut Self, ServerError> {
        let settings = Config::builder()
            .add_source(File::from(config_path.as_ref()))
            .build()?;
        let value = serde_json::Value::deserialize(settings)?;
        self.update_from_json(value)
    }

    pub fn with_log_level(&mut self, log_level: String) -> &mut Self {
        self.config.logging.log_level = log_level;
        self
    }

    pub fn with_tracing(&mut self, tracing: bool) -> &mut Self {
        self.config.tracing.enabled = tracing;
        self
    }

    pub fn with_tracing_level(&mut self, tracing_level: TracingLevel) -> &mut Self {
        self.config.tracing.level = tracing_level;
        self
    }

    pub fn with_otlp_agent_host(&mut self, otlp_agent_host: Option<String>) -> &mut Self {
        self.config.tracing.agent_host = otlp_agent_host;
        self
    }

    pub fn with_otlp_tracing_service_name(
        &mut self,
        otlp_tracing_service_name: String,
    ) -> &mut Self {
        self.config.tracing.service_name = otlp_tracing_service_name;
        self
    }

    pub fn with_otlp_transport_protocol(&mut self, otlp_protocol: TracingProtocol) -> &mut Self {
        self.config.tracing.transport_protocol = otlp_protocol;
        self
    }

    pub fn with_otlp_transport_headers(&mut self, headers: HashMap<String, String>) -> &mut Self {
        self.config.tracing.transport_headers = headers;
        self
    }

    pub fn with_otlp_transport_certificate(&mut self, certificte: Option<PathBuf>) -> &mut Self {
        self.config.tracing.transport_certificate = certificte;
        self
    }

    pub fn with_cache_capacity(&mut self, cache_capacity: u64) -> &mut Self {
        self.config.cache.capacity = cache_capacity;
        self
    }

    pub fn with_cache_read_only(&mut self, read_only: bool) -> &mut Self {
        self.config.cache.read_only = read_only;
        self
    }

    pub fn with_auth_public_key(
        &mut self,
        public_key: Option<String>,
    ) -> Result<&mut Self, PublicKeyError> {
        if let Some(public_key) = public_key {
            self.config.auth.public_key = Some(public_key.try_into()?);
        }
        Ok(self)
    }

    pub fn with_require_auth_for_reads(&mut self, require_auth_for_reads: bool) -> &mut Self {
        self.config.auth.require_auth_for_reads = require_auth_for_reads;
        self
    }

    pub fn with_auth_audience(&mut self, audience: Option<String>) -> &mut Self {
        self.config.auth.audience = audience;
        self
    }

    pub fn with_auth_issuer(&mut self, issuer: Option<String>) -> &mut Self {
        self.config.auth.issuer = issuer;
        self
    }

    pub fn with_auth_role_claim(&mut self, role_claim: Option<String>) -> &mut Self {
        self.config.auth.role_claim = role_claim;
        self
    }

    pub fn with_heavy_query_limit(&mut self, heavy_query_limit: Option<usize>) -> &mut Self {
        self.config.concurrency.heavy_query_limit = heavy_query_limit;
        self
    }

    pub fn with_exclusive_writes(&mut self, exclusive_writes: bool) -> &mut Self {
        self.config.concurrency.exclusive_writes = exclusive_writes;
        self
    }

    pub fn with_disable_batching(&mut self, disable_batching: bool) -> &mut Self {
        self.config.concurrency.disable_batching = disable_batching;
        self
    }

    pub fn with_max_batch_size(&mut self, max_batch_size: Option<usize>) -> &mut Self {
        self.config.concurrency.max_batch_size = max_batch_size;
        self
    }

    pub fn with_disable_lists(&mut self, disable_lists: bool) -> &mut Self {
        self.config.concurrency.disable_lists = disable_lists;
        self
    }

    pub fn with_max_page_size(&mut self, max_page_size: Option<usize>) -> &mut Self {
        self.config.concurrency.max_page_size = max_page_size;
        self
    }

    pub fn with_max_concurrent_loads(&mut self, max_concurrent_loads: Option<usize>) -> &mut Self {
        self.config.concurrency.max_concurrent_loads = max_concurrent_loads;
        self
    }

    pub fn with_max_query_depth(&mut self, max_query_depth: Option<usize>) -> &mut Self {
        self.config.schema.max_query_depth = max_query_depth;
        self
    }

    pub fn with_max_query_complexity(&mut self, max_query_complexity: Option<usize>) -> &mut Self {
        self.config.schema.max_query_complexity = max_query_complexity;
        self
    }

    pub fn with_max_recursive_depth(&mut self, max_recursive_depth: Option<usize>) -> &mut Self {
        self.config.schema.max_recursive_depth = max_recursive_depth;
        self
    }

    pub fn with_max_directives_per_field(
        &mut self,
        max_directives_per_field: Option<usize>,
    ) -> &mut Self {
        self.config.schema.max_directives_per_field = max_directives_per_field;
        self
    }

    pub fn with_disable_introspection(&mut self, disable_introspection: bool) -> &mut Self {
        self.config.schema.disable_introspection = disable_introspection;
        self
    }

    pub fn with_disable_ui(&mut self, disable_ui: bool) -> &mut Self {
        self.config.schema.disable_ui = disable_ui;
        self
    }

    pub fn with_allowed_parquet_paths(&mut self, allowed_paths: Vec<PathBuf>) -> &mut Self {
        self.config.parquet.allowed_paths = allowed_paths;
        self
    }

    pub fn with_public_dir(&mut self, public_dir: Option<PathBuf>) -> &mut Self {
        self.config.public_dir = public_dir;
        self
    }

    pub fn with_extension(&mut self, extension: impl ServerExtensionImpl) -> &mut Self {
        self.with_boxed_extension(Box::new(extension))
    }

    pub fn with_boxed_extension(&mut self, extension: BoxedExtension) -> &mut Self {
        self.config.extensions.push_boxed(extension);
        self
    }

    pub fn build(&mut self) -> AppConfig {
        self.config.clone()
    }
}
