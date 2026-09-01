use crate::{model::blocking_io, server::ServerError};
use clap::ValueEnum;
use config::ConfigError;
use field_types::FieldName;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{LogExporter, Protocol, SpanExporter, WithExportConfig, WithHttpConfig};
#[cfg(feature = "integration-test")]
use opentelemetry_sdk::{logs::InMemoryLogExporter, trace::InMemorySpanExporter};
use opentelemetry_sdk::{
    logs::SdkLoggerProvider,
    trace::{Sampler, SdkTracerProvider},
    Resource,
};
use raphtory_api::core::storage::arc_str::OptionAsStr;
use reqwest::{blocking::ClientBuilder, Certificate};
use serde::Deserialize;
use std::{collections::HashMap, fs::File, io::Read, path::PathBuf, time::Duration};
// Only the in-memory test exporters are lazily initialised.
#[cfg(feature = "integration-test")]
use std::sync::LazyLock;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter, EnumString, IntoStaticStr};

pub const DEFAULT_TRACING_ENABLED: bool = false;

#[derive(
    Clone,
    Debug,
    Deserialize,
    PartialEq,
    serde::Serialize,
    EnumIter,
    EnumString,
    Display,
    ValueEnum,
    IntoStaticStr,
)]
#[clap(rename_all = "UPPERCASE")]
#[serde(try_from = "String")]
#[serde(into = "&str")]
#[strum(ascii_case_insensitive)]
pub enum TracingLevel {
    COMPLETE,
    ESSENTIAL,
    MINIMAL,
}

impl TryFrom<String> for TracingLevel {
    type Error = strum::ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TracingLevel {
    pub fn all_levels_string() -> String {
        TracingLevel::iter()
            .map(|lvl| lvl.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    PartialEq,
    serde::Serialize,
    EnumIter,
    EnumString,
    Display,
    ValueEnum,
    IntoStaticStr,
)]
#[clap(rename_all = "UPPERCASE")]
#[serde(try_from = "String")]
#[serde(into = "&str")]
#[strum(ascii_case_insensitive)]
pub enum TracingProtocol {
    TONIC,
    HTTP,
    STDOUT,
    #[cfg(feature = "integration-test")]
    IN_MEMORY,
}

impl TryFrom<String> for TracingProtocol {
    type Error = strum::ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

pub const ESSENTIAL_TRACE_SPANS: [&str; 10] = [
    "addEdge",
    "addEdges",
    "deleteEdge",
    "graph",
    "updateGraph",
    "addNode",
    "node",
    "nodes",
    "edge",
    "edges",
];

pub const DEFAULT_TRACING_LEVEL: TracingLevel = TracingLevel::COMPLETE;

pub const DEFAULT_OTLP_TRANSPORT_PROTOCOL: TracingProtocol = TracingProtocol::TONIC;
pub const DEFAULT_OTLP_AGENT_PORT_TONIC: u16 = 4317;
pub const DEFAULT_OTLP_TRACING_SERVICE_NAME: &'static str = "Raphtory";

#[cfg(feature = "integration-test")]
// in-memory exporters to retrieve spans and logs in tests.
#[derive(Clone)]
pub struct GlobalExporters {
    pub span: InMemorySpanExporter,
    pub log: InMemoryLogExporter,
}

#[cfg(feature = "integration-test")]
/* GraphServer registers span and log exporters
   across the entire process, which can conflict
   when starting up servers with their own exporters.
   Making in-memory exporters global allows them to be
   initialized once and reused across multiple tests
   allowing the tests to retrieve spans and logs
   without conflicts.
*/
pub static GLOBAL_EXPORTERS: LazyLock<GlobalExporters> = LazyLock::new(|| GlobalExporters {
    span: InMemorySpanExporter::default(),
    log: InMemoryLogExporter::default(),
});

#[derive(Clone, Deserialize, Debug, PartialEq, serde::Serialize, FieldName)]
pub struct TracingConfig {
    pub enabled: bool,
    pub level: TracingLevel,
    pub agent_host: Option<String>,
    pub service_name: String,
    pub transport_protocol: TracingProtocol,
    /// Headers to use when transport_protocol is set to HTTP
    pub transport_headers: HashMap<String, String>,
    pub transport_certificate: Option<PathBuf>,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_TRACING_ENABLED,
            level: DEFAULT_TRACING_LEVEL,
            agent_host: None,
            service_name: DEFAULT_OTLP_TRACING_SERVICE_NAME.to_owned(),
            transport_protocol: DEFAULT_OTLP_TRANSPORT_PROTOCOL,
            transport_headers: Default::default(),
            transport_certificate: None,
        }
    }
}

impl TracingConfig {
    fn with_exporter<
        E: opentelemetry_sdk::trace::SpanExporter + 'static,
        L: opentelemetry_sdk::logs::LogExporter + 'static,
    >(
        &self,
        span_exporter: E,
        log_exporter: L,
    ) -> (SdkTracerProvider, SdkLoggerProvider) {
        let resource = Resource::builder()
            .with_attributes(vec![KeyValue::new(
                "service.name",
                self.service_name.clone(),
            )])
            .build();
        let tracer = SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter)
            .with_sampler(Sampler::AlwaysOn)
            .with_resource(resource.clone())
            .build();

        let logger = SdkLoggerProvider::builder()
            .with_batch_exporter(log_exporter)
            .with_resource(resource)
            .build();
        (tracer, logger)
    }

    #[cfg(feature = "integration-test")]
    fn with_simple_exporter<
        E: opentelemetry_sdk::trace::SpanExporter + 'static,
        L: opentelemetry_sdk::logs::LogExporter + 'static,
    >(
        &self,
        span_exporter: E,
        log_exporter: L,
    ) -> (SdkTracerProvider, SdkLoggerProvider) {
        let resource = Resource::builder()
            .with_attributes(vec![KeyValue::new(
                "service.name",
                self.service_name.clone(),
            )])
            .build();
        let tracer = SdkTracerProvider::builder()
            .with_simple_exporter(span_exporter)
            .with_sampler(Sampler::AlwaysOn)
            .with_resource(resource.clone())
            .build();

        let logger = SdkLoggerProvider::builder()
            .with_simple_exporter(log_exporter)
            .with_resource(resource)
            .build();
        (tracer, logger)
    }

    pub async fn tracer_provider(
        &self,
    ) -> Result<Option<(SdkTracerProvider, SdkLoggerProvider)>, ServerError> {
        if self.enabled {
            if let Some(agent_host) = self.agent_host.as_str() {
                if !agent_host.starts_with("http://") && !agent_host.starts_with("https://") {
                    let err = ServerError::ConfigError(ConfigError::Message(format!(
                        "otlp_agent_host needs to include the protocol, either http:// or https://, current value: {}",
                        agent_host)));
                    return Err(err);
                }
            }

            let providers = match self.transport_protocol {
                TracingProtocol::TONIC => {
                    let mut span_builder = SpanExporter::builder()
                        .with_tonic()
                        .with_timeout(Duration::from_secs(3));
                    let mut logger_builder = LogExporter::builder().with_tonic();
                    if let Some(agent_host) = self.agent_host.as_str() {
                        span_builder = span_builder.with_endpoint(agent_host);
                        logger_builder = logger_builder.with_endpoint(agent_host);
                    }

                    span_builder
                        .build()
                        .and_then(|span_exporter| {
                            logger_builder.build().map(|logger| (span_exporter, logger))
                        })
                        .map(|(span, log)| {
                            eprintln!(
                                // info!() here does not work since tracing is not enabled yet
                                "Sending traces to {} with protocol `TONIC` and tracing level `{}`",
                                self.agent_host.as_str().unwrap_or("default endpoint"),
                                self.level.clone()
                            );
                            self.with_exporter(span, log)
                        })
                }
                TracingProtocol::HTTP => {
                    let cert = self.transport_certificate.clone();

                    // needs to happen on blocking threadpool to avoid panic in initialisation
                    let client = blocking_io(move || {
                        let mut client_builder = ClientBuilder::new();
                        if let Some(cert) = cert {
                            let mut buf = Vec::new();
                            File::open(cert)?.read_to_end(&mut buf)?;
                            let cert = Certificate::from_pem(&buf)?;
                            client_builder = client_builder.add_root_certificate(cert);
                        }
                        let client = client_builder.build()?;
                        Ok::<_, ServerError>(client)
                    })
                    .await?;

                    let mut span_builder = SpanExporter::builder()
                        .with_http()
                        .with_protocol(Protocol::HttpBinary)
                        .with_headers(self.transport_headers.clone())
                        .with_http_client(client.clone())
                        .with_timeout(Duration::from_secs(3));
                    let mut logger_builder = LogExporter::builder()
                        .with_http()
                        .with_protocol(Protocol::HttpBinary)
                        .with_headers(self.transport_headers.clone())
                        .with_http_client(client)
                        .with_timeout(Duration::from_secs(3));

                    if let Some(agent_host) = self.agent_host.as_str() {
                        span_builder =
                            span_builder.with_endpoint(format!("{agent_host}/v1/traces"));
                        logger_builder =
                            logger_builder.with_endpoint(format!("{agent_host}/v1/logs"));
                    }
                    span_builder
                        .build()
                        .and_then(|span_exporter| {
                            logger_builder
                                .build()
                                .map(|log_exporter| (span_exporter, log_exporter))
                        })
                        .map(|(span, log)| {
                            eprintln!(
                                // info!() here does not work since tracing is not enabled yet
                                "Sending traces to {} with protocol `HTTP` and tracing level `{}`",
                                self.agent_host.as_str().unwrap_or("default endpoint"),
                                self.level.clone()
                            );
                            self.with_exporter(span, log)
                        })
                }
                TracingProtocol::STDOUT => {
                    eprintln!(
                        "Sending traces to stdout with tracing level `{}`",
                        self.level
                    );
                    Ok(self.with_exporter(
                        opentelemetry_stdout::SpanExporter::default(),
                        opentelemetry_stdout::LogExporter::default(),
                    ))
                }
                #[cfg(feature = "integration-test")]
                TracingProtocol::IN_MEMORY => {
                    eprintln!(
                        "Sending traces to in-memory exporter with tracing level `{}`",
                        self.level
                    );
                    Ok(self.with_simple_exporter(
                        GLOBAL_EXPORTERS.span.clone(),
                        GLOBAL_EXPORTERS.log.clone(),
                    ))
                }
            };

            match providers {
                Ok(providers) => Ok(Some(providers)),
                Err(e) => {
                    eprintln!("{}", e.to_string()); // error!() here does not work since tracing is not enabled yet
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }
}
