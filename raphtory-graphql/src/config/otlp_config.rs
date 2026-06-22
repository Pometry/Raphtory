use clap::{Args, ValueEnum};
use field_types::FieldName;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    trace::{Sampler, SdkTracerProvider},
    Resource,
};
use raphtory_api::core::storage::arc_str::OptionAsStr;
use serde::Deserialize;
use std::{collections::HashMap, env, time::Duration};
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

#[derive(Clone, Deserialize, Debug, PartialEq, serde::Serialize, FieldName)]
pub struct TracingConfig {
    pub tracing_enabled: bool,
    pub tracing_level: TracingLevel,
    pub otlp_agent_host: Option<String>,
    pub otlp_tracing_service_name: String,
    pub otlp_transport_protocol: TracingProtocol,
    /// Headers to use when transport_protocol is set to HTTP
    pub otlp_transport_headers: HashMap<String, String>,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            tracing_enabled: DEFAULT_TRACING_ENABLED,
            tracing_level: DEFAULT_TRACING_LEVEL,
            otlp_agent_host: None,
            otlp_tracing_service_name: DEFAULT_OTLP_TRACING_SERVICE_NAME.to_owned(),
            otlp_transport_protocol: DEFAULT_OTLP_TRANSPORT_PROTOCOL,
            otlp_transport_headers: Default::default(),
        }
    }
}

impl TracingConfig {
    fn with_exporter<E: opentelemetry_sdk::trace::SpanExporter + 'static>(
        &self,
        exporter: E,
    ) -> SdkTracerProvider {
        SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_sampler(Sampler::AlwaysOn)
            .with_resource(
                Resource::builder()
                    .with_attributes(vec![KeyValue::new(
                        "service.name",
                        self.otlp_tracing_service_name.clone(),
                    )])
                    .build(),
            )
            .build()
    }

    pub fn tracer_provider(&self) -> std::io::Result<Option<SdkTracerProvider>> {
        if self.tracing_enabled {
            if let Some(agent_host) = self.otlp_agent_host.as_str() {
                if !agent_host.starts_with("http://") && !agent_host.starts_with("https://") {
                    return Err(std::io::Error::other(
                        format!(
                            "otlp_agent_host needs to include the protocol, either http:// or https://, current value: {}",
                            agent_host
                        ),
                    ));
                }
            }

            let tracer_provider = match self.otlp_transport_protocol {
                TracingProtocol::TONIC => {
                    let mut builder = SpanExporter::builder()
                        .with_tonic()
                        .with_timeout(Duration::from_secs(3));
                    if let Some(agent_host) = self.otlp_agent_host.as_str() {
                        builder = builder.with_endpoint(agent_host);
                    }
                    builder.build().map(|exporter| {
                        eprintln!(
                            // info!() here does not work since tracing is not enabled yet
                            "Sending traces to {} with protocol `TONIC` and tracing level `{}`",
                            self.otlp_agent_host.as_str().unwrap_or("default endpoint"),
                            self.tracing_level.clone()
                        );
                        self.with_exporter(exporter)
                    })
                }
                TracingProtocol::HTTP => {
                    let mut builder = SpanExporter::builder()
                        .with_http()
                        .with_protocol(Protocol::HttpBinary)
                        .with_headers(self.otlp_transport_headers.clone())
                        .with_timeout(Duration::from_secs(3));
                    if let Some(agent_host) = self.otlp_agent_host.as_str() {
                        builder = builder.with_endpoint(format!("{agent_host}/v1/traces"));
                    }
                    builder
                        .build()
                        .map(|exporter| {
                            match self.otlp_agent_host.as_str() {
                                Some(host) => {
                                    eprintln!(
                                        // info!() here does not work since tracing is not enabled yet
                                        "Sending traces to {host}/v1/traces with protocol `HTTP` and tracing level `{}`",
                                        self.tracing_level.clone()
                                    );
                                }
                                None =>  {
                                    eprintln!(
                                        // info!() here does not work since tracing is not enabled yet
                                        "Sending traces to default endpoint with protocol `HTTP` and tracing level `{}`",
                                        self.tracing_level.clone()
                                    );
                                }
                            }
                            self.with_exporter(exporter)
                        })
                }
                TracingProtocol::STDOUT => {
                    eprintln!(
                        "Sending traces to stdout with tracing level `{}`",
                        self.tracing_level
                    );
                    Ok(self.with_exporter(opentelemetry_stdout::SpanExporter::default()))
                }
            };

            match tracer_provider {
                Ok(tracer_provider) => Ok(Some(tracer_provider)),
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
