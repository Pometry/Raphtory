use clap::{Args, ValueEnum};
use field_types::FieldName;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    trace::{Sampler, SdkTracerProvider},
    Resource,
};
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

pub const DEFAULT_OTLP_AGENT_HOST: &'static str = "http://localhost";
pub const DEFAULT_OTLP_AGENT_PORT: &'static str = "4317";
pub const DEFAULT_OTLP_TRACING_SERVICE_NAME: &'static str = "Raphtory";

#[derive(Clone, Deserialize, Debug, PartialEq, serde::Serialize, FieldName)]
pub struct TracingConfig {
    pub tracing_enabled: bool,
    pub tracing_level: TracingLevel,
    pub otlp_agent_host: String,
    pub otlp_agent_port: String,
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
            otlp_agent_host: DEFAULT_OTLP_AGENT_HOST.to_owned(),
            otlp_agent_port: DEFAULT_OTLP_AGENT_PORT.to_owned(),
            otlp_tracing_service_name: DEFAULT_OTLP_TRACING_SERVICE_NAME.to_owned(),
            otlp_transport_protocol: DEFAULT_OTLP_TRANSPORT_PROTOCOL,
            otlp_transport_headers: Default::default(),
        }
    }
}

impl TracingConfig {
    pub fn tracer_provider(&self) -> std::io::Result<Option<SdkTracerProvider>> {
        if self.tracing_enabled {
            if !self.otlp_agent_host.starts_with("http://")
                && !self.otlp_agent_host.starts_with("https://")
            {
                return Err(std::io::Error::other(
                    format!(
                        "otlp_agent_host needs to include the protocol, either http:// or https://, current value: {}",
                        self.otlp_agent_host
                    ),
                ));
            }

            let exporter = match self.otlp_transport_protocol {
                TracingProtocol::TONIC => SpanExporter::builder()
                    .with_tonic()
                    .with_endpoint(format!(
                        "{}:{}",
                        self.otlp_agent_host.clone(),
                        self.otlp_agent_port.clone()
                    ))
                    .with_timeout(Duration::from_secs(3))
                    .build(),
                TracingProtocol::HTTP => SpanExporter::builder()
                    .with_http()
                    .with_protocol(Protocol::HttpBinary)
                    .with_headers(self.otlp_transport_headers.clone())
                    .with_endpoint(format!(
                        "{}:{}",
                        self.otlp_agent_host.clone(),
                        self.otlp_agent_port.clone()
                    ))
                    .with_timeout(Duration::from_secs(3))
                    .build(),
            };

            match exporter {
                Ok(exporter) => {
                    let tracer_provider = SdkTracerProvider::builder()
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
                        .build();
                    eprintln!(
                        // info!() here does not work since tracing is not enabled yet
                        "Sending traces to {}:{} with tracing level: `{}`",
                        self.otlp_agent_host.clone(),
                        self.otlp_agent_port.clone(),
                        self.tracing_level.clone()
                    );
                    Ok(Some(tracer_provider))
                }
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
