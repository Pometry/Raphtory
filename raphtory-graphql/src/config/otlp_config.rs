use clap::ValueEnum;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    runtime,
    trace::{Sampler, TracerProvider},
    Resource,
};
use serde::Deserialize;
use std::time::Duration;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter, EnumString};

pub const DEFAULT_TRACING_ENABLED: bool = false;

#[derive(
    Clone, Debug, Deserialize, PartialEq, serde::Serialize, EnumIter, EnumString, Display, ValueEnum,
)]
pub enum TracingLevel {
    COMPLETE,
    ESSENTIAL,
    MINIMAL,
}

impl TracingLevel {
    pub fn all_levels_string() -> String {
        TracingLevel::iter()
            .map(|lvl| lvl.to_string())
            .collect::<Vec<_>>()
            .join(", ")
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

pub const DEFAULT_OTLP_AGENT_HOST: &'static str = "http://localhost";
pub const DEFAULT_OTLP_AGENT_PORT: &'static str = "4317";
pub const DEFAULT_OTLP_TRACING_SERVICE_NAME: &'static str = "Raphtory";

#[derive(Clone, Deserialize, Debug, PartialEq, serde::Serialize)]
pub struct TracingConfig {
    pub tracing_enabled: bool,
    pub tracing_level: TracingLevel,
    pub otlp_agent_host: String,
    pub otlp_agent_port: String,
    pub otlp_tracing_service_name: String,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            tracing_enabled: DEFAULT_TRACING_ENABLED,
            tracing_level: DEFAULT_TRACING_LEVEL,
            otlp_agent_host: DEFAULT_OTLP_AGENT_HOST.to_owned(),
            otlp_agent_port: DEFAULT_OTLP_AGENT_PORT.to_owned(),
            otlp_tracing_service_name: DEFAULT_OTLP_TRACING_SERVICE_NAME.to_owned(),
        }
    }
}

impl TracingConfig {
    pub fn tracer_provider(&self) -> std::io::Result<Option<TracerProvider>> {
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

            match SpanExporter::builder()
                .with_tonic()
                .with_endpoint(format!(
                    "{}:{}",
                    self.otlp_agent_host.clone(),
                    self.otlp_agent_port.clone()
                ))
                .with_timeout(Duration::from_secs(3))
                .build()
            {
                Ok(exporter) => {
                    let tracer_provider = TracerProvider::builder()
                        .with_batch_exporter(exporter, runtime::Tokio)
                        .with_sampler(Sampler::AlwaysOn)
                        .with_resource(Resource::new(vec![KeyValue::new(
                            "service.name",
                            self.otlp_tracing_service_name.clone(),
                        )]))
                        .build();
                    println!(
                        // info!() here does not work since tracing is not enabled yet
                        "Sending traces to {}:{} with tracing level: `{}`",
                        self.otlp_agent_host.clone(),
                        self.otlp_agent_port.clone(),
                        self.tracing_level.clone()
                    );
                    Ok(Some(tracer_provider))
                }
                Err(e) => {
                    println!("{}", e.to_string()); // error!() here does not work since tracing is not enabled yet
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }
}
