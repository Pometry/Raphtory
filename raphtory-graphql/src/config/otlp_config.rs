use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    runtime,
    trace::{Sampler, TracerProvider},
    Resource,
};
use serde::Deserialize;
use std::time::Duration;
use tracing::{error, info};

pub const DEFAULT_TRACING_ENABLED: bool = false;
pub const DEFAULT_OTLP_AGENT_HOST: &'static str = "http://localhost";
pub const DEFAULT_OTLP_AGENT_PORT: &'static str = "4317";
pub const DEFAULT_OTLP_TRACING_SERVICE_NAME: &'static str = "Raphtory";

#[derive(Clone, Deserialize, Debug, serde::Serialize)]
pub struct TracingConfig {
    pub tracing_enabled: bool,
    pub otlp_agent_host: String,
    pub otlp_agent_port: String,
    pub otlp_tracing_service_name: String,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            tracing_enabled: DEFAULT_TRACING_ENABLED,
            otlp_agent_host: DEFAULT_OTLP_AGENT_HOST.to_owned(),
            otlp_agent_port: DEFAULT_OTLP_AGENT_PORT.to_owned(),
            otlp_tracing_service_name: DEFAULT_OTLP_TRACING_SERVICE_NAME.to_owned(),
        }
    }
}

impl TracingConfig {
    pub fn tracer_provider(&self) -> Option<TracerProvider> {
        if self.tracing_enabled {
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
                    info!(
                        "Sending traces to {}:{}",
                        self.otlp_agent_host.clone(),
                        self.otlp_agent_port.clone()
                    );
                    Some(tracer_provider)
                }
                Err(e) => {
                    error!("{}", e.to_string());
                    None
                }
            }
        } else {
            None
        }
    }
}
