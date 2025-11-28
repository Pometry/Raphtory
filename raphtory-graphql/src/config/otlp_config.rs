use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    runtime,
    trace::{Sampler, TracerProvider},
    Resource,
};
use serde::Deserialize;
use std::time::Duration;

pub const DEFAULT_TRACING_ENABLED: bool = false;

pub const TRACE_LEVELS: [&str; 2] = ["Complete", "Essential"];

pub const DEFAULT_TRACING_LEVEL: &'static str = "Complete";

pub const DEFAULT_OTLP_AGENT_HOST: &'static str = "http://localhost";
pub const DEFAULT_OTLP_AGENT_PORT: &'static str = "4317";
pub const DEFAULT_OTLP_TRACING_SERVICE_NAME: &'static str = "Raphtory";

#[derive(Clone, Deserialize, Debug, PartialEq, serde::Serialize)]
pub struct TracingConfig {
    pub tracing_enabled: bool,
    pub tracing_level: String,
    pub otlp_agent_host: String,
    pub otlp_agent_port: String,
    pub otlp_tracing_service_name: String,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            tracing_enabled: DEFAULT_TRACING_ENABLED,
            tracing_level: DEFAULT_TRACING_LEVEL.to_string(),
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

            if !TRACE_LEVELS.contains(&self.tracing_level.as_str()) {
                let allowed = TRACE_LEVELS.join(", ");
                return Err(std::io::Error::other(format!(
                    "Trace level '{}' is invalid. Allowed values: {}",
                    self.tracing_level, allowed
                )));
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
