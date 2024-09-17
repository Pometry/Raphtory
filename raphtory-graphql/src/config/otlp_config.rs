use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    trace,
    trace::{Sampler, TracerProvider},
    Resource,
};
use serde::Deserialize;
use std::time::Duration;
use tracing::{error, info};

#[derive(Clone, Deserialize, Debug, PartialEq)]
pub struct TracingConfig {
    pub tracing_enabled: bool,
    pub otlp_agent_host: String,
    pub otlp_agent_port: String,
    pub otlp_tracing_service_name: String,
}
impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            tracing_enabled: false,
            otlp_agent_host: "http://localhost".to_string(),
            otlp_agent_port: "4317".to_string(),
            otlp_tracing_service_name: "Raphtory".to_string(),
        }
    }
}
impl TracingConfig {
    pub fn tracer_provider(&self) -> Option<TracerProvider> {
        if self.tracing_enabled {
            match opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint(format!(
                            "{}:{}",
                            self.otlp_agent_host.clone(),
                            self.otlp_agent_port.clone()
                        ))
                        .with_timeout(Duration::from_secs(3)),
                )
                .with_trace_config(
                    trace::Config::default()
                        .with_sampler(Sampler::AlwaysOn)
                        .with_resource(Resource::new(vec![KeyValue::new(
                            "service.name",
                            self.otlp_tracing_service_name.clone(),
                        )])),
                )
                .install_batch(opentelemetry_sdk::runtime::Tokio)
            {
                Ok(tracer_provider) => {
                    println!(
                        "Sending traces to {}:{}",
                        self.otlp_agent_host.clone(),
                        self.otlp_agent_port.clone()
                    );
                    Some(tracer_provider)
                    // Some(tracer_provider.tracer(self.otlp_tracing_service_name.clone()))
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
