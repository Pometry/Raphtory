use opentelemetry::{
    global,
    global::BoxedTracer,
    trace::{TraceError, TracerProvider},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    trace,
    trace::{Sampler, Tracer},
    Resource,
};
use std::{env, time::Duration};

struct JaegerConfig {
    jaeger_agent_host: String,
    jaeger_agent_port: String,
    jaeger_tracing_service_name: String,
}

pub fn create_tracer_from_env() -> Option<Tracer> {
    let jaeger_enabled: bool = env::var("JAEGER_ENABLED")
        .unwrap_or_else(|_| "false".into())
        .parse()
        .unwrap();

    if jaeger_enabled {
        let config = get_jaeger_config_from_env();
        init_tracer(config).ok()
    } else {
        None
    }
}

pub fn get_tracer() -> Option<Tracer> {
    let jaeger_enabled: bool = env::var("JAEGER_ENABLED")
        .unwrap_or_else(|_| "false".into())
        .parse()
        .unwrap();
    if jaeger_enabled {
        let config = get_jaeger_config_from_env();
        init_tracer(config).ok()
    } else {
        None
    }
}

fn init_tracer(config: JaegerConfig) -> Result<Tracer, TraceError> {
    let tracer_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(format!(
                    "{}:{}",
                    config.jaeger_agent_host, config.jaeger_agent_port
                ))
                .with_timeout(Duration::from_secs(3)),
        )
        .with_trace_config(
            trace::Config::default()
                .with_sampler(Sampler::AlwaysOn)
                .with_resource(Resource::new(vec![KeyValue::new(
                    "service.name",
                    config.jaeger_tracing_service_name.clone(),
                )])),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;
    Ok(tracer_provider.tracer(config.jaeger_tracing_service_name.clone()))
    // global::set_tracer_provider(tracer_provider);
    // Ok(global::tracer(config.jaeger_tracing_service_name))
}

fn get_jaeger_config_from_env() -> JaegerConfig {
    JaegerConfig {
        jaeger_agent_host: env::var("JAEGER_AGENT_HOST")
            .unwrap_or_else(|_| "http://localhost".into()),
        jaeger_agent_port: env::var("JAEGER_AGENT_PORT").unwrap_or_else(|_| "4317".into()),
        jaeger_tracing_service_name: env::var("TRACING_SERVICE_NAME")
            .unwrap_or_else(|_| "Raphtory".into()),
    }
}
