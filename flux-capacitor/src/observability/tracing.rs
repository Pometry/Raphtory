use opentelemetry::sdk::trace::{self, Sampler};
use opentelemetry::{
    global, runtime::Tokio, sdk::propagation::TraceContextPropagator, sdk::trace::Tracer,
};
use std::env;

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
        Some(init_tracer(config))
    } else {
        None
    }
}

fn init_tracer(config: JaegerConfig) -> Tracer {
    global::set_text_map_propagator(TraceContextPropagator::new());
    opentelemetry_jaeger::new_agent_pipeline()
        .with_endpoint(format!(
            "{}:{}",
            config.jaeger_agent_host, config.jaeger_agent_port
        ))
        .with_auto_split_batch(true)
        .with_service_name(config.jaeger_tracing_service_name)
        .with_trace_config(trace::config().with_sampler(Sampler::AlwaysOn))
        .install_batch(Tokio)
        .expect("pipeline install error")
}

fn get_jaeger_config_from_env() -> JaegerConfig {
    JaegerConfig {
        jaeger_agent_host: env::var("JAEGER_AGENT_HOST").unwrap_or_else(|_| "localhost".into()),
        jaeger_agent_port: env::var("JAEGER_AGENT_PORT").unwrap_or_else(|_| "6831".into()),
        jaeger_tracing_service_name: env::var("TRACING_SERVICE_NAME")
            .unwrap_or_else(|_| "axum-graphql".into()),
    }
}
