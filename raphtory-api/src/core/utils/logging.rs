use std::sync::OnceLock;
use tracing_subscriber::{
    fmt, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry,
};

pub fn get_log_env(log_level: String) -> EnvFilter {
    EnvFilter::new(format!(
        "pometry-storage-private={},pometry-storage-private={},raphtory={},raphtory-api={},raphtory-benchmark={},raphtory-cypher={},raphtory-graphql={}",
        log_level, log_level, log_level, log_level, log_level, log_level, log_level
    ))
}

pub fn init_global_logger(log_level: String) {
    static INIT: OnceLock<()> = OnceLock::new();

    INIT.get_or_init(|| {
        let filter = get_log_env(log_level);
        let registry = Registry::default()
            .with(filter)
            .with(fmt::layer().pretty().with_span_events(FmtSpan::NONE));
        registry.try_init().ok();
    });
}

pub fn global_error_logger() {
    init_global_logger("ERROR".to_string())
}
pub fn global_warn_logger() {
    init_global_logger("WARN".to_string())
}

pub fn global_info_logger() {
    init_global_logger("INFO".to_string())
}

pub fn global_debug_logger() {
    init_global_logger("DEBUG".to_string())
}
pub fn global_trace_logger() {
    init_global_logger("TRACE".to_string())
}

pub fn sysout_debug() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .init();
}
