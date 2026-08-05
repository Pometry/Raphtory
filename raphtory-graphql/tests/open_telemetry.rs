// OpenTelemetry Tests must be separated into their own binary to prevent polluting other tests since the span and log exporters are set globally.
use raphtory::{
    db::api::storage::storage::Args,
    prelude::{Graph, StableEncode},
};
use raphtory_graphql::{
    client::raphtory_client::RaphtoryGraphQLClient,
    config::{
        app_config::AppConfigBuilder,
        otlp_config::{TracingLevel, TracingProtocol, GLOBAL_EXPORTERS},
    },
    server::{GraphServer, RunningGraphServer},
};
use std::collections::{HashMap, HashSet};
use tempfile::{tempdir, TempDir};
use url::Url;

use opentelemetry_sdk::{logs::InMemoryLogExporter, trace::InMemorySpanExporter};

const OPEN_TELEMETRY_QUERY: &str = "query {
	updateGraph(path: \"g\") {
		addNode(time: 1, name: 1, properties: [{ key: \"seed\", value: { str: \"yes\" } }], nodeType: \"seed\", layer: \"main\") {
			success
			node { id }
		}
		addEdge(time: 5, src: 1, dst: 2, properties: [{ key: \"weight\", value: { f64: 1.5 } }], layer: \"main\") {
			success
		}
		graph {
			countNodes
			hasNode(name: 1)
		}
		flush
	}
}";

async fn setup_for_span_tests(
    tracing_level: TracingLevel,
) -> (
    RaphtoryGraphQLClient,
    RunningGraphServer,
    InMemorySpanExporter,
    InMemoryLogExporter,
    TempDir,
) {
    let span_exporter = GLOBAL_EXPORTERS.span.clone();
    let log_exporter = GLOBAL_EXPORTERS.log.clone();
    // reset logs and spans for next test
    span_exporter.reset();
    log_exporter.reset();
    let tmp_dir = tempdir().unwrap();
    let graph = Graph::new();
    graph.encode(tmp_dir.path().join("g")).unwrap();

    let app_config = AppConfigBuilder::new()
        .with_tracing(true)
        .with_tracing_level(tracing_level)
        .with_otlp_transport_protocol(TracingProtocol::IN_MEMORY)
        .build();

    let server = GraphServer::new(
        tmp_dir.path().to_path_buf(),
        Some(app_config.clone()),
        Args::default(),
    )
    .await
    .unwrap();
    let handler = server.start_with_port(0).await.unwrap();

    let endpoint = Url::parse(&format!("http://localhost:{}/", handler.port())).unwrap();
    let client = RaphtoryGraphQLClient::new(endpoint, None);
    (client, handler, span_exporter, log_exporter, tmp_dir)
}

async fn test_open_telemetry_spans_complete() {
    let (client, handler, span_exporter, log_exporter, _tmp_dir) =
        setup_for_span_tests(TracingLevel::COMPLETE).await;
    let _ = client
        .query(OPEN_TELEMETRY_QUERY, HashMap::new())
        .await
        .unwrap();
    handler.stop().await;

    let finished_spans = span_exporter.get_finished_spans().unwrap();
    let all_spans: HashSet<String> = finished_spans
        .iter()
        .map(|span| span.name.to_string())
        .collect();
    assert_eq!(
        all_spans,
        HashSet::from([
            "addEdge".to_string(),
            "addNode".to_string(),
            "countNodes".to_string(),
            "execute".to_string(),
            "flush".to_string(),
            "graph".to_string(),
            "hasNode".to_string(),
            "id".to_string(),
            "node".to_string(),
            "parse".to_string(),
            "request".to_string(),
            "success".to_string(),
            "updateGraph".to_string(),
            "validation".to_string(),
        ])
    );

    let emitted_logs = log_exporter.get_emitted_logs().unwrap();
    assert!(!emitted_logs.is_empty());
    handler.wait().await.unwrap();
}

async fn test_open_telemetry_spans_essential() {
    let (client, handler, span_exporter, log_exporter, _tmp_dir) =
        setup_for_span_tests(TracingLevel::ESSENTIAL).await;
    let _ = client
        .query(OPEN_TELEMETRY_QUERY, HashMap::new())
        .await
        .unwrap();
    handler.stop().await;

    let finished_spans = span_exporter.get_finished_spans().unwrap();
    let all_spans: HashSet<String> = finished_spans
        .iter()
        .map(|span| span.name.to_string())
        .collect();
    assert_eq!(
        all_spans,
        HashSet::from([
            "addEdge".to_string(),
            "addNode".to_string(),
            "execute".to_string(),
            "graph".to_string(),
            "node".to_string(),
            "parse".to_string(),
            "request".to_string(),
            "updateGraph".to_string(),
            "validation".to_string(),
        ])
    );

    let emitted_logs = log_exporter.get_emitted_logs().unwrap();
    assert!(!emitted_logs.is_empty());
    handler.wait().await.unwrap();
}

async fn test_open_telemetry_spans_minimal() {
    let (client, handler, span_exporter, log_exporter, _tmp_dir) =
        setup_for_span_tests(TracingLevel::MINIMAL).await;
    let _ = client
        .query(OPEN_TELEMETRY_QUERY, HashMap::new())
        .await
        .unwrap();
    handler.stop().await;

    let finished_spans = span_exporter.get_finished_spans().unwrap();
    let all_spans: HashSet<String> = finished_spans
        .iter()
        .map(|span| span.name.to_string())
        .collect();
    assert_eq!(
        all_spans,
        HashSet::from([
            "execute".to_string(),
            "parse".to_string(),
            "request".to_string(),
            "validation".to_string(),
        ])
    );

    let emitted_logs = log_exporter.get_emitted_logs().unwrap();
    assert!(!emitted_logs.is_empty());
}

#[tokio::test]
async fn test_open_telemetry_spans() {
    // The following tests share the same global in-memory exporters and hence need to be run sequentially to prevent spans and logs from getting mangled.
    test_open_telemetry_spans_complete().await;
    test_open_telemetry_spans_essential().await;
    test_open_telemetry_spans_minimal().await;
}
