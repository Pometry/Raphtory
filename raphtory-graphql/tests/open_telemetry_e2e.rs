use mock_collector::{MockServer, Protocol};
use raphtory::{
    db::api::storage::storage::Config,
    prelude::{Graph, StableEncode},
};
use raphtory_graphql::{
    client::remote_client::RemoteClient,
    config::{
        app_config::AppConfigBuilder,
        otlp_config::{TracingLevel, TracingProtocol},
    },
    server::GraphServer,
};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tempfile::tempdir;
use url::Url;

#[tokio::test]
async fn test_open_telemetry_http_tracing_server() {
    let tracing_server = MockServer::builder()
        .protocol(Protocol::HttpBinary)
        .start()
        .await
        .unwrap();

    let work_dir = tempdir().unwrap();
    let graph = Graph::new();
    graph.encode(work_dir.path().join("g")).unwrap();

    let app_config = AppConfigBuilder::new()
        .with_tracing(true)
        .with_tracing_level(TracingLevel::ESSENTIAL)
        .with_otlp_agent_host(Some(format!("http://{}", tracing_server.addr())))
        .with_otlp_transport_protocol(TracingProtocol::HTTP)
        .build();

    let server = GraphServer::new(
        work_dir.path().to_path_buf(),
        Some(app_config),
        Config::default(),
    )
    .await
    .unwrap();
    let handler = server.start_with_port(0).await.unwrap();

    let endpoint = Url::parse(&format!("http://localhost:{}/", handler.port())).unwrap();
    let client = RemoteClient::new(endpoint, None);
    let open_telemetry_query = "query {
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
}".to_string();
    let _ = client
        .query(&open_telemetry_query, HashMap::new())
        .await
        .unwrap();

    handler.stop().await;
    handler.wait().await.unwrap();

    tracing_server
        .wait_for_spans(1, Duration::from_secs(50))
        .await
        .unwrap();
    tracing_server
        .wait_for_logs(1, Duration::from_secs(50))
        .await
        .unwrap();

    tracing_server
        .with_collector(|collector| {
            let all_spans: HashSet<String> = collector
                .spans()
                .iter()
                .map(|span| span.span().name.clone())
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
            assert!(collector.log_count() > 0);
        })
        .await;

    tracing_server.shutdown().await.unwrap();
}
