use std::collections::HashMap;
use std::sync::{LazyLock, Once};
use std::time::Duration;

use raphtory::algorithms::components::in_component;
use raphtory::db::api::storage::storage::Config;
use raphtory::prelude::{Graph, GraphViewOps, NodeStateOps, NodeViewOps};
use raphtory_graphql::client::raphtory_client::RaphtoryGraphQLClient;
use raphtory_graphql::config::app_config::AppConfigBuilder;
use raphtory_graphql::server::{apply_server_extension, GraphServer, RunningGraphServer};
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use url::Url;

use crate::utils::strategy::{GraphPermission, NamespacePermission};

static AUTH_INIT: Once = Once::new();

fn init_raphtory_auth() {
    AUTH_INIT.call_once(::auth::init);
}

pub static RUNTIME: LazyLock<Runtime> =
    LazyLock::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

pub fn get_client(url: Url, token: String) -> RaphtoryGraphQLClient {
    RUNTIME
        .block_on(RaphtoryGraphQLClient::connect(url, Some(token)))
        .expect("connect GraphQL client")
}

pub fn start_server(port: u16, pub_key: &str) -> (RunningGraphServer, TempDir) {
    init_raphtory_auth();

    RUNTIME.block_on(async {
        let mut tempdir = TempDir::new().unwrap();
        tempdir.disable_cleanup(true);

        let work_dir = tempdir.path().to_path_buf();
        let permissions_path = work_dir.join("permissions.json");

        let app_config = AppConfigBuilder::new()
            .with_auth_public_key(Some(pub_key.to_string()))
            .expect("test auth public key")
            .build();

        let server = GraphServer::new(work_dir, Some(app_config), None, Config::default())
            .await
            .unwrap();

        // Configure permissions store.
        let server = apply_server_extension(server, Some(permissions_path.as_path()));
        let server = server.start_with_port(port).await.unwrap();

        // Wait for server to start.
        tokio::time::sleep(Duration::from_secs(1)).await;

        (server, tempdir)
    })
}

fn gql(client: &RaphtoryGraphQLClient, query: &str) -> HashMap<String, JsonValue> {
    RUNTIME
        .block_on(client.query(query, HashMap::new()))
        .unwrap_or_else(|e| panic!("Error executing query {query}: {e}"))
}

pub fn create_role(client: &RaphtoryGraphQLClient, name: &str) {
    let query =
        format!(r#"mutation {{ permissions {{ createRole(name: "{name}") {{ success }} }} }}"#);

    let data = gql(client, &query);

    let success = data
        .get("permissions")
        .and_then(|p| p.get("createRole"))
        .and_then(|c| c.get("success"))
        .and_then(JsonValue::as_bool);

    assert_eq!(success, Some(true), "createRole {name} data: {data:?}");
}

pub fn grant_graph(
    client: &RaphtoryGraphQLClient,
    role: &str,
    path: &str,
    permission: GraphPermission,
) {
    let query = format!(
        r#"mutation {{ permissions {{ grantGraph(role: "{role}", path: "{path}", permission: {}) {{ success }} }} }}"#,
        permission.as_gql()
    );

    let data = gql(client, &query);

    let success = data
        .get("permissions")
        .and_then(|p| p.get("grantGraph"))
        .and_then(|c| c.get("success"))
        .and_then(JsonValue::as_bool);

    assert_eq!(
        success,
        Some(true),
        "grantGraph role={role} path={path} permission={} data: {data:?}",
        permission.as_gql()
    );
}

pub fn grant_namespace(
    client: &RaphtoryGraphQLClient,
    role: &str,
    path: &str,
    permission: NamespacePermission,
) {
    let query = format!(
        r#"mutation {{ permissions {{ grantNamespace(role: "{role}", path: "{path}", permission: {}) {{ success }} }} }}"#,
        permission.as_gql()
    );

    let data = gql(client, &query);

    let success = data
        .get("permissions")
        .and_then(|p| p.get("grantNamespace"))
        .and_then(|c| c.get("success"))
        .and_then(JsonValue::as_bool);

    assert_eq!(
        success,
        Some(true),
        "grantNamespace role={role} path={path} permission={} data: {data:?}",
        permission.as_gql()
    );
}

/// Create namespaces and graphs in graphql using the given tree.
/// Each leaf node is turned into a graph, all other nodes are turned into namespaces.
pub fn create_graphs(client: &RaphtoryGraphQLClient, tree: &Graph) -> Vec<String> {
    let mut graph_paths = Vec::new();

    for node in tree.nodes() {
        if node.out_neighbours().is_empty() { // Leaf node
            // In-components are sorted by distance from the leaf node.
            let mut in_components = in_component(node.clone())
                .iter()
                .map(|(node_view, _)| node_view.name())
                .collect::<Vec<_>>();

            // Include the leaf node itself in the path.
            in_components.push(node.name());
            graph_paths.push(in_components.join("/"));
        }
    }

    for path in &graph_paths {
        RUNTIME.block_on(client.new_graph(path, "EVENT")).unwrap();
    }

    graph_paths
}
