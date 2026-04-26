use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Once;
use std::{hint::black_box, sync::LazyLock, time::Duration};

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use proptest::prelude::*;
use rand::prelude::IndexedRandom;
use raphtory::{algorithms::components::in_component, prelude::*};
use raphtory::db::api::storage::storage::Config;
use raphtory_graphql::client::raphtory_client::RaphtoryGraphQLClient;
use raphtory_graphql::config::app_config::AppConfigBuilder;
use raphtory_graphql::server::{apply_server_extension, GraphServer, RunningGraphServer};
use serde_json::{json, Value as JsonValue};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use url::Url;

const PORT: u16 = 43871;

/// Same key pair as `Raphtory/python/tests/test_permissions.py` and `test_auth.py`.
const AUTH_PUB_KEY: &str = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno=";
const AUTH_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFzEcSO/duEjjX4qKxDVy4uLqfmiEIA6bEw1qiPyzTQg
-----END PRIVATE KEY-----";

static AUTH_INIT: Once = Once::new();
static RUNTIME: LazyLock<Runtime> =
    LazyLock::new(|| Runtime::new().expect("Failed to create Tokio runtime"));
static ADMIN_JWT: LazyLock<String> = LazyLock::new(|| {
    let key = EncodingKey::from_ed_pem(AUTH_PRIVATE_KEY_PEM.as_bytes())
        .expect("decode Ed25519 private key for test JWTs");
    encode(
        &Header::new(Algorithm::EdDSA),
        &json!({ "access": "rw", "role": "admin" }),
        &key,
    )
    .expect("encode admin JWT")
});

fn init_raphtory_auth() {
    AUTH_INIT.call_once(auth::init);
}

/// `createRole` with admin credentials — mirrors Python `create_role` + default `gql(..., ADMIN_HEADERS)`.
fn create_role(client: &RaphtoryGraphQLClient, name: &str) {
    let q = format!(
        r#"mutation {{ permissions {{ createRole(name: "{name}") {{ success }} }} }}"#
    );
    let data = RUNTIME
        .block_on(client.query(&q, HashMap::new()))
        .unwrap_or_else(|e| panic!("createRole {name}: {e}"));
    let success = data
        .get("permissions")
        .and_then(|p| p.get("createRole"))
        .and_then(|c| c.get("success"))
        .and_then(JsonValue::as_bool);
    assert_eq!(success, Some(true), "createRole {name} data: {data:?}");
}

/// Create namespaces and graphs in graphql using the given tree.
/// Every leaf node is turned into a graph using the path from root as the namespace.
fn create_graphs(tree: &Graph, url: Url, admin_token: &str) -> Vec<String> {
    let mut graph_paths = Vec::new();

    for node in tree.nodes() {
        if node.out_neighbours().len() == 0 { // Leaf node
            // In-component order yields path from root to node.
            let mut in_components = in_component(node.clone())
                .iter()
                .map(|(node_view, _)| node_view.name())
                .collect::<Vec<_>>();

            // Include the node itself in the path.
            in_components.push(node.name());

            let path = in_components.join("/");
            graph_paths.push(path);
        }
    }

    let client = RUNTIME
        .block_on(RaphtoryGraphQLClient::connect(
            url,
            Some(admin_token.to_string()),
        ))
        .unwrap();

    for path in graph_paths.iter() {
        RUNTIME
            .block_on(client.new_graph(path, "EVENT"))
            .unwrap();
    }

    graph_paths
}

/// Create a random tree with the given number of nodes.
fn create_tree(nodes: u64) -> Graph {
    let graph = Graph::new();

    if nodes == 0 {
        return graph;
    }

    for node in 0..nodes {
        let name = format!("node_{node}");

        graph
            .add_node(0, name, NO_PROPS, None, None)
            .unwrap();
    }

    let mut rng = rand::rng();
    let mut available_parents = vec![0]; // start with root

    // For each node, add an edge to a random parent.
    for node in 1..nodes {
        let parent = available_parents.choose(&mut rng).unwrap();
        let parent_name = format!("node_{parent}");
        let node_name = format!("node_{node}");

        graph
            .add_edge(0, parent_name, node_name, NO_PROPS, None)
            .unwrap();

        available_parents.push(node);
    }

    graph
}

fn start_server() -> (RunningGraphServer, TempDir) {
    init_raphtory_auth();

    RUNTIME.block_on(async {
        let mut tempdir = TempDir::new().unwrap();

        // Prevent server drop from failing due to tempdir cleanup.
        tempdir.disable_cleanup(true);

        let work_dir = tempdir.path().to_path_buf();
        let permissions_path = work_dir.join("permissions.json");
        // Same public key and default `require_auth_for_reads` as Python `test_permissions` server.
        let app_config = AppConfigBuilder::new()
            .with_auth_public_key(Some(AUTH_PUB_KEY.to_string()))
            .expect("test auth public key")
            .build();

        let server = GraphServer::new(
            work_dir,
            Some(app_config),
            None,
            Config::default(),
        )
        .await
        .unwrap();
        let server = apply_server_extension(server, Some(permissions_path.as_path()));

        let server = server.start_with_port(PORT).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        (server, tempdir)
    })
}

#[test]
fn permissions_proptest() {
    const PROPTEST_CASES: u32 = 10;
    const TREE_SIZE_RANGE: RangeInclusive<u64> = 1..=20;
    const NUM_USERS_RANGE: RangeInclusive<u64> = 1..=10;

    proptest!(
        ProptestConfig::with_cases(PROPTEST_CASES),
        |(tree_size in TREE_SIZE_RANGE, num_users in NUM_USERS_RANGE)| {
            let url = Url::parse(&format!("http://127.0.0.1:{PORT}")).unwrap();
            let (_server, _tempdir) = start_server();
            let namespace_tree = create_tree(tree_size);
            let admin_token = ADMIN_JWT.as_str();
            let graph_paths = create_graphs(&namespace_tree, url.clone(), admin_token);

            let client = RUNTIME
                .block_on(RaphtoryGraphQLClient::connect(
                    url,
                    Some(admin_token.to_string()),
                ))
                .unwrap();

            for i in 0..num_users {
                let role = format!("user_{i}");
                create_role(&client, &role);
            }

            black_box(_server);
            black_box(_tempdir);
            black_box(graph_paths);
        }
    );
}
