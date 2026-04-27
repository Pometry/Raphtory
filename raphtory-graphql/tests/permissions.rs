mod utils;

use std::collections::BTreeSet;
use std::hint::black_box;
use std::ops::RangeInclusive;
use std::sync::LazyLock;

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use rand::prelude::IndexedRandom;
use raphtory::prelude::*;
use serde_json::json;
use url::Url;

use utils::graphql::{
    create_graphs, create_role, get_client,
    grant_graph, grant_namespace, start_server,
};
use utils::strategy::{permissions_strategy, PermissionTarget};

const PORT: u16 = 43871;

// Borrowed from test_permissions.py.
const PUB_KEY: &str = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno=";
const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFzEcSO/duEjjX4qKxDVy4uLqfmiEIA6bEw1qiPyzTQg
-----END PRIVATE KEY-----";

pub static ADMIN_JWT: LazyLock<String> = LazyLock::new(|| {
    let key = EncodingKey::from_ed_pem(PRIVATE_KEY.as_bytes())
        .expect("decode Ed25519 private key for test JWTs");

    encode(
        &Header::new(Algorithm::EdDSA),
        &json!({ "access": "rw", "role": "admin" }),
        &key,
    )
    .expect("encode admin JWT")
});

pub fn namespace_paths(graph_paths: &[String]) -> Vec<String> {
    let mut namespaces = BTreeSet::new();

    for path in graph_paths {
        let mut end = 0usize;
        while let Some(rel) = path[end..].find('/') {
            end += rel;
            namespaces.insert(path[..end].to_string());
            end += 1;
        }
    }

    namespaces.into_iter().collect()
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

#[test]
fn permissions_proptest() {
    const PROPTEST_CASES: u32 = 10;
    const TREE_SIZE_RANGE: RangeInclusive<u64> = 1..=20;
    const NUM_USERS_RANGE: RangeInclusive<u64> = 1..=10;

    proptest!(
        ProptestConfig::with_cases(PROPTEST_CASES),
        |(tree_size in TREE_SIZE_RANGE, num_users in NUM_USERS_RANGE)| {
            let (_server, _tempdir) = start_server(PORT, PUB_KEY);

            let url = Url::parse(&format!("http://127.0.0.1:{PORT}")).unwrap();
            let client = get_client(url, ADMIN_JWT.to_string());

            // Create nested namespaces and graphs on the server.
            let namespace_tree = create_tree(tree_size);
            let graph_paths = create_graphs(&client, &namespace_tree);
            let namespace_paths = namespace_paths(&graph_paths);

            // Create roles for each user.
            for i in 0..num_users {
                let role = format!("user_{i}");
                create_role(&client, &role);
            }

            let mut runner = proptest::test_runner::TestRunner::default();

            let grants = permissions_strategy(num_users, graph_paths.len(), namespace_paths.len())
                .new_tree(&mut runner)
                .unwrap()
                .current();

            for grant in &grants {
                let role = format!("user_{}", grant.user_id);

                let (path, target) = match grant.target {
                    PermissionTarget::Graph => (
                        &graph_paths[grant.path_idx],
                        PermissionTarget::Graph,
                    ),
                    PermissionTarget::Namespace => (
                        &namespace_paths[grant.path_idx],
                        PermissionTarget::Namespace,
                    ),
                };

                match target {
                    PermissionTarget::Graph => {
                        grant_graph(&client, &role, path, grant.graph_permission)
                    }
                    PermissionTarget::Namespace => {
                        grant_namespace(&client, &role, path, grant.namespace_permission)
                    }
                }
            }

            black_box(_server);
            black_box(_tempdir);
            black_box(graph_paths);
            black_box(namespace_paths);
            black_box(grants);
        }
    );
}
