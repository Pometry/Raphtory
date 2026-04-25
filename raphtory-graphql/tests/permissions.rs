use std::ops::RangeInclusive;
use std::{hint::black_box, sync::LazyLock, time::Duration};

use proptest::prelude::*;
use rand::prelude::IndexedRandom;
use raphtory::{algorithms::components::in_component, prelude::*};
use raphtory::db::api::storage::storage::Config;
use raphtory_graphql::{
    client::raphtory_client::RaphtoryGraphQLClient,
    server::{RunningGraphServer, GraphServer},
};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use url::Url;

const PORT: u16 = 43871;

static RUNTIME: LazyLock<Runtime> =
    LazyLock::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

/// Create namespaces and graphs in graphql using the given tree.
/// Every leaf node is turned into a graph using the path from root as the namespace.
fn create_graphs(tree: &Graph, url: Url) -> Vec<String> {
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
        .block_on(RaphtoryGraphQLClient::connect(url, None))
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
    RUNTIME.block_on(async {
        let mut tempdir = TempDir::new().unwrap();

        // Prevent server drop from failing due to tempdir cleanup.
        tempdir.disable_cleanup(true);

        let server = GraphServer::new(
            tempdir.path().to_path_buf(),
            None,
            None,
            Config::default(),
        )
        .await
        .unwrap();

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
            let graph_paths = create_graphs(&namespace_tree, url);

            // For each
            black_box(_server);
            black_box(_tempdir);
        }
    );
}
