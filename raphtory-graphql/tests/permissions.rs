use std::time::Duration;

use rand::prelude::IndexedRandom;
use raphtory::{algorithms::components::in_component, prelude::*};
use raphtory_graphql::{client::raphtory_client::RaphtoryGraphQLClient, server::RunningGraphServer, GraphServer};
use tempfile::tempdir;
use url::Url;

const PORT: u16 = 43871;

/// Create namespaces in graphql using the given tree.
/// Every leaf node is turned into a graph using the path from root as the namespace.
async fn create_namespaces(tree: &Graph, url: Url) {
    let client = RaphtoryGraphQLClient::connect(url, None).await.unwrap();
    let mut graph_paths = Vec::new();

    for node in tree.nodes() {
        if node.out_neighbours().len() == 0 { // Leaf node
            // In-component order yields path from root to node.
            let mut in_components = in_component(node.clone())
                .iter()
                .map(|(node_view, _)| {
                    node_view.name()
                })
                .collect::<Vec<_>>();

            // Include the node itself in the path.
            in_components.push(node.name());

            let path = in_components.join("/");
            graph_paths.push(path);
        }
    }

    graph_paths.sort();

    println!("Graph paths: {graph_paths:?}");

    for path in graph_paths {
        println!("Creating graph: {path}");
        client.new_graph(path.as_str(), "EVENT").await.unwrap();
    }
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

async fn start_server() -> RunningGraphServer {
    let tempdir = tempdir().unwrap();
    let server = GraphServer::new(tempdir.path().to_path_buf(), None, None, Config::default()).await.unwrap();
    let server = server.start_with_port(PORT).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    server
}

#[tokio::test]
async fn permissions_proptest() {
    let url = Url::parse(&format!("http://127.0.0.1:{PORT}")).unwrap();
    let namespace_tree = create_tree(10);

    let server = start_server().await;
    create_namespaces(&namespace_tree, url).await;
}
