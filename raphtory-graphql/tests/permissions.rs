use rand::prelude::IndexedRandom;
use raphtory::prelude::*;
use raphtory_graphql::client::{raphtory_client::RaphtoryGraphQLClient, ClientError};
use url::Url;

/// Create namespaces in graphql using the given tree.
/// Leaf nodes are graphs, internal nodes are namespaces.
fn create_namespaces(tree: &Graph, url: Url) {
    let client = RaphtoryGraphQLClient::new(url, None);
}

/// Create a random tree with the given number of nodes.
fn create_tree(nodes: u64) -> Graph {
    let graph = Graph::new();

    if nodes == 0 {
        return graph;
    }

    for node_id in 0..nodes {
        graph
            .add_node(0, node_id, NO_PROPS, None)
            .unwrap();
    }

    let mut rng = rand::rng();
    let mut available_parents = vec![0]; // start with root

    for node_id in 1..nodes {
        let parent_id = available_parents.choose(&mut rng).unwrap();

        graph
            .add_edge(0, *parent_id, node_id, NO_PROPS, None)
            .unwrap();

        available_parents.push(node_id);
    }

    graph
}

#[test]
fn smoke_create_namespace_tree() {
    let g = create_tree(10);
    assert_eq!(g.count_nodes(), 10);
    assert_eq!(g.count_edges(), 9);
}
