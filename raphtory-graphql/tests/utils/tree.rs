use raphtory::prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS};

/// Build a namespace tree with the given number of nodes and parent relationships.
/// `parents` is a slice of indices that represent the parent of each node.
pub fn build_namespace_tree(parents: &[usize]) -> Graph {
    let graph = Graph::new();

    if parents.is_empty() {
        return graph;
    }

    for node in 0..parents.len() {
        let name = format!("node_{node}");

        graph.add_node(0, name, NO_PROPS, None, None).unwrap();

        // Root node has no parent.
        if node == 0 {
            continue;
        }

        let parent = parents[node];
        let parent_name = format!("node_{parent}");
        let node_name = format!("node_{node}");

        graph
            .add_edge(0, parent_name, node_name, NO_PROPS, None)
            .unwrap();
    }

    graph
}

/// Build paths for all leaf nodes in the tree.
/// "node_0" is treated as the root namespace.
/// Example: node_0 -> node_1 -> node_2 returns ["node_1/node_2"]
pub fn leaf_paths(tree: &Graph) -> Vec<String> {
    let mut stack = Vec::new();
    let root = tree.node("node_0").unwrap();
    let mut leaves = Vec::new();

    // Leave out node_0 since it is the root namespace.
    for neighbour in root.out_neighbours() {
        stack.push((neighbour, String::new()));
    }

    while let Some((node, parent_path)) = stack.pop() {
        let node_path = if parent_path.is_empty() {
            // Prevent leading slash in paths.
            node.name()
        } else {
            [parent_path, node.name()].join("/")
        };

        let neighbours = node.out_neighbours();

        if neighbours.is_empty() {
            leaves.push(node_path);
        } else {
            for neighbour in node.out_neighbours() {
                stack.push((neighbour, node_path.clone()));
            }
        }
    }

    leaves
}

/// Build paths for all branch nodes in the tree.
/// "node_0" is treated as the root namespace.
/// Example: node_0 -> node_1 -> node_2 returns ["node_1/"]
pub fn branch_paths(tree: &Graph) -> Vec<String> {
    let mut stack = Vec::new();
    let root = tree.node("node_0").unwrap();
    let mut branches = Vec::new();

    // Leave out node_0 since it is the root namespace.
    for neighbour in root.out_neighbours() {
        stack.push((neighbour, String::new()));
    }

    while let Some((node, parent_path)) = stack.pop() {
        let node_path = if parent_path.is_empty() {
            // Prevent leading slash in paths.
            node.name()
        } else {
            [parent_path, node.name()].join("/")
        };

        let neighbours = node.out_neighbours();

        if !neighbours.is_empty() {
            branches.push(node_path.clone());

            for neighbour in neighbours {
                stack.push((neighbour, node_path.clone()));
            }
        }
    }

    branches
}
