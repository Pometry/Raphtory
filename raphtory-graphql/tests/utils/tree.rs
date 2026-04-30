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

        graph
            .add_node(0, name, NO_PROPS, None, None)
            .unwrap();

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
/// Example: a -> b -> c returns ["a/b/c"]
pub fn leaf_paths(tree: &Graph) -> Vec<String> {
    let mut stack = Vec::new();
    let root = tree.node("node_0").unwrap();
    let mut leaves = Vec::new();

    stack.push((root, String::new()));

    while let Some((node, parent_path)) = stack.pop() {
        // Prevent leading slash in paths.
        let node_path = if parent_path.is_empty() {
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
/// Example: a -> b -> c returns ["a", "a/b"]
pub fn branch_paths(tree: &Graph) -> Vec<String> {
    let mut stack = Vec::new();
    let root = tree.node("node_0").unwrap();
    let mut branches = Vec::new();

    stack.push((root, String::new()));

    while let Some((node, parent_path)) = stack.pop() {
        // Prevent leading slash in paths.
        let node_path = if parent_path.is_empty() {
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
