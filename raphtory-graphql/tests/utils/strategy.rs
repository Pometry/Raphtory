use std::{fmt, ops::RangeInclusive};
use std::str::FromStr;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;
use raphtory::prelude::*;

#[derive(Clone)]
pub struct PermissionsCase {
    pub num_users: usize,
    pub grants: Vec<PermissionGrant>,
    pub graph_paths: Vec<String>,
    pub namespace_tree: Graph,
}

impl fmt::Debug for PermissionsCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Exclude the namespace tree to avoid printing large graphs.
        f.debug_struct("PermissionsCase")
            .field("num_users", &self.num_users)
            .field("grants", &self.grants)
            .field("graph_paths", &self.graph_paths)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub enum PermissionGrant {
    Graph {
        user_id: usize,
        path: String,
        permission: Permission,
    },
    Namespace {
        user_id: usize,
        path: String,
        permission: Permission,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum Permission {
    Discover,
    Introspect,
    Read,
    Write,
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Permission::Discover => f.write_str("DISCOVER"),
            Permission::Introspect => f.write_str("INTROSPECT"),
            Permission::Read => f.write_str("READ"),
            Permission::Write => f.write_str("WRITE"),
        }
    }
}

impl FromStr for Permission {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DISCOVER" => Ok(Permission::Discover),
            "INTROSPECT" => Ok(Permission::Introspect),
            "READ" => Ok(Permission::Read),
            "WRITE" => Ok(Permission::Write),
            _ => Err("invalid permission"),
        }
    }
}

pub fn permissions_strategy(
    namespace_size: RangeInclusive<usize>,
    num_users: RangeInclusive<usize>,
) -> BoxedStrategy<PermissionsCase> {
    (namespace_size, num_users)
        .prop_flat_map(|(namespace_size, num_users)| {
            parents_strategy(namespace_size).prop_flat_map(move |parents| {
                let namespace_tree = build_namespace_tree(&parents);
                let graph_paths = leaf_paths(&namespace_tree);
                let namespace_paths = branch_paths(&namespace_tree);

                grants_strategy(num_users, graph_paths.clone(), namespace_paths).prop_map(move |grants| {
                    PermissionsCase {
                        num_users,
                        namespace_tree: namespace_tree.clone(),
                        graph_paths: graph_paths.clone(),
                        grants,
                    }
                })
            })
        })
        .boxed()
}

/// Create a strategy to pick a parent for each node in a tree.
fn parents_strategy(tree_size: usize) -> BoxedStrategy<Vec<usize>> {
    if tree_size <= 1 {
        return Just(vec![]).boxed();
    }

    // Root node has no parent.
    let mut strategy: BoxedStrategy<Vec<usize>> = Just(vec![0]).boxed();

    for node in 1..tree_size {
        strategy = strategy
            .prop_flat_map(move |parents| {
                // Pick a random parent for the current node from existing nodes seen so far.
                (0usize..node).prop_map(move |parent| {
                    let mut parents = parents.clone();

                    parents.push(parent);
                    parents
                })
            })
            .boxed();
    }

    strategy
}

fn grants_strategy(
    num_users: usize,
    graph_paths: Vec<String>,
    namespace_paths: Vec<String>,
) -> impl Strategy<Value = Vec<PermissionGrant>> {
    let max_user = num_users - 1;
    let total_paths = namespace_paths.len() + graph_paths.len();
    let num_grants = 1..=total_paths; // FIXME: Increase this to 3x.

    prop::collection::vec(
        (0..=max_user, 0..total_paths)
            .prop_flat_map(move |(user_id, path_idx)| {
                // Choose either a namespace or a graph path based on path_idx.
                if path_idx < namespace_paths.len() {
                    let path = namespace_paths[path_idx].clone();

                    // FIXME: Include revoke permissions.
                    // Exclude Discover since it cannot be applied directly to namespaces.
                    prop_oneof![
                        Just(Permission::Introspect),
                        Just(Permission::Read),
                        Just(Permission::Write),
                    ]
                    .prop_map(move |permission| PermissionGrant::Namespace {
                        user_id,
                        path: path.clone(),
                        permission,
                    })
                    .boxed()
                } else {
                    let graph_idx = path_idx - namespace_paths.len();
                    let path = graph_paths[graph_idx].clone();

                    // FIXME: Include revoke permissions.
                    // Exclude Introspect since it cannot be applied directly to graphs.
                    prop_oneof![
                        Just(Permission::Read),
                        Just(Permission::Write),
                    ]
                    .prop_map(move |permission| PermissionGrant::Graph {
                        user_id,
                        path: path.clone(),
                        permission,
                    })
                    .boxed()
                }
            }),
        num_grants,
    )
}

/// Build a namespace tree with the given number of nodes and parent relationships.
/// `parents` is a slice of indices that represent the parent of each node.
pub fn build_namespace_tree(parents: &[usize]) -> Graph {
    let graph = Graph::new();

    if parents.len() == 0 {
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
    let parent_path = "".to_string();
    let mut leaves = Vec::new();

    stack.push((root, parent_path));

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
    let parent_path = "".to_string();
    let mut branches = Vec::new();

    stack.push((root, parent_path));

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
