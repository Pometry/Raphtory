use std::ops::RangeInclusive;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;
use raphtory::prelude::*;

const MAX_GRANTS_PER_CASE: usize = 50;

/// Build a namespace tree with the given number of nodes and parent relationships.
/// `parents` is a slice of indices that represent the parent of each node, excluding the root.
pub fn build_namespace_tree(nodes: usize, parents: &[usize]) -> Graph {
    let graph = Graph::new();

    if parents.len() != nodes - 1 {
        panic!("parents must have length nodes - 1");
    }

    if nodes == 0 {
        return graph;
    }

    for node in 0..nodes {
        let name = format!("node_{node}");

        graph
            .add_node(0, name, NO_PROPS, None, None)
            .unwrap();
    }

    for node in 1..nodes {
        let parent = parents[node - 1];
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

/// Create a strategy to pick a parent for each node in a tree.
fn parents_strategy(tree_size: usize) -> BoxedStrategy<Vec<usize>> {
    if tree_size <= 1 {
        return Just(vec![]).boxed();
    }

    let mut strategy: BoxedStrategy<Vec<usize>> = Just(vec![]).boxed();

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

#[derive(Debug, Clone)]
pub struct PermissionsCase {
    pub num_users: usize,
    pub namespace_tree: Graph,
    pub grants: Vec<PermissionGrant>,
}

#[derive(Debug, Clone, Copy)]
pub enum GraphPermission {
    Read,
    Write,
}

impl GraphPermission {
    pub(crate) fn as_gql(self) -> &'static str {
        match self {
            GraphPermission::Read => "READ",
            GraphPermission::Write => "WRITE",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum NamespacePermission {
    Introspect,
    Read,
    Write,
}

impl NamespacePermission {
    pub(crate) fn as_gql(self) -> &'static str {
        match self {
            NamespacePermission::Introspect => "INTROSPECT",
            NamespacePermission::Read => "READ",
            NamespacePermission::Write => "WRITE",
        }
    }
}

#[derive(Debug, Clone)]
pub enum PermissionGrant {
    Graph {
        user_id: usize,
        path: String,
        permission: GraphPermission,
    },
    Namespace {
        user_id: usize,
        path: String,
        permission: NamespacePermission,
    },
}

fn grants_strategy(
    num_users: usize,
    graph_paths: Vec<String>,
    namespace_paths: Vec<String>,
) -> impl Strategy<Value = Vec<PermissionGrant>> {
    let max_user = num_users.saturating_sub(1);
    let max_graph = graph_paths.len().saturating_sub(1);
    let max_namespace = namespace_paths.len().saturating_sub(1);
    let namespace_available = !namespace_paths.is_empty();

    prop::collection::vec(
        (
            0..=max_user,
            any::<bool>(),
            prop_oneof![Just(GraphPermission::Read), Just(GraphPermission::Write)],
            prop_oneof![
                Just(NamespacePermission::Introspect),
                Just(NamespacePermission::Read),
                Just(NamespacePermission::Write),
            ],
            0usize..=max_graph,
            0usize..=max_namespace,
        )
            .prop_map(
                move |(
                    user_id,
                    choose_namespace,
                    graph_permission,
                    namespace_permission,
                    graph_idx,
                    namespace_idx,
                )| {
                    if choose_namespace && namespace_available {
                        PermissionGrant::Namespace {
                            user_id,
                            path: namespace_paths[namespace_idx].clone(),
                            permission: namespace_permission,
                        }
                    } else {
                        PermissionGrant::Graph {
                            user_id,
                            path: graph_paths[graph_idx].clone(),
                            permission: graph_permission,
                        }
                    }
                },
            ),
        1..=MAX_GRANTS_PER_CASE,
    )
}

pub fn permissions_strategy(
    namespace_size: RangeInclusive<usize>,
    num_users: RangeInclusive<usize>,
) -> BoxedStrategy<PermissionsCase> {
    (namespace_size, num_users)
        .prop_flat_map(|(namespace_size, num_users)| {
            parents_strategy(namespace_size).prop_flat_map(move |parents| {
                let namespace_tree = build_namespace_tree(namespace_size, &parents);
                let leaf_paths = leaf_paths(&namespace_tree);
                let branch_paths = branch_paths(&namespace_tree);

                grants_strategy(num_users, leaf_paths.clone(), branch_paths.clone())
                    .prop_map(move |grants| PermissionsCase {
                        num_users,
                        namespace_tree: namespace_tree.clone(),
                        grants,
                    })
            })
        })
        .boxed()
}
