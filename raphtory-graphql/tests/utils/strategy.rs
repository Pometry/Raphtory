use std::{fmt, ops::RangeInclusive};
use std::str::FromStr;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;
use raphtory::prelude::*;
use crate::utils::tree::{branch_paths, build_namespace_tree, leaf_paths};

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

#[derive(Debug, Clone, Copy)]
pub enum GrantType {
    Graph,
    Namespace,
}

#[derive(Debug, Clone)]
pub struct PermissionGrant {
    pub grant_type: GrantType,
    pub user_id: usize,
    pub path: String,
    pub permission: Permission,
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

                    // Exclude discover since it cannot be applied directly to namespaces.
                    prop_oneof![
                        Just(Permission::Introspect),
                        Just(Permission::Read),
                        Just(Permission::Write),
                    ]
                    .prop_map(move |permission| PermissionGrant {
                        grant_type: GrantType::Namespace,
                        user_id,
                        path: path.clone(),
                        permission,
                    })
                    .boxed()
                } else {
                    let graph_idx = path_idx - namespace_paths.len();
                    let path = graph_paths[graph_idx].clone();

                    // Exclude introspect since it cannot be applied directly to graphs.
                    prop_oneof![
                        Just(Permission::Read),
                        Just(Permission::Write),
                    ]
                    .prop_map(move |permission| PermissionGrant {
                        grant_type: GrantType::Graph,
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

