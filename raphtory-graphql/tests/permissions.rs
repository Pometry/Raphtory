mod utils;

use std::ops::RangeInclusive;
use std::str::FromStr;

use proptest::prelude::*;
use raphtory::db::api::view::MaterializedGraph;
use raphtory::db::graph::node::NodeView;
use raphtory_graphql::client::raphtory_client::RaphtoryGraphQLClient;
use raphtory::prelude::{GraphViewOps, NodeViewOps, Prop, PropUnwrap, PropertiesOps};
use url::Url;

use utils::jwt::{user_jwt, ADMIN_JWT, PUB_KEY};
use utils::strategy::{permissions_strategy, GrantType, Permission, PermissionGrant};

use utils::graphql::{
    create_graph, create_role, create_grant, get_client,
    start_server,
};

use crate::utils::graphql::{validate_graph_grant, validate_namespace_grant};

const PORT: u16 = 43871;

// Track a permission grant across the given namespace tree.
fn track_grant(grant: &PermissionGrant, user_trees: &[MaterializedGraph]) {
    let user_id = grant.user_id;
    let path = grant.path.as_str();
    let permission = grant.permission;

    let user_tree = &user_trees[user_id];

    // Find the node in the tree corresponding to the given path.
    let node_name = path.split('/').last().unwrap();
    let node = user_tree.node(node_name).unwrap();

    // Update the node's direct permission.
    node
        .update_metadata(vec![
            ("permission", Prop::Str(permission.to_string().into())),
            ("direct", Prop::Bool(true)),
        ])
        .unwrap();

    // Propagate discover to ancestor namespaces.
    propagate_up(path, user_tree);

    // Namespace grants also propagate to descendants.
    if grant.grant_type == GrantType::Namespace {
        propagate_down(path, user_tree, permission);
    }
}

// Propagate discover permissions to ancestor namespaces.
fn propagate_up(path: &str, user_tree: &MaterializedGraph) {
    // Apply discover to each node in the path.
    for node_name in path.split('/').filter(|segment| !segment.is_empty()) {
        let node = user_tree.node(node_name).unwrap();

        // Discover permissions have least precedence, set them if no other permission is set.
        if node.metadata().get("permission").is_none() {
            node
                .update_metadata(vec![
                    ("permission", Prop::Str(Permission::Discover.to_string().into())),
                    ("direct", Prop::Bool(false)),
                ])
                .unwrap();
        }
    }
}

// Propagates a permission to descendant namespaces and graphs.
fn propagate_down(path: &str, user_tree: &MaterializedGraph, permission: Permission) {
    let node_name = path.split('/').last().unwrap();
    let node = user_tree.node(node_name).unwrap();
    let mut stack = Vec::new();

    // Start by propagating to the immediate children.
    for neighbour in node.out_neighbours() {
        stack.push(neighbour);
    }

    while let Some(node) = stack.pop() {
        let node_permission = node.metadata().get("permission");

        if let Some(_) = node_permission {
            let direct = node.metadata().get("direct").unwrap().into_bool().unwrap();

            // Direct permissions override inherited permissions, skip this node
            // and stop propagating.
            if direct {
                continue;
            }
        }

        node
            .update_metadata(vec![
                ("permission", Prop::Str(permission.to_string().into())),
                ("direct", Prop::Bool(false)),
            ])
            .unwrap();

        for neighbour in node.out_neighbours() {
            stack.push(neighbour);
        }
    }
}

fn validate_grant(
    path: &str,
    node: &NodeView<'_, MaterializedGraph>,
    client: &RaphtoryGraphQLClient,
) {
    let permission = node
        .metadata()
        .get("permission")
        .and_then(|p| p.into_str())
        .and_then(|s| Permission::from_str(s.as_ref()).ok());

    let is_graph_path = node.out_neighbours().is_empty();

    if is_graph_path {
        validate_graph_grant(path, permission, client);
    } else {
        validate_namespace_grant(path, permission, client);
    }
}

#[test]
fn permissions_proptest() {
    const PROPTEST_CASES: u32 = 10;
    const NAMESPACE_SIZE: RangeInclusive<usize> = 1..=100;
    const NUM_GRANTS: RangeInclusive<usize> = 1..=100;
    const NUM_USERS: RangeInclusive<usize> = 1..=20;

    proptest!(
        ProptestConfig::with_cases(PROPTEST_CASES),
        |(case in permissions_strategy(NAMESPACE_SIZE, NUM_GRANTS, NUM_USERS))| {
            let (_server, _tempdir) = start_server(PORT, PUB_KEY);

            let url = Url::parse(&format!("http://127.0.0.1:{PORT}")).unwrap();
            let admin_client = get_client(url.clone(), ADMIN_JWT.to_string());

            let tree = case.namespace_tree;

            // Create graphs on the server.
            for path in &case.graph_paths {
                create_graph(path, &admin_client);
            }

            let mut user_trees = Vec::with_capacity(case.num_users);

            // Create roles and separate namespace trees for each user.
            for i in 0..case.num_users {
                let role = format!("user_{i}");
                create_role(&role, &admin_client);

                let user_tree = tree.materialize().unwrap();
                user_trees.push(user_tree);
            }

            // Create grants on the server and track them locally.
            for grant in &case.grants {
                create_grant(grant, &admin_client);
                track_grant(grant, &user_trees);
            }

            // Validate grants across graph paths for each user.
            for path in &case.graph_paths {
                for (user_id, user_tree) in user_trees.iter().enumerate() {
                    let node_name = path.split('/').last().unwrap();
                    let node = user_tree.node(node_name).unwrap();

                    let role = format!("user_{user_id}");
                    let user_client = get_client(url.clone(), user_jwt(&role));
                    validate_grant(path.as_str(), &node, &user_client);
                }
            }

            // Validate grants across namespace paths for each user.
            for path in &case.namespace_paths {
                for (user_id, user_tree) in user_trees.iter().enumerate() {
                    let node_name = path.split('/').last().unwrap();
                    let node = user_tree.node(node_name).unwrap();

                    // Ignore namespaces with no children since permissions are irrelevant.
                    if node.out_neighbours().is_empty() {
                        continue;
                    }

                    let role = format!("user_{user_id}");
                    let user_client = get_client(url.clone(), user_jwt(&role));
                    validate_grant(path.as_str(), &node, &user_client);
                }
            }
        }
    );
}
