mod utils;

use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::LazyLock;

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use proptest::prelude::*;
use raphtory::db::api::view::MaterializedGraph;
use raphtory::prelude::{GraphViewOps, NodeViewOps, Prop, PropUnwrap, PropertiesOps};
use serde_json::json;
use url::Url;

use utils::strategy::{permissions_strategy, Permission, PermissionGrant};

use utils::graphql::{
    create_graph, create_role, create_grant, get_client,
    start_server,
};

use crate::utils::graphql::{validate_graph_grant, validate_namespace_grant};

const PORT: u16 = 43871;

// Borrowed from test_permissions.py.
const PUB_KEY: &str = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno=";
const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFzEcSO/duEjjX4qKxDVy4uLqfmiEIA6bEw1qiPyzTQg
-----END PRIVATE KEY-----";

static ADMIN_JWT: LazyLock<String> = LazyLock::new(|| {
    let key = EncodingKey::from_ed_pem(PRIVATE_KEY.as_bytes())
        .expect("decode Ed25519 private key for test JWTs");

    encode(
        &Header::new(Algorithm::EdDSA),
        &json!({ "access": "rw", "role": "admin" }),
        &key,
    )
    .expect("encode admin JWT")
});

// Track a permission grant across the given namespace tree.
fn track_grant(grant: &PermissionGrant, user_trees: &[MaterializedGraph]) {
    match grant {
        PermissionGrant::Graph { user_id, path, permission } => {
            assert!(matches!(permission, Permission::Read | Permission::Write));

            let user_tree = &user_trees[*user_id];

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
        }
        PermissionGrant::Namespace { user_id, path, permission } => {
            assert!(
                matches!(permission, Permission::Introspect | Permission::Read | Permission::Write)
            );

            let user_tree = &user_trees[*user_id];

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

            // Propagate the permission to descendant namespaces and graphs.
            propagate_down(path, user_tree, *permission);
        }
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

fn validate_grants(user_tree: &MaterializedGraph) {
    for node in user_tree.nodes() {
        let permission_prop = node.metadata().get("permission").and_then(|p| p.into_str());
        let permission = permission_prop.and_then(|p| Permission::from_str(p.as_ref()).ok());

        if node.out_neighbours().is_empty() {
            validate_graph_grant(&node, permission);
        } else {
            validate_namespace_grant(&node, permission);
        }
    }
}

#[test]
fn permissions_proptest() {
    const PROPTEST_CASES: u32 = 10;
    const NAMESPACE_SIZE: RangeInclusive<usize> = 1..=20;
    const NUM_USERS: RangeInclusive<usize> = 1..=10;

    proptest!(
        ProptestConfig::with_cases(PROPTEST_CASES),
        |(case in permissions_strategy(NAMESPACE_SIZE, NUM_USERS))| {
            let (_server, _tempdir) = start_server(PORT, PUB_KEY);

            let url = Url::parse(&format!("http://127.0.0.1:{PORT}")).unwrap();
            let client = get_client(url, ADMIN_JWT.to_string());

            let tree = case.namespace_tree.clone();

            // Create graphs on the server.
            for path in &case.graph_paths {
                create_graph(&client, path);
            }

            let mut user_trees = Vec::with_capacity(case.num_users);

            // Create roles and separate namespace trees for each user.
            for i in 0..case.num_users {
                let role = format!("user_{i}");
                create_role(&client, &role);

                let user_tree = tree.materialize().unwrap();
                user_trees.push(user_tree);
            }

            for grant in &case.grants {
                create_grant(&client, grant);
                track_grant(grant, &user_trees);
            }

            for user_tree in &user_trees {
                validate_grants(user_tree);
            }
        }
    );
}
