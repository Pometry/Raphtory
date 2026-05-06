use std::collections::HashSet;

use raphtory_graphql::client::{raphtory_client::RaphtoryGraphQLClient, ClientError};
use serde_json::Value as JsonValue;

use crate::utils::{
    graphql::{create_graph, delete_graph, gql},
    strategy::Permission,
};

// --- Graph validators ---

pub fn validate_graph_grant(
    path: &str,
    permission: Option<Permission>,
    client: &RaphtoryGraphQLClient,
) {
    match permission {
        Some(permission_enum) => match permission_enum {
            Permission::Discover => {
                panic!("Discover permission is not supported for graphs");
            }
            Permission::Introspect => {
                assert!(
                    can_introspect_graph(path, client).unwrap(),
                    "graph {path} should be introspectable"
                );
                assert!(
                    !can_read_graph(path, client).unwrap(),
                    "graph {path} should not be readable"
                );
                assert!(
                    !can_write_graph(path, client).unwrap(),
                    "graph {path} should not be writable"
                );
            }
            Permission::Read => {
                assert!(
                    can_introspect_graph(path, client).unwrap(),
                    "graph {path} should be introspectable"
                );
                assert!(
                    can_read_graph(path, client).unwrap(),
                    "graph {path} should be readable"
                );
                assert!(
                    !can_write_graph(path, client).unwrap(),
                    "graph {path} should not be writable"
                );
            }
            Permission::Write => {
                assert!(
                    can_introspect_graph(path, client).unwrap(),
                    "graph {path} should be introspectable"
                );
                assert!(
                    can_read_graph(path, client).unwrap(),
                    "graph {path} should be readable"
                );
                assert!(
                    can_write_graph(path, client).unwrap(),
                    "graph {path} should be writable"
                );
            }
        },
        None => {
            // Graph has no permission attributed to it, no access should be allowed.
            assert!(
                !can_read_graph(path, client).unwrap(),
                "graph {path} should not be readable"
            );
            assert!(
                !can_introspect_graph(path, client).unwrap(),
                "graph {path} should not be introspectable"
            );
            assert!(
                !can_write_graph(path, client).unwrap(),
                "graph {path} should not be writable"
            );
        }
    }
}

pub fn can_introspect_graph(
    path: &str,
    client: &RaphtoryGraphQLClient,
) -> Result<bool, ClientError> {
    let query = format!(r#"query {{ graphMetadata(path: "{path}") {{ path nodeCount }} }}"#);
    let data = match gql(&query, client) {
        Ok(data) => data,
        Err(e) if access_denied(&e) => return Ok(false),
        Err(e) => return Err(e),
    };

    Ok(data
        .get("graphMetadata")
        .is_some_and(|metadata| !metadata.is_null()))
}

pub fn can_read_graph(path: &str, client: &RaphtoryGraphQLClient) -> Result<bool, ClientError> {
    let query = format!(r#"query {{ graph(path: "{path}") {{ path }} }}"#);
    let data = match gql(&query, client) {
        Ok(data) => data,
        Err(e) if access_denied(&e) => return Ok(false),
        Err(e) => return Err(e),
    };

    Ok(data
        .get("graph")
        .and_then(|graph| graph.get("path"))
        .and_then(JsonValue::as_str)
        .is_some_and(|graph_path| graph_path == path))
}

pub fn can_write_graph(path: &str, client: &RaphtoryGraphQLClient) -> Result<bool, ClientError> {
    let query = format!(
        r#"query {{ updateGraph(path: "{path}") {{ addNode(time: 1, name: "test_node") {{ success }} }} }}"#
    );
    let data = match gql(&query, client) {
        Ok(data) => data,
        Err(e) if access_denied(&e) => return Ok(false),
        Err(e) => return Err(e),
    };

    Ok(data
        .get("updateGraph")
        .and_then(|update| update.get("addNode"))
        .and_then(|add_node| add_node.get("success"))
        .and_then(JsonValue::as_bool)
        .unwrap_or(false))
}

// --- Namespace validators ---

pub fn validate_namespace_grant(
    path: &str,
    permission: Option<Permission>,
    client: &RaphtoryGraphQLClient,
    children: &[String],
) {
    match permission {
        Some(permission_enum) => match permission_enum {
            Permission::Discover => {
                assert!(
                    can_discover_namespace(path, client).unwrap(),
                    "namespace {path} should be discoverable"
                );
            }
            Permission::Introspect => {
                assert!(
                    can_introspect_namespace(path, children, client).unwrap(),
                    "namespace {path} should be introspectable"
                );
                assert!(
                    !can_write_namespace(path, client).unwrap(),
                    "namespace {path} should not be writable"
                );
            }
            Permission::Read => {
                assert!(
                    can_introspect_namespace(path, children, client).unwrap(),
                    "namespace {path} should be introspectable"
                );
                assert!(
                    !can_write_namespace(path, client).unwrap(),
                    "namespace {path} should not be writable"
                );
            }
            Permission::Write => {
                assert!(
                    can_introspect_namespace(path, children, client).unwrap(),
                    "namespace {path} should be introspectable"
                );
                assert!(
                    can_write_namespace(path, client).unwrap(),
                    "namespace {path} should be writable"
                );
            }
        },
        None => {
            // Namespace has no permission attributed to it, no access should be allowed.
            assert!(
                !can_discover_namespace(path, client).unwrap(),
                "namespace {path} should not be discoverable"
            );
            assert!(
                !can_introspect_namespace(path, children, client).unwrap(),
                "namespace {path} should not be introspectable"
            );
            assert!(
                !can_write_namespace(path, client).unwrap(),
                "namespace {path} should not be writable"
            );
        }
    }
}

/// Verify that the namespace at `path` appears in its parent listing.
pub fn can_discover_namespace(
    path: &str,
    client: &RaphtoryGraphQLClient,
) -> Result<bool, ClientError> {
    let parent = path.rsplit_once('/').map_or("root", |(parent, _)| parent);

    let query = if parent == "root" {
        r#"query { root { children { list { path } } } }"#.to_string()
    } else {
        format!(r#"query {{ namespace(path: "{parent}") {{ children {{ list {{ path }} }} }} }}"#)
    };

    let data = match gql(&query, client) {
        Ok(data) => data,
        Err(e) if access_denied(&e) => return Ok(false),
        Err(e) => return Err(e),
    };

    let children = if parent == "root" {
        data.get("root")
    } else {
        data.get("namespace")
    };

    // Verify that the parent's children list is non-empty and contains this namespace.
    Ok(children
        .and_then(|namespace| namespace.get("children"))
        .and_then(|children| children.get("list"))
        .and_then(JsonValue::as_array)
        .is_some_and(|entries| {
            !entries.is_empty()
                && entries.iter().any(|entry| {
                    entry
                        .get("path")
                        .and_then(JsonValue::as_str)
                        .is_some_and(|entry_path| entry_path == path)
                })
        }))
}

// Verify that the namespace at `path` can have its listings browsed and that
// immediate graphs and child namespaces match `children`.
pub fn can_introspect_namespace(
    path: &str,
    children: &[String],
    client: &RaphtoryGraphQLClient,
) -> Result<bool, ClientError> {
    let query = format!(
        r#"query {{ namespace(path: "{path}") {{ graphs {{ list {{ path }} }} children {{ list {{ path }} }} }} }}"#
    );

    let data = match gql(&query, client) {
        Ok(data) => data,
        Err(e) if access_denied(&e) => return Ok(false),
        Err(e) => return Err(e),
    };

    let Some(namespace) = data.get("namespace").filter(|v| !v.is_null()) else {
        return Ok(false);
    };

    let mut listed = HashSet::new();

    if let Some(entries) = namespace
        .get("graphs")
        .and_then(|graphs| graphs.get("list"))
        .and_then(JsonValue::as_array)
    {
        for entry in entries {
            if let Some(p) = entry.get("path").and_then(JsonValue::as_str) {
                listed.insert(path_last_segment(p).to_string());
            }
        }
    }

    if let Some(entries) = namespace
        .get("children")
        .and_then(|ch| ch.get("list"))
        .and_then(JsonValue::as_array)
    {
        for entry in entries {
            if let Some(p) = entry.get("path").and_then(JsonValue::as_str) {
                listed.insert(path_last_segment(p).to_string());
            }
        }
    }

    let expected: HashSet<String> = children.iter().cloned().collect();

    Ok(listed == expected)
}

/// Can create and delete graphs at `path`.
pub fn can_write_namespace(
    path: &str,
    client: &RaphtoryGraphQLClient,
) -> Result<bool, ClientError> {
    let graph_path = format!("{path}/test_new_graph");

    match create_graph(&graph_path, client) {
        Ok(()) => {}
        Err(e) if access_denied(&e) => return Ok(false),
        Err(e) => return Err(e),
    }

    match delete_graph(&graph_path, client) {
        Ok(()) => Ok(true),
        Err(e) if access_denied(&e) => Ok(false),
        Err(e) => Err(e),
    }
}

fn access_denied(err: &ClientError) -> bool {
    err.to_string().contains("Access denied")
}

fn path_last_segment(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}
