use raphtory_graphql::client::raphtory_client::RaphtoryGraphQLClient;
use serde_json::Value as JsonValue;

use crate::utils::graphql::{create_graph, delete_graph, gql};
use crate::utils::strategy::Permission;

pub fn validate_graph_grant(
    path: &str,
    permission: Option<Permission>,
    client: &RaphtoryGraphQLClient,
) {
    match permission {
        Some(permission_enum) => {
            match permission_enum {
                Permission::Discover => {
                    panic!("Discover permission is not supported for graphs");
                }
                Permission::Introspect => {
                    assert!(can_introspect_graph(path, client), "graph {path} should be introspectable");
                    assert!(!can_read_graph(path, client), "graph {path} should not be readable");
                    assert!(!can_write_graph(path, client), "graph {path} should not be writable");
                }
                Permission::Read => {
                    assert!(can_introspect_graph(path, client), "graph {path} should be introspectable");
                    assert!(can_read_graph(path, client), "graph {path} should be readable");
                    assert!(!can_write_graph(path, client), "graph {path} should not be writable");
                }
                Permission::Write => {
                    assert!(can_introspect_graph(path, client), "graph {path} should be introspectable");
                    assert!(can_read_graph(path, client), "graph {path} should be readable");
                    assert!(can_write_graph(path, client), "graph {path} should be writable");
                }
            }
        }
        None => {
            // Graph has no permission attributed to it, no access should be allowed.
            assert!(!can_read_graph(path, client), "graph {path} should not be readable");
            assert!(!can_introspect_graph(path, client), "graph {path} should not be introspectable");
            assert!(!can_write_graph(path, client), "graph {path} should not be writable");
        }
    }
}

pub fn can_introspect_graph(path: &str, client: &RaphtoryGraphQLClient) -> bool {
    let query = format!(r#"query {{ graphMetadata(path: "{path}") {{ path nodeCount }} }}"#);
    let data = gql(&query, client)
        .unwrap_or_else(|e| panic!("Error executing query: {query}: {e}"));

    data.get("graphMetadata")
        .is_some_and(|metadata| !metadata.is_null())
}

pub fn can_read_graph(path: &str, client: &RaphtoryGraphQLClient) -> bool {
    let query = format!(r#"query {{ graph(path: "{path}") {{ path }} }}"#);
    let data = gql(&query, client)
        .unwrap_or_else(|e| panic!("Error executing query: {query}: {e}"));

    data.get("graph")
        .and_then(|graph| graph.get("path"))
        .and_then(JsonValue::as_str)
        .is_some_and(|graph_path| graph_path == path)
}

pub fn can_write_graph(path: &str, client: &RaphtoryGraphQLClient) -> bool {
    let query = format!(
        r#"query {{ updateGraph(path: "{path}") {{ addNode(time: 1, name: "test_node") {{ success }} }} }}"#
    );

    // Write requests are denied if the user does not have write access to the graph.
    // This is unlike read requests which return a successful but empty response if
    // the user does not have read access.
    let data = match gql(&query, client) {
        Ok(data) => data,
        Err(e) if e.to_string().contains("Access denied") => return false,
        Err(e) => panic!("Error executing query: {query}: {e}"),
    };

    data.get("updateGraph")
        .and_then(|update| update.get("addNode"))
        .and_then(|add_node| add_node.get("success"))
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
}

pub fn validate_namespace_grant(
    path: &str,
    permission: Option<Permission>,
    client: &RaphtoryGraphQLClient,
    num_children: usize,
) {
    match permission {
        Some(permission_enum) => {
            match permission_enum {
                Permission::Discover => {
                    assert!(can_discover_namespace(path, client), "namespace {path} should be discoverable");
                }
                Permission::Introspect => {
                    assert!(
                        can_introspect_namespace(path, num_children, client),
                        "namespace {path} should be introspectable"
                    );
                    assert!(!can_write_namespace(path, client), "namespace {path} should not be writable");
                }
                Permission::Read => {
                    assert!(can_introspect_namespace(path, num_children, client), "namespace {path} should be introspectable");
                    assert!(!can_write_namespace(path, client), "namespace {path} should not be writable");
                }
                Permission::Write => {
                    assert!(can_introspect_namespace(path, num_children, client), "namespace {path} should be introspectable");
                    assert!(can_write_namespace(path, client), "namespace {path} should be writable");
                }
            }
        }
        None => {
            // Namespace has no permission attributed to it, no access should be allowed.
            assert!(!can_discover_namespace(path, client), "namespace {path} should not be discoverable");
            assert!(
                !can_introspect_namespace(path, num_children, client),
                "namespace {path} should not be introspectable"
            );
        }
    }
}

/// Verify that the namespace at `path` appears in its parent listing.
pub fn can_discover_namespace(path: &str, client: &RaphtoryGraphQLClient) -> bool {
    let parent = path.rsplit_once('/').map_or("root", |(parent, _)| parent);

    let query = if parent == "root" {
        r#"query { root { children { list { path } } } }"#.to_string()
    } else {
        format!(r#"query {{ namespace(path: "{parent}") {{ children {{ list {{ path }} }} }} }}"#)
    };

    let data = gql(&query, client)
        .unwrap_or_else(|e| panic!("Error executing query: {query}: {e}"));

    let children = if parent == "root" {
        data.get("root")
    } else {
        data.get("namespace")
    };

    // Verify that the parent's children list is non-empty and contains this namespace.
    children
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
        })
}

// Verify that the namespace at `path` can have its listings browsed and that
// immediate graphs plus child namespaces match the expected `num_children`.
pub fn can_introspect_namespace(
    path: &str,
    num_children: usize,
    client: &RaphtoryGraphQLClient,
) -> bool {
    let query = format!(
        r#"query {{ namespace(path: "{path}") {{ graphs {{ list {{ path }} }} children {{ list {{ path }} }} }} }}"#
    );

    let data = match gql(&query, client) {
        Ok(data) => data,
        Err(e) if e.to_string().contains("Access denied") => return false,
        Err(e) => panic!("Error executing query: {query}: {e}"),
    };

    let Some(namespace) = data.get("namespace").filter(|v| !v.is_null()) else {
        return false;
    };

    let num_graphs = namespace
        .get("graphs")
        .and_then(|graphs| graphs.get("list"))
        .and_then(JsonValue::as_array)
        .map(|entries| entries.len())
        .unwrap_or(0);

    let num_namespaces = namespace
        .get("children")
        .and_then(|children| children.get("list"))
        .and_then(JsonValue::as_array)
        .map(|entries| entries.len())
        .unwrap_or(0);

    // INSTROSPECT implies all listings of `path` are visible.
    num_graphs + num_namespaces == num_children
}

/// Can create and delete graphs at `path`.
pub fn can_write_namespace(path: &str, client: &RaphtoryGraphQLClient) -> bool {
    if create_graph(path, client).is_err() {
        // Do not attempt delete if create fails.
        return false;
    }

    delete_graph(path, client).is_ok()
}
