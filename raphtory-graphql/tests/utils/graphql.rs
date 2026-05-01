use std::collections::HashMap;
use std::sync::{LazyLock, Once};
use std::time::Duration;

use raphtory::db::api::storage::storage::Config;
use raphtory_graphql::client::{raphtory_client::RaphtoryGraphQLClient, ClientError};
use raphtory_graphql::config::app_config::AppConfigBuilder;
use raphtory_graphql::server::{apply_server_extension, GraphServer, RunningGraphServer};
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use url::Url;

use crate::utils::strategy::{GrantType, Permission, PermissionGrant};

static AUTH_INIT: Once = Once::new();

fn init_raphtory_auth() {
    AUTH_INIT.call_once(::auth::init);
}

pub static RUNTIME: LazyLock<Runtime> =
    LazyLock::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

pub fn get_client(url: Url, token: String) -> RaphtoryGraphQLClient {
    RUNTIME
        .block_on(RaphtoryGraphQLClient::connect(url, Some(token)))
        .expect("connect GraphQL client")
}

pub fn start_server(port: u16, pub_key: &str) -> (RunningGraphServer, TempDir) {
    init_raphtory_auth();

    RUNTIME.block_on(async {
        let mut tempdir = TempDir::new().unwrap();

        // Prevent cleanup so server can flush to workdir after drop.
        tempdir.disable_cleanup(true);

        let work_dir = tempdir.path().to_path_buf();
        let permissions_path = work_dir.join("permissions.json");

        let app_config = AppConfigBuilder::new()
            .with_auth_public_key(Some(pub_key.to_string()))
            .expect("test auth public key")
            .build();

        let server = GraphServer::new(work_dir, Some(app_config), None, Config::default())
            .await
            .unwrap();

        // Configure permissions store.
        let server = apply_server_extension(server, Some(permissions_path.as_path()));
        let server = server.start_with_port(port).await.unwrap();

        // Wait for server to start.
        tokio::time::sleep(Duration::from_secs(1)).await;

        (server, tempdir)
    })
}

/// Create a graph on the graphql server using the given path.
pub fn create_graph(path: &str, client: &RaphtoryGraphQLClient) {
    RUNTIME.block_on(client.new_graph(path, "EVENT")).unwrap();
}

pub fn create_role(name: &str, client: &RaphtoryGraphQLClient) {
    let query =
        format!(r#"mutation {{ permissions {{ createRole(name: "{name}") {{ success }} }} }}"#);

    let data = gql(&query, client)
        .unwrap_or_else(|e| panic!("Error executing query: {query}: {e}"));

    let success = data
        .get("permissions")
        .and_then(|p| p.get("createRole"))
        .and_then(|c| c.get("success"))
        .and_then(JsonValue::as_bool);

    assert_eq!(success, Some(true), "createRole {name} data: {data:?}");
}

pub fn create_grant(grant: &PermissionGrant, client: &RaphtoryGraphQLClient) {
    let role = format!("user_{}", grant.user_id);

    match grant.grant_type {
        GrantType::Graph => grant_graph(&grant.path, &role, grant.permission, client),
        GrantType::Namespace => grant_namespace(&grant.path, &role, grant.permission, client),
    }
}

pub fn grant_graph(
    path: &str,
    role: &str,
    permission: Permission,
    client: &RaphtoryGraphQLClient,
) {
    let query = format!(
        r#"mutation {{ permissions {{ grantGraph(role: "{role}", path: "{path}", permission: {}) {{ success }} }} }}"#,
        permission
    );

    let data = gql(&query, client)
        .unwrap_or_else(|e| panic!("Error executing query: {query}: {e}"));

    let success = data
        .get("permissions")
        .and_then(|p| p.get("grantGraph"))
        .and_then(|c| c.get("success"))
        .and_then(JsonValue::as_bool);

    assert_eq!(
        success,
        Some(true),
        "grantGraph role={role} path={path} permission={permission} data: {data:?}"
    );
}

pub fn grant_namespace(
    path: &str,
    role: &str,
    permission: Permission,
    client: &RaphtoryGraphQLClient,
) {
    let query = format!(
        r#"mutation {{ permissions {{ grantNamespace(role: "{role}", path: "{path}", permission: {}) {{ success }} }} }}"#,
        permission
    );

    let data = gql(&query, client)
        .unwrap_or_else(|e| panic!("Error executing query: {query}: {e}"));

    let success = data
        .get("permissions")
        .and_then(|p| p.get("grantNamespace"))
        .and_then(|c| c.get("success"))
        .and_then(JsonValue::as_bool);

    assert_eq!(
        success,
        Some(true),
        "grantNamespace role={role} path={path} permission={permission} data: {data:?}"
    );
}

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
    // the graph does not exist.
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
) {
    match permission {
        Some(permission_enum) => {
            match permission_enum {
                Permission::Discover => {
                    assert!(can_discover_namespace(path, client), "namespace {path} should be discoverable");
                }
                Permission::Introspect => {
                    assert!(can_introspect_namespace(path, client), "namespace {path} should be introspectable");
                }
                _ => {
                    // Ignore READ and WRITE permissions since they are propagated down to graphs
                    // and are tested separately.
                }
            }
        }
        None => {
            // Namespace has no permission attributed to it, no access should be allowed.
            assert!(!can_discover_namespace(path, client), "namespace {path} should not be discoverable");
            assert!(!can_introspect_namespace(path, client), "namespace {path} should not be introspectable");
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

// Verify that the namespace at `path` can have its listings browsed.
pub fn can_introspect_namespace(path: &str, client: &RaphtoryGraphQLClient) -> bool {
    let query = format!(
        r#"query {{ namespace(path: "{path}") {{ graphs {{ list {{ path }} }} children {{ list {{ path }} }} }} }}"#
    );

    let data = gql(&query, client)
        .unwrap_or_else(|e| panic!("Error executing query: {query}: {e}"));

    let namespace = data.get("namespace");

    let has_graphs_list = namespace
        .and_then(|ns| ns.get("graphs"))
        .and_then(|graphs| graphs.get("list"))
        .and_then(JsonValue::as_array)
        .is_some_and(|entries| !entries.is_empty());

    let has_children_list = namespace
        .and_then(|ns| ns.get("children"))
        .and_then(|children| children.get("list"))
        .and_then(JsonValue::as_array)
        .is_some_and(|entries| !entries.is_empty());

    has_graphs_list || has_children_list
}

fn gql(
    query: &str,
    client: &RaphtoryGraphQLClient,
) -> Result<HashMap<String, JsonValue>, ClientError> {
    RUNTIME.block_on(client.query(query, HashMap::new()))
}
