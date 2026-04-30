use std::collections::HashMap;
use std::sync::{LazyLock, Once};
use std::time::Duration;

use raphtory::db::api::storage::storage::Config;
use raphtory_graphql::client::raphtory_client::RaphtoryGraphQLClient;
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

    let data = gql(&query, client);

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

    let data = gql(&query, client);

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

    let data = gql(&query, client);

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
    let _ = (path, client);
    match permission {
        Some(_) => {
            todo!()
        }
        None => {
            todo!()
        }
    }
}

pub fn validate_namespace_grant(
    path: &str,
    permission: Option<Permission>,
    client: &RaphtoryGraphQLClient,
) {
    let _ = (path, permission, client);
}

pub fn query_graph(path: &str, client: &RaphtoryGraphQLClient) -> HashMap<String, JsonValue> {
    let query = format!(r#"query {{ graph(path: "{path}") {{ path }} }}"#);
    gql(&query, client)
}

fn gql(query: &str, client: &RaphtoryGraphQLClient) -> HashMap<String, JsonValue> {
    RUNTIME
        .block_on(client.query(query, HashMap::new()))
        .unwrap_or_else(|e| panic!("Error executing query {query}: {e}"))
}
