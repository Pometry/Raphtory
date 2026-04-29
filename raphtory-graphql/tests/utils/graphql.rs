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

use crate::utils::strategy::{Permission, PermissionGrant};

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

fn gql(client: &RaphtoryGraphQLClient, query: &str) -> HashMap<String, JsonValue> {
    RUNTIME
        .block_on(client.query(query, HashMap::new()))
        .unwrap_or_else(|e| panic!("Error executing query {query}: {e}"))
}

pub fn create_role(client: &RaphtoryGraphQLClient, name: &str) {
    let query =
        format!(r#"mutation {{ permissions {{ createRole(name: "{name}") {{ success }} }} }}"#);

    let data = gql(client, &query);

    let success = data
        .get("permissions")
        .and_then(|p| p.get("createRole"))
        .and_then(|c| c.get("success"))
        .and_then(JsonValue::as_bool);

    assert_eq!(success, Some(true), "createRole {name} data: {data:?}");
}

/// Create a graph on the graphql server using the given path.
pub fn create_graph(client: &RaphtoryGraphQLClient, path: &str) {
    RUNTIME.block_on(client.new_graph(path, "EVENT")).unwrap();
}

pub fn create_grant(client: &RaphtoryGraphQLClient, grant: &PermissionGrant) {
    match grant {
        PermissionGrant::Graph { user_id, path, permission } => {
            let role = format!("user_{user_id}");
            grant_graph(client, &role, path, *permission);
        }
        PermissionGrant::Namespace { user_id, path, permission } => {
            let role = format!("user_{user_id}");
            grant_namespace(client, &role, path, *permission);
        }
    }
}

pub fn grant_graph(
    client: &RaphtoryGraphQLClient,
    role: &str,
    path: &str,
    permission: Permission,
) {
    let query = format!(
        r#"mutation {{ permissions {{ grantGraph(role: "{role}", path: "{path}", permission: {}) {{ success }} }} }}"#,
        permission.as_str()
    );

    let data = gql(client, &query);

    let success = data
        .get("permissions")
        .and_then(|p| p.get("grantGraph"))
        .and_then(|c| c.get("success"))
        .and_then(JsonValue::as_bool);

    assert_eq!(
        success,
        Some(true),
        "grantGraph role={role} path={path} permission={} data: {data:?}",
        permission.as_str()
    );
}

pub fn grant_namespace(
    client: &RaphtoryGraphQLClient,
    role: &str,
    path: &str,
    permission: Permission,
) {
    let query = format!(
        r#"mutation {{ permissions {{ grantNamespace(role: "{role}", path: "{path}", permission: {}) {{ success }} }} }}"#,
        permission.as_str()
    );

    let data = gql(client, &query);

    let success = data
        .get("permissions")
        .and_then(|p| p.get("grantNamespace"))
        .and_then(|c| c.get("success"))
        .and_then(JsonValue::as_bool);

    assert_eq!(
        success,
        Some(true),
        "grantNamespace role={role} path={path} permission={} data: {data:?}",
        permission.as_str()
    );
}
