//! A build in which two server plugins register under the same name must be refused at startup,
//! not silently resolved to one of them (whose flags and config the other would then shadow).
//!
//! This is its own test binary on purpose: `register_cli_plugin!` submits to a process-global
//! `inventory`, so the two colliding registrations below exist only here and cannot affect any
//! other test binary.

use raphtory_graphql::{
    config::app_config::AppConfig,
    plugin::server::{extension::ServerExtension, plugin::ServerPlugin},
    register_cli_plugin,
    server::ServerError,
    GraphServer,
};
use serde_json::{to_value, Value};
use tempfile::TempDir;

/// A do-nothing extension whose only relevant trait is its name.
#[derive(clap::Args, Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct Named;

impl ServerExtension for Named {
    fn apply(&self, server: GraphServer) -> Result<GraphServer, ServerError> {
        Ok(server)
    }
    fn name(&self) -> &str {
        "clash"
    }
    fn update_from_json(&mut self, _value: &Value) -> Result<(), ServerError> {
        Ok(())
    }
    fn to_json(&self) -> Result<Value, ServerError> {
        to_value(self).map_err(ServerError::config_error)
    }
}

#[derive(Copy, Clone)]
struct PluginOne;
impl ServerPlugin for PluginOne {
    type Extension = Named;
    fn new(&self) -> Self::Extension {
        Named
    }
}

#[derive(Copy, Clone)]
struct PluginTwo;
impl ServerPlugin for PluginTwo {
    type Extension = Named;
    fn new(&self) -> Self::Extension {
        Named
    }
}

// Two plugins, one name: the collision this test is about.
register_cli_plugin!(PluginOne);
register_cli_plugin!(PluginTwo);

#[tokio::test]
#[should_panic(expected = "Multiple cli plugins registered with name 'clash'")]
async fn duplicate_plugin_names_are_refused_at_startup() {
    let work_dir = TempDir::new().unwrap();
    let _ = GraphServer::new(
        work_dir.path().to_path_buf(),
        Some(AppConfig::default()),
        Default::default(),
    )
    .await;
}
