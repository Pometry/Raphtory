use crate::plugin::server::{extension::ServerExtension, EXTENSIONS};
use serde::{de::DeserializeOwned, Serialize};

/// Interface for defining a command-line plugin. This only defines the constructor for the actual
/// plugin, the actual parsing is defined by `Self::Extension`.
pub trait ServerPlugin: Send + Sync + 'static {
    type Extension: ServerExtension + clap::Args + Clone + Serialize + DeserializeOwned;

    /// Create a new plugin instance

    fn new(&self) -> Self::Extension;
}

/// Register a command-line plugin. This injects the command line arguments defined by the plugin
/// and enables reading of the plugin configuration from the config file
pub fn register_cli_plugin(plugin: impl ServerPlugin) {
    EXTENSIONS
        .lock()
        .expect("plugin lock poisoned")
        .insert(plugin.new().name().to_string(), Box::new(plugin));
}
