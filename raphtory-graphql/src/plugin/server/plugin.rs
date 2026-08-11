use crate::plugin::server::{extension::ServerExtension, EXTENSIONS};
use serde::{de::DeserializeOwned, Serialize};

pub trait ServerPlugin: Send + Sync + 'static {
    type Extension: ServerExtension + clap::Args + Clone + Serialize + DeserializeOwned;

    fn new(&self) -> Self::Extension;
}

pub fn register_cli_plugin(plugin: impl ServerPlugin) {
    EXTENSIONS
        .lock()
        .expect("plugin lock poisoned")
        .insert(plugin.new().name().to_string(), Box::new(plugin));
}
