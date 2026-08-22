//! Interface for defining server extensions which can define new command-line arguments and config
//!
//! The interface comes in two parts, the `ServerPlugin` trait which defines the constructor for the
//! `ServerExtension` which defines the hook that is called during server initialisation.
//!
//!

use crate::{
    plugin::server::{internal::ServerPluginImpl, plugin::PluginRegistration},
    server::ServerError,
};
use indexmap::{map::Entry, IndexMap};
use once_cell::sync::Lazy;
use std::{
    iter::{IntoIterator, Iterator},
    ops::Deref,
    sync::Mutex,
};
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum PluginRegistrationError {
    #[error("Multiple cli plugins registered with name '{0}'")]
    Multiple(String),
    /// Extension settings sit at the top level beside the built-in sections, and the built-ins are
    /// matched first — by `AppConfigFieldName` on the json path, and by serde's named fields before
    /// `flatten` on the file path. A colliding name would therefore never receive its config.
    #[error("Extension '{0}' collides with a built-in config section and would never be configured")]
    ShadowsConfigSection(String),
    #[error("No registered plugin with name '{0}'")]
    Unknown(String),
}

static EXTENSIONS: Lazy<IndexMap<String, Box<dyn ServerPluginImpl>>> = Lazy::new(|| {
    let mut map = IndexMap::new();
    for registration in inventory::iter::<PluginRegistration> {
        let plugin = registration.0();
        let name = plugin.name();
        match map.entry(name) {
            Entry::Occupied(entry) => {
                // unrecoverable
                panic!("Multiple plugins registered with name {}", entry.key());
            }
            Entry::Vacant(entry) => {
                entry.insert(plugin);
            }
        }
    }
    map
});

fn get_plugin(name: &str) -> Result<&dyn ServerPluginImpl, PluginRegistrationError> {
    EXTENSIONS
        .get(name)
        .map(|plugin| plugin.as_ref())
        .ok_or_else(|| PluginRegistrationError::Unknown(name.to_string()))
}

fn get_plugins() -> impl Iterator<Item = &'static dyn ServerPluginImpl> {
    EXTENSIONS.values().map(|ext| ext.as_ref())
}

/// Whether a plugin with this name was registered at compile time. Lets a build report which
/// optional extensions it was compiled with.
pub(crate) fn is_registered(name: &str) -> bool {
    EXTENSIONS.contains_key(name)
}

pub mod extension;
pub mod plugin;

mod internal;
