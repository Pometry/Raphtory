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

pub mod extension;
pub mod plugin;

mod internal;
