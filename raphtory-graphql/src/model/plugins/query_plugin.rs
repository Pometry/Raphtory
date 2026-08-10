use super::{operation::NoOpQuery, RegisterFunction};
use crate::model::plugins::{entry_point::EntryPoint, operation::Operation};
use async_graphql::{dynamic::FieldValue, indexmap::IndexMap, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use std::{
    borrow::Cow,
    sync::{Mutex, MutexGuard},
};

static PLUGINS: Lazy<Mutex<Vec<Box<dyn RegisterPlugin>>>> = Lazy::new(|| Mutex::new(Vec::new()));

#[derive(Clone, Default)]
pub struct Plugins;

pub trait RegisterPlugin: Send + 'static {
    fn register(&self, registry: Registry) -> Registry;
}

/// Register a plugin to extend the graphql schema
pub fn register_plugin(plugin: impl RegisterPlugin) {
    PLUGINS
        .lock()
        .expect("Plugin registration lock poisoned")
        .push(Box::new(plugin))
}

/// Clear all schema plugins
pub fn clear_plugins() {
    PLUGINS
        .lock()
        .expect("Plugin registration lock poisoned")
        .clear();
}

impl Register for Plugins {
    fn register(mut registry: Registry) -> Registry {
        let plugins = PLUGINS.lock().expect("Plugin registration lock poisoned");
        for plugin in plugins.iter() {
            registry = plugin.register(registry);
        }
        registry
    }
}
