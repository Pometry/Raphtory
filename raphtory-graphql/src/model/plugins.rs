use crate::plugin::schema::RegisterPlugin;
use dynamic_graphql::internal::{Register, Registry};
use once_cell::sync::Lazy;
use std::sync::Mutex;

pub(crate) static PLUGINS: Lazy<Mutex<Vec<Box<dyn RegisterPlugin>>>> =
    Lazy::new(|| Mutex::new(Vec::new()));

#[derive(Clone, Default)]
pub struct Plugins;

impl Register for Plugins {
    fn register(mut registry: Registry) -> Registry {
        let plugins = PLUGINS.lock().expect("Plugin registration lock poisoned");
        for plugin in plugins.iter() {
            registry = plugin.register(registry);
        }
        registry
    }
}
