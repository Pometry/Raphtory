use crate::model::plugins::PLUGINS;
use dyn_clone::{clone_trait_object, DynClone};
use dynamic_graphql::internal::Registry;

pub trait RegisterPlugin: DynClone + Send + Sync + 'static {
    fn register(&self, registry: Registry) -> Registry;
}

clone_trait_object!(RegisterPlugin);

/// Register a global plugin to extend the graphql schema
pub fn register_schema_plugin(plugin: impl RegisterPlugin) {
    PLUGINS
        .lock()
        .expect("Plugin registration lock poisoned")
        .push(Box::new(plugin))
}

/// Clear all global schema plugins
pub fn clear_schema_plugins() {
    PLUGINS
        .lock()
        .expect("Plugin registration lock poisoned")
        .clear();
}
