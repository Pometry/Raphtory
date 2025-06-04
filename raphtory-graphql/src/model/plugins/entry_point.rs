use async_graphql::dynamic::Object;
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use itertools::Itertools;
use std::{collections::HashMap, sync::MutexGuard};

use super::RegisterFunction;

pub trait EntryPoint<'a>: Register + TypeName + OutputTypeName + ResolveOwned<'a> + Sync {
    fn predefined_operations() -> HashMap<&'static str, RegisterFunction>;

    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>>;

    fn register_operations(registry: Registry) -> Registry {
        let mut registry = registry;
        let mut object = Object::new(Self::get_type_name());

        for (name, register_operation) in Self::predefined_operations() {
            (registry, object) = register_operation(name, registry, object);
        }

        let mut plugins = Self::lock_plugins();
        let plugin_names = plugins.keys().cloned().collect_vec();
        for name in plugin_names {
            let register_operation = plugins.remove(&name).unwrap();
            (registry, object) = register_operation(&name, registry, object);
        }

        registry.register_type(object)
    }
}
