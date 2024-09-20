use crate::model::algorithms::RegisterFunction;
use async_graphql::dynamic::Object;
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use itertools::Itertools;
use std::{collections::HashMap, sync::MutexGuard};

pub trait MutationEntryPoint<'a>:
Register + TypeName + OutputTypeName + ResolveOwned<'a> + Sync
{
    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>>;
    fn register_mutations(registry: Registry) -> Registry {
        let mut registry = registry;
        let mut object = Object::new(Self::get_type_name());

        let mut plugins = Self::lock_plugins();
        let plugin_names = plugins.keys().cloned().collect_vec();
        for name in plugin_names {
            let register_algo = plugins.remove(&name).unwrap();
            (registry, object) = register_algo(&name, registry, object);
        }

        registry.register_type(object)
    }
}
