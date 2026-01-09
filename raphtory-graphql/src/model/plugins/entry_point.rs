use super::RegisterFunction;
use async_graphql::{dynamic::Object, indexmap::IndexMap};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use std::sync::MutexGuard;

pub trait EntryPoint<'a>: Register + TypeName + OutputTypeName + ResolveOwned<'a> + Sync {
    fn predefined_operations() -> IndexMap<&'static str, RegisterFunction>;

    fn lock_plugins() -> MutexGuard<'static, IndexMap<String, RegisterFunction>>;

    fn register_operations(registry: Registry) -> Registry {
        let mut registry = registry;
        let mut object = Object::new(Self::get_type_name());

        for (name, register_operation) in Self::predefined_operations() {
            (registry, object) = register_operation(name, registry, object);
        }

        let mut plugins = Self::lock_plugins();
        for (name, operation) in plugins.drain(..) {
            (registry, object) = operation(&name, registry, object);
        }
        registry.register_type(object)
    }
}
