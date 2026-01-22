use super::RegisterFunction;
use async_graphql::{dynamic::Object, indexmap::IndexMap};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use std::sync::MutexGuard;

pub trait MutationEntryPoint<'a>:
    Register + TypeName + OutputTypeName + ResolveOwned<'a> + Sync
{
    fn lock_plugins() -> MutexGuard<'static, IndexMap<String, RegisterFunction>>;

    fn register_mutations(registry: Registry) -> Registry {
        let mut registry = registry;
        let mut object = Object::new(Self::get_type_name());

        let mut plugins = Self::lock_plugins();
        for (name, register_algo) in plugins.drain(..) {
            (registry, object) = register_algo(&name, registry, object);
        }
        registry.register_type(object)
    }
}
