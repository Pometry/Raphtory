use crate::model::algorithms::RegisterFunction;
use async_graphql::dynamic::Object;
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use itertools::Itertools;
use std::{collections::HashMap, sync::MutexGuard};

pub trait AlgorithmEntryPoint<'a>:
    Register + TypeName + OutputTypeName + ResolveOwned<'a> + Sync
{
    fn predefined_algos() -> HashMap<&'static str, RegisterFunction>;
    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>>;
    fn register_algos(registry: Registry) -> Registry {
        let mut registry = registry;
        let mut object = Object::new(Self::get_type_name());

        for (name, register_algo) in Self::predefined_algos() {
            (registry, object) = register_algo(name, registry, object);
        }

        for (name, register_algo) in Self::lock_plugins().iter() {
            (registry, object) = register_algo(name, registry, object);
        }

        println!(
            "registering type {} with plugins: {:?}",
            Self::get_type_name(),
            Self::lock_plugins().keys().collect_vec()
        );

        println!(
            "input object is {} with content: {:?}",
            object.type_name(),
            object
        );

        registry.register_type(object)
    }
}
