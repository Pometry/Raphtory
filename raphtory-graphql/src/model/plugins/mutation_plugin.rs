use crate::model::{
    algorithms::RegisterFunction,
    plugins::{
        entry_point::EntryPoint,
        operation::{NoOpMutation, Operation},
    },
};
use async_graphql::{dynamic::FieldValue, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

pub static MUTATION_PLUGINS: Lazy<Mutex<HashMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Default)]
pub struct MutationPlugin {}

impl<'a> EntryPoint<'a> for MutationPlugin {
    fn predefined_operations() -> HashMap<&'static str, RegisterFunction> {
        HashMap::from([(
            "NoOps",
            Box::new(NoOpMutation::register_operation) as RegisterFunction,
        )])
    }

    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        MUTATION_PLUGINS.lock().unwrap()
    }
}

impl Register for MutationPlugin {
    fn register(registry: Registry) -> Registry {
        Self::register_operations(registry)
    }
}

impl TypeName for MutationPlugin {
    fn get_type_name() -> Cow<'static, str> {
        "MutationPlugin".into()
    }
}

impl OutputTypeName for MutationPlugin {}

impl<'a> ResolveOwned<'a> for MutationPlugin {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
