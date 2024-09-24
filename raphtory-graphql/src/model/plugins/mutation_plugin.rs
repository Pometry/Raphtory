use crate::model::{
    algorithms::RegisterFunction,
    plugins::{
        mutation_entry_point::MutationEntryPoint,
        operation::{NoOpMutation, Operation},
    },
};
use async_graphql::{dynamic::FieldValue, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use std::{
    borrow::Cow,
    collections::HashMap,
    string::ToString,
    sync::{Mutex, MutexGuard},
};

pub static MUTATION_PLUGINS: Lazy<Mutex<HashMap<String, RegisterFunction>>> = Lazy::new(|| {
    Mutex::new(HashMap::from([(
        "NoOps".to_string(),
        Box::new(NoOpMutation::register_operation) as RegisterFunction,
    )]))
});

#[derive(Clone, Default)]
pub struct MutationPlugin {}

impl<'a> MutationEntryPoint<'a> for MutationPlugin {
    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        MUTATION_PLUGINS.lock().unwrap()
    }
}

impl Register for MutationPlugin {
    fn register(registry: Registry) -> Registry {
        Self::register_mutations(registry)
    }
}

impl TypeName for MutationPlugin {
    fn get_type_name() -> Cow<'static, str> {
        "MutationPlugins".into()
    }
}

impl OutputTypeName for MutationPlugin {}

impl<'a> ResolveOwned<'a> for MutationPlugin {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
