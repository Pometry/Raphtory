use crate::model::algorithms::{mutation_entry_point::MutationEntryPoint, RegisterFunction};
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
pub struct MutationPlugins {}

impl<'a> MutationEntryPoint<'a> for MutationPlugins {
    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        MUTATION_PLUGINS.lock().unwrap()
    }
}

impl Register for MutationPlugins {
    fn register(registry: Registry) -> Registry {
        Self::register_mutations(registry)
    }
}

impl TypeName for MutationPlugins {
    fn get_type_name() -> Cow<'static, str> {
        "MutationPlugins".into()
    }
}

impl OutputTypeName for MutationPlugins {}

impl<'a> ResolveOwned<'a> for MutationPlugins {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
