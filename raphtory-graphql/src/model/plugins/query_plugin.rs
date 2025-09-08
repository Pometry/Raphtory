use crate::model::plugins::{entry_point::EntryPoint, operation::Operation};
use async_graphql::{dynamic::FieldValue, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

use super::{operation::NoOpQuery, RegisterFunction};

pub static QUERY_PLUGINS: Lazy<Mutex<HashMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Default)]
pub struct QueryPlugin;

impl<'a> EntryPoint<'a> for QueryPlugin {
    fn predefined_operations() -> HashMap<&'static str, RegisterFunction> {
        HashMap::from([(
            "NoOps",
            Box::new(NoOpQuery::register_operation) as RegisterFunction,
        )])
    }

    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        QUERY_PLUGINS.lock().unwrap()
    }
}

impl Register for QueryPlugin {
    fn register(registry: Registry) -> Registry {
        Self::register_operations(registry)
    }
}

impl TypeName for QueryPlugin {
    fn get_type_name() -> Cow<'static, str> {
        "QueryPlugin".into()
    }
}

impl OutputTypeName for QueryPlugin {}

impl<'a> ResolveOwned<'a> for QueryPlugin {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
