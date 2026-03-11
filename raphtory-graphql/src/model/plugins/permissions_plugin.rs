use super::{
    operation::{NoOpPermissions, NoOpPermissionsQuery, Operation},
    RegisterFunction,
};
use crate::model::plugins::entry_point::EntryPoint;
use async_graphql::{dynamic::FieldValue, indexmap::IndexMap, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use std::{
    borrow::Cow,
    sync::{Mutex, MutexGuard},
};

pub static PERMISSIONS_MUTATIONS: Lazy<Mutex<IndexMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(IndexMap::new()));

pub static PERMISSIONS_QUERIES: Lazy<Mutex<IndexMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(IndexMap::new()));

#[derive(Clone, Default)]
pub struct PermissionsPlugin;

impl<'a> EntryPoint<'a> for PermissionsPlugin {
    fn predefined_operations() -> IndexMap<&'static str, RegisterFunction> {
        IndexMap::from([(
            "NoOps",
            Box::new(NoOpPermissions::register_operation) as RegisterFunction,
        )])
    }

    fn lock_plugins() -> MutexGuard<'static, IndexMap<String, RegisterFunction>> {
        PERMISSIONS_MUTATIONS.lock().unwrap()
    }
}

impl Register for PermissionsPlugin {
    fn register(registry: Registry) -> Registry {
        Self::register_operations(registry)
    }
}

impl TypeName for PermissionsPlugin {
    fn get_type_name() -> Cow<'static, str> {
        "PermissionsPlugin".into()
    }
}

impl OutputTypeName for PermissionsPlugin {}

impl<'a> ResolveOwned<'a> for PermissionsPlugin {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}

/// Read-only entry point for permissions queries (admin-gated via require_write_access_dynamic).
#[derive(Clone, Default)]
pub struct PermissionsQueryPlugin;

impl<'a> EntryPoint<'a> for PermissionsQueryPlugin {
    fn predefined_operations() -> IndexMap<&'static str, RegisterFunction> {
        IndexMap::from([(
            "NoOps",
            Box::new(NoOpPermissionsQuery::register_operation) as RegisterFunction,
        )])
    }

    fn lock_plugins() -> MutexGuard<'static, IndexMap<String, RegisterFunction>> {
        PERMISSIONS_QUERIES.lock().unwrap()
    }
}

impl Register for PermissionsQueryPlugin {
    fn register(registry: Registry) -> Registry {
        Self::register_operations(registry)
    }
}

impl TypeName for PermissionsQueryPlugin {
    fn get_type_name() -> Cow<'static, str> {
        "PermissionsQueryPlugin".into()
    }
}

impl OutputTypeName for PermissionsQueryPlugin {}

impl<'a> ResolveOwned<'a> for PermissionsQueryPlugin {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
