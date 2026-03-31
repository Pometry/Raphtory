use super::permissions_plugin::{
    PermissionsPlugin, PermissionsQueryPlugin, PERMISSIONS_MUT_ENTRYPOINT,
    PERMISSIONS_QRY_ENTRYPOINT,
};
use crate::auth::require_jwt_write_access_dynamic;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, TypeRef};
use dynamic_graphql::internal::{Register, Registry};
use std::sync::atomic::Ordering;

/// Conditionally adds the `permissions` field to the root Mutation type.
/// Only registers when `register_permissions_entrypoint()` has been called
/// (i.e., when RBAC is configured via `raphtory-auth::init()`).
pub struct PermissionsEntrypointMut;

/// Conditionally adds the `permissions` field to the root Query type.
/// Only registers when `register_permissions_query_entrypoint()` has been called.
pub struct PermissionsEntrypointQuery;

impl Register for PermissionsEntrypointMut {
    fn register(registry: Registry) -> Registry {
        if !PERMISSIONS_MUT_ENTRYPOINT.load(Ordering::SeqCst) {
            return registry;
        }
        let registry = registry.register::<PermissionsPlugin>();
        registry.update_object("MutRoot", "PermissionsEntrypointMut", |obj| {
            obj.field(Field::new(
                "permissions",
                TypeRef::named_nn("PermissionsPlugin"),
                |ctx| {
                    FieldFuture::new(async move {
                        require_jwt_write_access_dynamic(&ctx)?;
                        Ok(Some(FieldValue::owned_any(PermissionsPlugin::default())))
                    })
                },
            ))
        })
    }
}

impl Register for PermissionsEntrypointQuery {
    fn register(registry: Registry) -> Registry {
        if !PERMISSIONS_QRY_ENTRYPOINT.load(Ordering::SeqCst) {
            return registry;
        }
        let registry = registry.register::<PermissionsQueryPlugin>();
        registry.update_object("QueryRoot", "PermissionsEntrypointQuery", |obj| {
            obj.field(Field::new(
                "permissions",
                TypeRef::named_nn("PermissionsQueryPlugin"),
                |ctx| {
                    FieldFuture::new(async move {
                        require_jwt_write_access_dynamic(&ctx)?;
                        Ok(Some(FieldValue::owned_any(PermissionsQueryPlugin::default())))
                    })
                },
            ))
        })
    }
}
