use super::permissions_plugin::{
    PermissionsPlugin, PermissionsQueryPlugin, PERMISSIONS_MUTATIONS, PERMISSIONS_MUT_ENTRYPOINT,
    PERMISSIONS_QRY_ENTRYPOINT, PERMISSIONS_QUERIES,
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
///
/// The entrypoint itself is not admin-gated: each query field under it enforces
/// its own access check. Admin-only fields (`listRoles`, `getRole`) require write
/// access, while `myPermissions` returns only the caller's own grants and is
/// therefore reachable by any authenticated caller.
pub struct PermissionsEntrypointQuery;

impl Register for PermissionsEntrypointMut {
    fn register(registry: Registry) -> Registry {
        // Registering the plugin drains its op map, so gate on whether this
        // schema build actually has ops to attach — not just the process-global
        // "RBAC was configured" flag, which stays set. Without the emptiness
        // check, a store-less server created after a store-backed one in the
        // same process would declare an empty `PermissionsPlugin` and fail to
        // load its schema.
        if !PERMISSIONS_MUT_ENTRYPOINT.load(Ordering::SeqCst)
            || PERMISSIONS_MUTATIONS.lock().unwrap().is_empty()
        {
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
                        Ok(Some(FieldValue::owned_any(PermissionsPlugin)))
                    })
                },
            ))
        })
    }
}

impl Register for PermissionsEntrypointQuery {
    fn register(registry: Registry) -> Registry {
        // See PermissionsEntrypointMut: gate on there being ops to attach, so a
        // store-less server after a store-backed one doesn't declare an empty
        // `PermissionsQueryPlugin` (the op map is drained on registration).
        if !PERMISSIONS_QRY_ENTRYPOINT.load(Ordering::SeqCst)
            || PERMISSIONS_QUERIES.lock().unwrap().is_empty()
        {
            return registry;
        }
        let registry = registry.register::<PermissionsQueryPlugin>();
        registry.update_object("QueryRoot", "PermissionsEntrypointQuery", |obj| {
            obj.field(Field::new(
                "permissions",
                TypeRef::named_nn("PermissionsQueryPlugin"),
                |_ctx| {
                    FieldFuture::new(async move {
                        // Access is enforced per-field: admin-only fields check
                        // write access themselves; `myPermissions` is self-scoped.
                        Ok(Some(FieldValue::owned_any(
                            PermissionsQueryPlugin::default(),
                        )))
                    })
                },
            ))
        })
    }
}
