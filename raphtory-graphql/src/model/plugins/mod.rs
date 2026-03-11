use async_graphql::dynamic::Object;
use dynamic_graphql::internal::Registry;

pub mod algorithms;
pub mod entry_point;
pub mod graph_algorithm_plugin;
pub mod mutation_entry_point;
pub mod mutation_plugin;
pub mod operation;
pub mod permissions_plugin;
pub mod query_entry_point;
pub mod query_plugin;

pub type RegisterFunction = Box<dyn FnOnce(&str, Registry, Object) -> (Registry, Object) + Send>;

/// Register an operation into the `PermissionsPlugin` entry point (mutation root).
/// Call this before `GraphServer::run()` / `create_schema()`.
pub fn register_permissions_mutation<O>(name: &'static str)
where
    O: for<'a> operation::Operation<'a, permissions_plugin::PermissionsPlugin> + 'static,
{
    permissions_plugin::PERMISSIONS_MUTATIONS
        .lock()
        .unwrap()
        .insert(name.to_string(), Box::new(O::register_operation));
}

/// Register an operation into the `PermissionsQueryPlugin` entry point (query root).
/// Ops registered here must call `require_write_access_dynamic` themselves since they
/// are not covered by the `MutationAuth` extension.
pub fn register_permissions_query<O>(name: &'static str)
where
    O: for<'a> operation::Operation<'a, permissions_plugin::PermissionsQueryPlugin> + 'static,
{
    permissions_plugin::PERMISSIONS_QUERIES
        .lock()
        .unwrap()
        .insert(name.to_string(), Box::new(O::register_operation));
}
