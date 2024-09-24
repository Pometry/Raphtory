use crate::model::{
    algorithms::{global_search::GlobalSearch, RegisterFunction},
    plugins::{query::Query, query_entry_point::QueryEntryPoint},
};
use async_graphql::{dynamic::FieldValue, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use raphtory::{
    db::api::view::MaterializedGraph, search::IndexedGraph,
    vectors::vectorised_graph::DynamicVectorisedGraph,
};
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

pub static QUERY_PLUGINS: Lazy<Mutex<HashMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Default)]
pub struct QueryPlugin {
    pub graphs: Arc<RwLock<HashMap<String, IndexedGraph<MaterializedGraph>>>>,
    pub vectorised_graphs: Arc<RwLock<HashMap<String, DynamicVectorisedGraph>>>,
}

impl<'a> QueryEntryPoint<'a> for QueryPlugin {
    fn predefined_queries() -> HashMap<&'static str, RegisterFunction> {
        HashMap::from([(
            "globalSearch",
            Box::new(GlobalSearch::register_query) as RegisterFunction,
        )])
    }

    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        QUERY_PLUGINS.lock().unwrap()
    }
}

impl Register for QueryPlugin {
    fn register(registry: Registry) -> Registry {
        Self::register_queries(registry)
    }
}

impl TypeName for QueryPlugin {
    fn get_type_name() -> Cow<'static, str> {
        "QueryPlugins".into()
    }
}

impl OutputTypeName for QueryPlugin {}

impl<'a> ResolveOwned<'a> for QueryPlugin {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
