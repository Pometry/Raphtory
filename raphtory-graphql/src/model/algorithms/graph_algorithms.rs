use crate::model::algorithms::{
    query::{Pagerank, Query},
    query_entry_point::QueryEntryPoint,
    RegisterFunction,
};
use async_graphql::{dynamic::FieldValue, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use raphtory::db::api::view::DynamicGraph;
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

use super::query::ShortestPath;

pub static GRAPH_ALGO_PLUGINS: Lazy<Mutex<HashMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub struct GraphAlgorithms {
    pub graph: DynamicGraph,
}

impl From<DynamicGraph> for GraphAlgorithms {
    fn from(graph: DynamicGraph) -> Self {
        Self { graph }
    }
}

impl<'a> QueryEntryPoint<'a> for GraphAlgorithms {
    fn predefined_queries() -> HashMap<&'static str, RegisterFunction> {
        HashMap::from([
            (
                "pagerank",
                Box::new(Pagerank::register_query) as RegisterFunction,
            ),
            (
                "shortest_path",
                Box::new(ShortestPath::register_query) as RegisterFunction,
            ),
        ])
    }

    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        GRAPH_ALGO_PLUGINS.lock().unwrap()
    }
}

impl Register for GraphAlgorithms {
    fn register(registry: Registry) -> Registry {
        Self::register_queries(registry)
    }
}

impl TypeName for GraphAlgorithms {
    fn get_type_name() -> Cow<'static, str> {
        "GraphAlgorithms".into()
    }
}

impl OutputTypeName for GraphAlgorithms {}

impl<'a> ResolveOwned<'a> for GraphAlgorithms {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
