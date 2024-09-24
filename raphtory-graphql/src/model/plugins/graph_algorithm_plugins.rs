use crate::model::{
    algorithms::{
        algorithms::{Pagerank, ShortestPath},
        RegisterFunction,
    },
    plugins::query_entry_point::QueryEntryPoint,
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
use crate::model::plugins::operation::Operation;

pub static GRAPH_ALGO_PLUGINS: Lazy<Mutex<HashMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub struct GraphAlgorithmPlugins {
    pub graph: DynamicGraph,
}

impl From<DynamicGraph> for GraphAlgorithmPlugins {
    fn from(graph: DynamicGraph) -> Self {
        Self { graph }
    }
}

impl<'a> QueryEntryPoint<'a> for GraphAlgorithmPlugins {
    fn predefined_queries() -> HashMap<&'static str, RegisterFunction> {
        HashMap::from([
            (
                "pagerank",
                Box::new(Pagerank::register_operation) as RegisterFunction,
            ),
            (
                "shortest_path",
                Box::new(ShortestPath::register_operation) as RegisterFunction,
            ),
        ])
    }

    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        GRAPH_ALGO_PLUGINS.lock().unwrap()
    }
}

impl Register for GraphAlgorithmPlugins {
    fn register(registry: Registry) -> Registry {
        Self::register_queries(registry)
    }
}

impl TypeName for GraphAlgorithmPlugins {
    fn get_type_name() -> Cow<'static, str> {
        "GraphAlgorithmPlugins".into()
    }
}

impl OutputTypeName for GraphAlgorithmPlugins {}

impl<'a> ResolveOwned<'a> for GraphAlgorithmPlugins {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
