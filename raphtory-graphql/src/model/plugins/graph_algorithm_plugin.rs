use crate::model::plugins::entry_point::EntryPoint;
use async_graphql::{dynamic::FieldValue, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use raphtory::db::api::view::DynamicGraph;
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

use super::{
    algorithms::{Pagerank, ShortestPath},
    operation::Operation,
    RegisterFunction,
};

pub static GRAPH_ALGO_PLUGINS: Lazy<Mutex<HashMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub struct GraphAlgorithmPlugin {
    pub graph: DynamicGraph,
}

impl From<DynamicGraph> for GraphAlgorithmPlugin {
    fn from(graph: DynamicGraph) -> Self {
        Self { graph }
    }
}

impl<'a> EntryPoint<'a> for GraphAlgorithmPlugin {
    fn predefined_operations() -> HashMap<&'static str, RegisterFunction> {
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

impl Register for GraphAlgorithmPlugin {
    fn register(registry: Registry) -> Registry {
        Self::register_operations(registry)
    }
}

impl TypeName for GraphAlgorithmPlugin {
    fn get_type_name() -> Cow<'static, str> {
        "GraphAlgorithmPlugin".into()
    }
}

impl OutputTypeName for GraphAlgorithmPlugin {}

impl<'a> ResolveOwned<'a> for GraphAlgorithmPlugin {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
