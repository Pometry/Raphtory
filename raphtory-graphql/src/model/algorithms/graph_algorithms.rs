use crate::model::algorithms::{
    algorithm::{Algorithm, Pagerank},
    algorithm_entry_point::AlgorithmEntryPoint,
    RegisterFunction,
};
use async_graphql::{dynamic::FieldValue, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use raphtory::db::api::view::internal::DynamicGraph;
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

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

impl<'a> AlgorithmEntryPoint<'a> for GraphAlgorithms {
    fn predefined_algos() -> HashMap<&'static str, RegisterFunction> {
        HashMap::from([("pagerank", Pagerank::register_algo as RegisterFunction)])
    }
    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        GRAPH_ALGO_PLUGINS.lock().unwrap()
    }
}

impl Register for GraphAlgorithms {
    fn register(registry: Registry) -> Registry {
        Self::register_algos(registry)
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
