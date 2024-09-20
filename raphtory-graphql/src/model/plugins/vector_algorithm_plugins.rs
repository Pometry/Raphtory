use crate::model::{
    algorithms::{similarity_search::SimilaritySearch, RegisterFunction},
    plugins::{query::Query, query_entry_point::QueryEntryPoint},
};
use async_graphql::{dynamic::FieldValue, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use raphtory::vectors::vectorised_graph::DynamicVectorisedGraph;
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

pub static VECTOR_ALGO_PLUGINS: Lazy<Mutex<HashMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub struct VectorAlgorithmPlugins {
    pub graph: DynamicVectorisedGraph,
}

impl From<DynamicVectorisedGraph> for VectorAlgorithmPlugins {
    fn from(graph: DynamicVectorisedGraph) -> Self {
        Self { graph }
    }
}

impl<'a> QueryEntryPoint<'a> for VectorAlgorithmPlugins {
    fn predefined_queries() -> HashMap<&'static str, RegisterFunction> {
        HashMap::from([(
            "similaritySearch",
            Box::new(SimilaritySearch::register_query) as RegisterFunction,
        )])
    }

    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        VECTOR_ALGO_PLUGINS.lock().unwrap()
    }
}

impl Register for VectorAlgorithmPlugins {
    fn register(registry: Registry) -> Registry {
        Self::register_queries(registry)
    }
}

impl TypeName for VectorAlgorithmPlugins {
    fn get_type_name() -> Cow<'static, str> {
        "VectorAlgorithmPlugins".into()
    }
}

impl OutputTypeName for VectorAlgorithmPlugins {}

impl<'a> ResolveOwned<'a> for VectorAlgorithmPlugins {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
