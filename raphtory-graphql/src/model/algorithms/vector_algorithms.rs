use crate::model::algorithms::{
    query::Query, query_entry_point::QueryEntryPoint, similarity_search::SimilaritySearch,
    RegisterFunction,
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

pub struct VectorAlgorithms {
    pub graph: DynamicVectorisedGraph,
}

impl From<DynamicVectorisedGraph> for VectorAlgorithms {
    fn from(graph: DynamicVectorisedGraph) -> Self {
        Self { graph }
    }
}

impl<'a> QueryEntryPoint<'a> for VectorAlgorithms {
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

impl Register for VectorAlgorithms {
    fn register(registry: Registry) -> Registry {
        Self::register_queries(registry)
    }
}

impl TypeName for VectorAlgorithms {
    fn get_type_name() -> Cow<'static, str> {
        "VectorAlgorithms".into()
    }
}

impl OutputTypeName for VectorAlgorithms {}

impl<'a> ResolveOwned<'a> for VectorAlgorithms {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
