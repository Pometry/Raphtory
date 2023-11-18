use crate::{
    data::DynamicVectorisedGraph,
    model::algorithms::{
        algorithm::Algorithm, algorithm_entry_point::AlgorithmEntryPoint,
        similarity_search::SimilaritySearch, RegisterFunction,
    },
};
use async_graphql::{dynamic::FieldValue, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
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

impl<'a> AlgorithmEntryPoint<'a> for VectorAlgorithms {
    fn predefined_algos() -> HashMap<&'static str, RegisterFunction> {
        HashMap::from([(
            "similaritySearch",
            SimilaritySearch::register_algo as RegisterFunction,
        )])
    }
    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        VECTOR_ALGO_PLUGINS.lock().unwrap()
    }
}

impl Register for VectorAlgorithms {
    fn register(registry: Registry) -> Registry {
        Self::register_algos(registry)
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
