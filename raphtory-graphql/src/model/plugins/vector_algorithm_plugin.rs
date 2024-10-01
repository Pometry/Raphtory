use crate::model::{
    algorithms::{similarity_search::SimilaritySearch, RegisterFunction},
    plugins::{entry_point::EntryPoint, operation::Operation},
};
use async_graphql::{dynamic::FieldValue, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use raphtory::{db::api::view::MaterializedGraph, vectors::vectorised_graph::VectorisedGraph};
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

pub static VECTOR_ALGO_PLUGINS: Lazy<Mutex<HashMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub struct VectorAlgorithmPlugin {
    pub graph: VectorisedGraph<MaterializedGraph>,
}

impl From<VectorisedGraph<MaterializedGraph>> for VectorAlgorithmPlugin {
    fn from(graph: VectorisedGraph<MaterializedGraph>) -> Self {
        Self { graph }
    }
}

impl<'a> EntryPoint<'a> for VectorAlgorithmPlugin {
    fn predefined_operations() -> HashMap<&'static str, RegisterFunction> {
        HashMap::from([(
            "similaritySearch",
            Box::new(SimilaritySearch::register_operation) as RegisterFunction,
        )])
    }

    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        VECTOR_ALGO_PLUGINS.lock().unwrap()
    }
}

impl Register for VectorAlgorithmPlugin {
    fn register(registry: Registry) -> Registry {
        Self::register_operations(registry)
    }
}

impl TypeName for VectorAlgorithmPlugin {
    fn get_type_name() -> Cow<'static, str> {
        "VectorAlgorithmPlugin".into()
    }
}

impl OutputTypeName for VectorAlgorithmPlugin {}

impl<'a> ResolveOwned<'a> for VectorAlgorithmPlugin {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
