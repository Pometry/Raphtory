use crate::{
    graph::GraphWithVectors,
    model::algorithms::{
        algorithm::Algorithm, algorithm_entry_point::AlgorithmEntryPoint,
        global_search::GlobalSearch, RegisterFunction,
    },
};
use async_graphql::{dynamic::FieldValue, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use raphtory::{
    db::api::view::MaterializedGraph,
    vectors::{vectorised_graph::VectorisedGraph, EmbeddingFunction},
};
use std::{
    borrow::Cow,
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
};

pub static GLOBAL_PLUGINS: Lazy<Mutex<HashMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
pub struct GlobalPlugins {
    pub graphs: Arc<HashMap<String, VectorisedGraph<MaterializedGraph>>>,
}

impl<'a> AlgorithmEntryPoint<'a> for GlobalPlugins {
    fn predefined_algos() -> HashMap<&'static str, RegisterFunction> {
        HashMap::from([(
            "globalSearch",
            Box::new(GlobalSearch::register_algo) as RegisterFunction,
        )])
    }
    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        GLOBAL_PLUGINS.lock().unwrap()
    }
}

impl Register for GlobalPlugins {
    fn register(registry: Registry) -> Registry {
        Self::register_algos(registry)
    }
}
impl TypeName for GlobalPlugins {
    fn get_type_name() -> Cow<'static, str> {
        "GlobalPlugins".into()
    }
}
impl OutputTypeName for GlobalPlugins {}
impl<'a> ResolveOwned<'a> for GlobalPlugins {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
