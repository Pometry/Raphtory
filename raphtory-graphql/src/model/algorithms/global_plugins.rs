use crate::{
    data::DynamicVectorisedGraph,
    model::algorithms::{
        algorithm::{Algorithm, Pagerank},
        algorithm_entry_point::AlgorithmEntryPoint,
        global_search::GlobalSearch,
        RegisterFunction,
    },
};
use async_graphql::{dynamic::FieldValue, Context};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use raphtory::{db::api::view::MaterializedGraph, search::IndexedGraph};
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

pub static GLOBAL_PLUGINS: Lazy<Mutex<HashMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub struct GlobalPlugins {
    pub graphs: Arc<RwLock<HashMap<String, IndexedGraph<MaterializedGraph>>>>,
    pub vectorised_graphs: Arc<RwLock<HashMap<String, DynamicVectorisedGraph>>>,
}

impl<'a> AlgorithmEntryPoint<'a> for GlobalPlugins {
    fn predefined_algos() -> HashMap<&'static str, RegisterFunction> {
        // HashMap::from([("pagerank", Pagerank::register_algo as RegisterFunction)])
        HashMap::from([(
            "globalSearch",
            GlobalSearch::register_algo as RegisterFunction,
        )])
    }
    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        crate::model::algorithms::graph_algorithms::GRAPH_ALGO_PLUGINS
            .lock()
            .unwrap()
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
