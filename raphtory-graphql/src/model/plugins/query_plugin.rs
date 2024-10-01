use crate::{
    graph::GraphWithVectors,
    model::{
        algorithms::{global_search::GlobalSearch, RegisterFunction},
        plugins::{entry_point::EntryPoint, operation::Operation},
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

pub static QUERY_PLUGINS: Lazy<Mutex<HashMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
pub struct QueryPlugin {
    pub graphs: Arc<HashMap<String, VectorisedGraph<MaterializedGraph>>>,
}

impl<'a> EntryPoint<'a> for QueryPlugin {
    fn predefined_operations() -> HashMap<&'static str, RegisterFunction> {
        HashMap::from([(
            "globalSearch",
            Box::new(GlobalSearch::register_operation) as RegisterFunction,
        )])
    }

    fn lock_plugins() -> MutexGuard<'static, HashMap<String, RegisterFunction>> {
        QUERY_PLUGINS.lock().unwrap()
    }
}

impl Register for QueryPlugin {
    fn register(registry: Registry) -> Registry {
        Self::register_operations(registry)
    }
}

impl TypeName for QueryPlugin {
    fn get_type_name() -> Cow<'static, str> {
        "QueryPlugin".into()
    }
}

impl OutputTypeName for QueryPlugin {}

impl<'a> ResolveOwned<'a> for QueryPlugin {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}
