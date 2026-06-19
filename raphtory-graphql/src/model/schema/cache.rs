use crate::model::schema::property_schema::PropertySchema;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

/// Cache key for an edge schema entry: `(layer name, src node type, dst node type)`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct EdgeSchemaKey {
    pub layer: String,
    pub src_type: String,
    pub dst_type: String,
}

impl EdgeSchemaKey {
    pub(crate) fn new(layer: String, src_type: String, dst_type: String) -> Self {
        Self {
            layer,
            src_type,
            dst_type,
        }
    }
}

/// Per-graph cache of computed edge schema results.
/// Lives on the in-memory `GraphWithVectors` and is shared with the base `GqlGraph` view.
#[derive(Default)]
pub(crate) struct SchemaCache {
    properties: RwLock<HashMap<EdgeSchemaKey, Arc<Vec<PropertySchema>>>>,
    metadata: RwLock<HashMap<EdgeSchemaKey, Arc<Vec<PropertySchema>>>>,
}

impl SchemaCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn get_edge_properties(
        &self,
        key: &EdgeSchemaKey,
    ) -> Option<Arc<Vec<PropertySchema>>> {
        self.properties.read().unwrap().get(key).cloned()
    }

    pub(crate) fn store_edge_properties(
        &self,
        key: EdgeSchemaKey,
        value: Arc<Vec<PropertySchema>>,
    ) {
        self.properties.write().unwrap().insert(key, value);
    }

    pub(crate) fn get_edge_metadata(
        &self,
        key: &EdgeSchemaKey,
    ) -> Option<Arc<Vec<PropertySchema>>> {
        self.metadata.read().unwrap().get(key).cloned()
    }

    pub(crate) fn store_edge_metadata(&self, key: EdgeSchemaKey, value: Arc<Vec<PropertySchema>>) {
        self.metadata.write().unwrap().insert(key, value);
    }

    /// Drop all cached schema data. Called on any graph mutation.
    pub(crate) fn invalidate(&self) {
        self.properties.write().unwrap().clear();
        self.metadata.write().unwrap().clear();
    }
}
