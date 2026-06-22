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

/// Cache key for a node schema entry: the node type id.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct NodeSchemaKey {
    pub type_id: usize,
}

impl NodeSchemaKey {
    pub(crate) fn new(type_id: usize) -> Self {
        Self { type_id }
    }
}

/// Root per-graph schema cache. Lives on the in-memory `GraphWithVectors` and is
/// shared with the base `GqlGraph` view.
#[derive(Default)]
pub(crate) struct SchemaCache {
    edge: EdgeSchemaCache,
    node: NodeSchemaCache,
}

impl SchemaCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn edge(&self) -> &EdgeSchemaCache {
        &self.edge
    }

    pub(crate) fn node(&self) -> &NodeSchemaCache {
        &self.node
    }

    /// Drop all cached schema data. Called on any graph mutation.
    pub(crate) fn invalidate(&self) {
        self.edge.invalidate();
        self.node.invalidate();
    }
}

/// Cache of computed edge schema results, keyed by `(layer, src_type, dst_type)`.
#[derive(Default)]
pub(crate) struct EdgeSchemaCache {
    properties: RwLock<HashMap<EdgeSchemaKey, Arc<Vec<PropertySchema>>>>,
    metadata: RwLock<HashMap<EdgeSchemaKey, Arc<Vec<PropertySchema>>>>,
}

impl EdgeSchemaCache {
    pub(crate) fn get_properties(&self, key: &EdgeSchemaKey) -> Option<Arc<Vec<PropertySchema>>> {
        self.properties.read().unwrap().get(key).cloned()
    }

    pub(crate) fn store_properties(&self, key: EdgeSchemaKey, value: Arc<Vec<PropertySchema>>) {
        self.properties.write().unwrap().insert(key, value);
    }

    pub(crate) fn get_metadata(&self, key: &EdgeSchemaKey) -> Option<Arc<Vec<PropertySchema>>> {
        self.metadata.read().unwrap().get(key).cloned()
    }

    pub(crate) fn store_metadata(&self, key: EdgeSchemaKey, value: Arc<Vec<PropertySchema>>) {
        self.metadata.write().unwrap().insert(key, value);
    }

    fn invalidate(&self) {
        self.properties.write().unwrap().clear();
        self.metadata.write().unwrap().clear();
    }
}

/// Cache of computed node schema results, keyed by node type id.
#[derive(Default)]
pub(crate) struct NodeSchemaCache {
    properties: RwLock<HashMap<NodeSchemaKey, Arc<Vec<PropertySchema>>>>,
    metadata: RwLock<HashMap<NodeSchemaKey, Arc<Vec<PropertySchema>>>>,
}

impl NodeSchemaCache {
    pub(crate) fn get_properties(&self, key: &NodeSchemaKey) -> Option<Arc<Vec<PropertySchema>>> {
        self.properties.read().unwrap().get(key).cloned()
    }

    pub(crate) fn store_properties(&self, key: NodeSchemaKey, value: Arc<Vec<PropertySchema>>) {
        self.properties.write().unwrap().insert(key, value);
    }

    pub(crate) fn get_metadata(&self, key: &NodeSchemaKey) -> Option<Arc<Vec<PropertySchema>>> {
        self.metadata.read().unwrap().get(key).cloned()
    }

    pub(crate) fn store_metadata(&self, key: NodeSchemaKey, value: Arc<Vec<PropertySchema>>) {
        self.metadata.write().unwrap().insert(key, value);
    }

    fn invalidate(&self) {
        self.properties.write().unwrap().clear();
        self.metadata.write().unwrap().clear();
    }
}
