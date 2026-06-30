use crate::model::schema::property_schema::PropertySchema;
use std::{collections::HashMap, sync::RwLock};

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
    node: NodeSchemaCache,
}

impl SchemaCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn node(&self) -> &NodeSchemaCache {
        &self.node
    }

    /// Drop all cached schema data. Called on any graph mutation.
    pub(crate) fn invalidate(&self) {
        self.node.invalidate();
    }
}

/// Cache of computed node schema results, keyed by node type id.
#[derive(Default)]
pub(crate) struct NodeSchemaCache {
    properties: RwLock<HashMap<NodeSchemaKey, Vec<PropertySchema>>>,
    metadata: RwLock<HashMap<NodeSchemaKey, Vec<PropertySchema>>>,
}

impl NodeSchemaCache {
    pub(crate) fn get_properties(&self, key: &NodeSchemaKey) -> Option<Vec<PropertySchema>> {
        self.properties.read().unwrap().get(key).cloned()
    }

    pub(crate) fn store_properties(&self, key: NodeSchemaKey, value: Vec<PropertySchema>) {
        self.properties.write().unwrap().insert(key, value);
    }

    pub(crate) fn get_metadata(&self, key: &NodeSchemaKey) -> Option<Vec<PropertySchema>> {
        self.metadata.read().unwrap().get(key).cloned()
    }

    pub(crate) fn store_metadata(&self, key: NodeSchemaKey, value: Vec<PropertySchema>) {
        self.metadata.write().unwrap().insert(key, value);
    }

    fn invalidate(&self) {
        self.properties.write().unwrap().clear();
        self.metadata.write().unwrap().clear();
    }
}
