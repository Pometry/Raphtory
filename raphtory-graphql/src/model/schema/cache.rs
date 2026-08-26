use crate::model::schema::property_schema::PropertySchema;
use raphtory_api::core::entities::LayerId;
use std::{collections::HashMap, sync::RwLock};

/// Root per-graph schema cache. Lives on the in-memory `GraphWithVectors` and is
/// shared with the base `GqlGraph` view.
#[derive(Default)]
pub struct SchemaCache {
    node: NodeSchemaCache,
    layer: LayerSchemaCache,
}

impl SchemaCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn node(&self) -> &NodeSchemaCache {
        &self.node
    }

    pub(crate) fn layer(&self) -> &LayerSchemaCache {
        &self.layer
    }

    /// Drop all cached schema data. Called on any graph mutation.
    pub(crate) fn invalidate(&self) {
        self.node.invalidate();
        self.layer.invalidate();
    }
}

/// Cache of computed node schema results, keyed by node type id.
#[derive(Default)]
pub(crate) struct NodeSchemaCache {
    properties: RwLock<HashMap<usize, Vec<PropertySchema>>>,
    metadata: RwLock<HashMap<usize, Vec<PropertySchema>>>,
}

impl NodeSchemaCache {
    pub(crate) fn get_properties(&self, type_id: usize) -> Option<Vec<PropertySchema>> {
        self.properties.read().unwrap().get(&type_id).cloned()
    }

    pub(crate) fn store_properties(&self, type_id: usize, value: Vec<PropertySchema>) {
        self.properties.write().unwrap().insert(type_id, value);
    }

    pub(crate) fn get_metadata(&self, type_id: usize) -> Option<Vec<PropertySchema>> {
        self.metadata.read().unwrap().get(&type_id).cloned()
    }

    pub(crate) fn store_metadata(&self, type_id: usize, value: Vec<PropertySchema>) {
        self.metadata.write().unwrap().insert(type_id, value);
    }

    fn invalidate(&self) {
        self.properties.write().unwrap().clear();
        self.metadata.write().unwrap().clear();
    }
}

/// Cache of computed layer schema results, keyed by layer id. Caches the
/// resolved property/metadata lists for a layer whether or not variants were
/// collected, so a repeated `schema` query reuses them until the next mutation.
#[derive(Default)]
pub(crate) struct LayerSchemaCache {
    properties: RwLock<HashMap<LayerId, Vec<PropertySchema>>>,
    metadata: RwLock<HashMap<LayerId, Vec<PropertySchema>>>,
}

impl LayerSchemaCache {
    pub(crate) fn get_properties(&self, key: &LayerId) -> Option<Vec<PropertySchema>> {
        self.properties.read().unwrap().get(key).cloned()
    }

    pub(crate) fn store_properties(&self, key: LayerId, value: Vec<PropertySchema>) {
        self.properties.write().unwrap().insert(key, value);
    }

    pub(crate) fn get_metadata(&self, key: &LayerId) -> Option<Vec<PropertySchema>> {
        self.metadata.read().unwrap().get(key).cloned()
    }

    pub(crate) fn store_metadata(&self, key: LayerId, value: Vec<PropertySchema>) {
        self.metadata.write().unwrap().insert(key, value);
    }

    fn invalidate(&self) {
        self.properties.write().unwrap().clear();
        self.metadata.write().unwrap().clear();
    }
}
