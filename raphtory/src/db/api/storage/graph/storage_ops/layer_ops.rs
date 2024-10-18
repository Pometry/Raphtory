use raphtory_api::core::{entities::Layer, utils::errors::GraphError};
use raphtory_memstorage::db::api::storage::graph::GraphStorage;

use crate::{core::entities::LayerIds, db::api::view::internal::InternalLayerOps};

impl InternalLayerOps for GraphStorage {
    fn layer_ids(&self) -> &LayerIds {
        &LayerIds::All
    }

    fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, GraphError> {
        match self {
            GraphStorage::Mem(storage) => storage.graph().layer_ids(key),
            GraphStorage::Unlocked(storage) => storage.layer_ids(key),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.layer_ids_from_names(key),
        }
    }

    fn valid_layer_ids_from_names(&self, key: Layer) -> LayerIds {
        match self {
            GraphStorage::Mem(storage) => storage.graph().valid_layer_ids(key),
            GraphStorage::Unlocked(storage) => storage.valid_layer_ids(key),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.valid_layer_ids_from_names(key),
        }
    }
}
