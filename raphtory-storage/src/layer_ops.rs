use crate::{
    core_ops::CoreGraphOps,
    graph::{graph::GraphStorage, locked::LockedGraph},
};
use raphtory_api::{
    core::entities::{Layer, LayerIds},
    inherit::Base,
};
use raphtory_core::entities::graph::tgraph::InvalidLayer;
use std::sync::Arc;

pub trait InternalLayerOps: CoreGraphOps {
    /// get the layer ids for the graph view
    fn layer_ids(&self) -> &LayerIds;

    /// Get the layer id for the given layer name
    fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, InvalidLayer> {
        let layer_ids = match self.core_graph() {
            GraphStorage::Mem(LockedGraph { graph, .. }) | GraphStorage::Unlocked(graph) => {
                graph.layer_ids(key)
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(graph) => graph.layer_ids_from_names(key),
        }?;
        Ok(layer_ids.intersect(self.layer_ids()))
    }

    /// Get the valid layer ids for given layer names
    fn valid_layer_ids_from_names(&self, key: Layer) -> LayerIds {
        let layer_ids = match self.core_graph() {
            GraphStorage::Unlocked(graph) | GraphStorage::Mem(LockedGraph { graph, .. }) => {
                graph.valid_layer_ids(key)
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(graph) => graph.valid_layer_ids_from_names(key),
        };
        layer_ids.intersect(self.layer_ids())
    }
}

impl InternalLayerOps for GraphStorage {
    fn layer_ids(&self) -> &LayerIds {
        &LayerIds::All
    }
}

pub trait InheritLayerOps: Base {}

impl<G: InheritLayerOps + CoreGraphOps> InternalLayerOps for G
where
    G::Base: InternalLayerOps,
{
    #[inline]
    fn layer_ids(&self) -> &LayerIds {
        self.base().layer_ids()
    }
}

impl<T: ?Sized> InheritLayerOps for Arc<T> {}
impl<T: ?Sized> InheritLayerOps for &T {}
