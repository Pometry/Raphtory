use crate::{core::entities::LayerIds, db::api::view::internal::Base};
use enum_dispatch::enum_dispatch;
use raphtory_api::core::{entities::Layer, utils::errors::GraphError};

#[enum_dispatch]
pub trait InternalLayerOps {
    /// get the layer ids for the graph view
    fn layer_ids(&self) -> &LayerIds;

    /// Get the layer id for the given layer name
    fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, GraphError>;

    /// Get the valid layer ids for given layer names
    fn valid_layer_ids_from_names(&self, key: Layer) -> LayerIds;
}

pub trait InheritLayerOps: Base {}

impl<G: InheritLayerOps> DelegateLayerOps for G
where
    G::Base: InternalLayerOps,
{
    type Internal = G::Base;

    #[inline]
    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateLayerOps {
    type Internal: InternalLayerOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateLayerOps> InternalLayerOps for G {
    #[inline]
    fn layer_ids(&self) -> &LayerIds {
        self.graph().layer_ids()
    }

    #[inline]
    fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, GraphError> {
        self.graph().layer_ids_from_names(key)
    }

    #[inline]
    fn valid_layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.graph().valid_layer_ids_from_names(key)
    }
}
