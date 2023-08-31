use crate::{
    core::entities::{edges::edge_store::EdgeStore, LayerIds},
    db::api::view::internal::Base,
    prelude::Layer,
};

pub trait LayerOps {
    /// get the layer ids for the graph view
    fn layer_ids(&self) -> LayerIds;

    /// Get the layer id for the given layer name
    fn layer_ids_from_names(&self, key: Layer) -> LayerIds;

    /// get the layer ids for the given edge id
    fn edge_layer_ids(&self, e: &EdgeStore) -> LayerIds;
}

pub trait InheritLayerOps: Base {}

impl<G: InheritLayerOps> DelegateLayerOps for G
where
    G::Base: LayerOps,
{
    type Internal = G::Base;

    #[inline]
    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateLayerOps {
    type Internal: LayerOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateLayerOps> LayerOps for G {
    #[inline]
    fn layer_ids(&self) -> LayerIds {
        self.graph().layer_ids()
    }

    #[inline]
    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.graph().layer_ids_from_names(key)
    }

    #[inline]
    fn edge_layer_ids(&self, e: &EdgeStore) -> LayerIds {
        self.graph().edge_layer_ids(e)
    }
}
