use crate::core::entities::LayerIds;
use raphtory_api::{
    core::{entities::ELID, storage::timeindex::TimeIndexEntry},
    inherit::Base,
};
use raphtory_storage::graph::edges::edge_ref::EdgeStorageRef;

pub trait InternalEdgeLayerFilterOps {
    /// Set to true when filtering, used for optimisations
    fn internal_edge_layer_filtered(&self) -> bool;

    /// If true, all edges removed by this filter have also been removed from the edge list
    fn internal_layer_filter_edge_list_trusted(&self) -> bool;

    /// Filter a layer for an edge
    fn internal_filter_edge_layer(&self, edge: EdgeStorageRef, layer: usize) -> bool;

    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        false
    }
}

pub trait InternalExplodedEdgeFilterOps {
    /// Set to true when filtering, used for optimisations
    fn internal_exploded_edge_filtered(&self) -> bool;

    /// If true, all edges removed by this filter have also been removed from the edge list
    fn internal_exploded_filter_edge_list_trusted(&self) -> bool;

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> bool;

    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
        false
    }
}

pub trait InternalEdgeFilterOps {
    /// If true, the edges from the underlying storage are filtered
    fn internal_edge_filtered(&self) -> bool;

    /// If true, all edges returned by `self.edge_list()` exist, otherwise it needs further filtering
    fn internal_edge_list_trusted(&self) -> bool;

    fn internal_filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool;

    fn node_filter_includes_edge_filter(&self) -> bool {
        false
    }
}

pub trait InheritAllEdgeFilterOps: Base {}

pub trait InheritEdgeFilterOps: Base {}

impl<G: InheritAllEdgeFilterOps> InheritEdgeFilterOps for G {}
impl<G: InheritEdgeFilterOps<Base: InternalEdgeFilterOps>> InternalEdgeFilterOps for G {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.base().internal_edge_filtered()
    }
    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.base().internal_edge_list_trusted()
    }
    #[inline]
    fn internal_filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.base().internal_filter_edge(edge, layer_ids)
    }

    #[inline]
    fn node_filter_includes_edge_filter(&self) -> bool {
        self.base().node_filter_includes_edge_filter()
    }
}

pub trait InheritEdgeLayerFilterOps: Base {}

impl<G: InheritAllEdgeFilterOps> InheritEdgeLayerFilterOps for G {}

impl<G: InheritEdgeLayerFilterOps<Base: InternalEdgeLayerFilterOps>> InternalEdgeLayerFilterOps
    for G
{
    #[inline]
    fn internal_edge_layer_filtered(&self) -> bool {
        self.base().internal_edge_layer_filtered()
    }

    #[inline]
    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        self.base().internal_layer_filter_edge_list_trusted()
    }

    #[inline]
    fn internal_filter_edge_layer(&self, edge: EdgeStorageRef, layer: usize) -> bool {
        self.base().internal_filter_edge_layer(edge, layer)
    }

    #[inline]
    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        self.base().node_filter_includes_edge_layer_filter()
    }
}

pub trait InheritExplodedEdgeFilterOps: Base {}

impl<G: InheritAllEdgeFilterOps> InheritExplodedEdgeFilterOps for G {}

impl<G: InheritExplodedEdgeFilterOps<Base: InternalExplodedEdgeFilterOps>>
    InternalExplodedEdgeFilterOps for G
{
    #[inline]
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.base().internal_exploded_edge_filtered()
    }
    #[inline]
    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        self.base().internal_exploded_filter_edge_list_trusted()
    }
    #[inline]
    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> bool {
        self.base().internal_filter_exploded_edge(eid, t, layer_ids)
    }

    #[inline]
    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
        self.base().node_filter_includes_exploded_edge_filter()
    }
}
