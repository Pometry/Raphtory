use crate::{core::entities::LayerIds, db::api::view::internal::Base};
use enum_dispatch::enum_dispatch;
use raphtory_memstorage::db::api::storage::graph::edges::edge_ref::EdgeStorageRef;

#[enum_dispatch]
pub trait EdgeFilterOps {
    /// If true, the edges from the underlying storage are filtered
    fn edges_filtered(&self) -> bool;

    /// If true, all edges returned by `self.edge_list()` exist, otherwise it needs further filtering
    fn edge_list_trusted(&self) -> bool;

    /// If true, do not need to check src and dst of the edge separately, even if nodes are filtered
    /// (i.e., edge filter already makes sure there are no edges between non-existent nodes)
    fn edge_filter_includes_node_filter(&self) -> bool;

    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool;
}

pub trait InheritEdgeFilterOps: Base {}

impl<G: InheritEdgeFilterOps> DelegateEdgeFilterOps for G
where
    G::Base: EdgeFilterOps,
{
    type Internal = G::Base;

    #[inline]
    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateEdgeFilterOps {
    type Internal: EdgeFilterOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateEdgeFilterOps> EdgeFilterOps for G {
    #[inline]
    fn edges_filtered(&self) -> bool {
        self.graph().edges_filtered()
    }

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        self.graph().edge_list_trusted()
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        self.graph().edge_filter_includes_node_filter()
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph().filter_edge(edge, layer_ids)
    }
}
