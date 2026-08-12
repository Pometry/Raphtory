use crate::{
    api::edges::LockedEdgeSegment,
    segments::edge::{entry::MemEdgeRef, segment::MemEdgeSegment},
    utils::Iter4,
    LocalPOS,
};
use parking_lot::{
    lock_api::ArcRwLockReadGuard,
    RawRwLock,
};
use raphtory_api::core::entities::{
    properties::meta::STATIC_GRAPH_LAYER_ID,
    LayerId,
};
use raphtory_api_macros::box_on_debug_lifetime;
use raphtory_core::entities::{edges::edge_ref::EdgeRef, LayerIds};
use rayon::prelude::*;

#[derive(Debug)]
pub struct ArcLockedEdgeSegmentView {
    inner: ArcRwLockReadGuard<RawRwLock, MemEdgeSegment>,
    num_edges: u32,
}

impl ArcLockedEdgeSegmentView {
    pub(crate) fn new(
        inner: ArcRwLockReadGuard<RawRwLock, MemEdgeSegment>,
        num_edges: u32,
    ) -> Self {
        Self { inner, num_edges }
    }

    fn edge_iter_layer<'a>(
        &'a self,
        layer_id: LayerId,
    ) -> impl Iterator<Item = MemEdgeRef<'a>> + Send + Sync + 'a {
        self.inner
            .get_layer(layer_id)
            .into_iter()
            .flat_map(|layer| layer.filled_positions())
            .map(move |pos| MemEdgeRef::new(pos, &self.inner, None))
    }

    fn edge_par_iter_layer<'a>(
        &'a self,
        layer_id: LayerId,
    ) -> impl ParallelIterator<Item = MemEdgeRef<'a>> + 'a {
        self.inner
            .get_layer(layer_id)
            .into_par_iter()
            .flat_map(|layer| layer.filled_positions_par())
            .map(move |pos| MemEdgeRef::new(pos, &self.inner, None))
    }
}

impl LockedEdgeSegment for ArcLockedEdgeSegmentView {
    type EntryRef<'a> = MemEdgeRef<'a>;

    fn entry_ref<'a>(
        &'a self,
        edge_pos: impl Into<LocalPOS>,
        edge_ref: Option<EdgeRef>,
    ) -> Self::EntryRef<'a>
    where
        Self: 'a,
    {
        let edge_pos = edge_pos.into();
        MemEdgeRef::new(edge_pos, &self.inner, edge_ref)
    }

    #[box_on_debug_lifetime]
    fn edge_iter<'a, 'b: 'a>(
        &'a self,
        layer_ids: &'b LayerIds,
    ) -> impl Iterator<Item = Self::EntryRef<'a>> + Send + Sync + 'a {
        match layer_ids {
            LayerIds::None => Iter4::I(std::iter::empty()),
            LayerIds::All => Iter4::J(self.edge_iter_layer(STATIC_GRAPH_LAYER_ID)),
            LayerIds::One(layer_id) => Iter4::K(self.edge_iter_layer(*layer_id)),
            LayerIds::Multiple(multiple) => Iter4::L(
                self.edge_iter_layer(STATIC_GRAPH_LAYER_ID)
                    .filter(|pos| pos.has_layers(multiple)),
            ),
        }
    }

    fn edge_par_iter<'a, 'b: 'a>(
        &'a self,
        layer_ids: &'b LayerIds,
    ) -> impl ParallelIterator<Item = Self::EntryRef<'a>> + 'a {
        match layer_ids {
            LayerIds::None => Iter4::I(rayon::iter::empty()),
            LayerIds::All => Iter4::J(self.edge_par_iter_layer(STATIC_GRAPH_LAYER_ID)),
            LayerIds::One(layer_id) => Iter4::K(self.edge_par_iter_layer(*layer_id)),
            LayerIds::Multiple(multiple) => Iter4::L(
                self.edge_par_iter_layer(STATIC_GRAPH_LAYER_ID)
                    .filter(|pos| pos.has_layers(multiple)),
            ),
        }
    }

    fn num_edges(&self) -> u32 {
        self.num_edges
    }
}
