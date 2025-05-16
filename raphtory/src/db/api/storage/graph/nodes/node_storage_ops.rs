use std::borrow::Cow;

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, GidRef, LayerIds, VID},
        storage::ArcNodeEntry,
        Direction,
    },
    db::api::{storage::graph::tprop_storage_ops::TPropOps, view::internal::NodeAdditions},
    prelude::{GraphViewOps, Prop},
};
use itertools::Itertools;
use raphtory_api::iter::{BoxedLIter, IntoDynBoxed};

pub trait NodeStorageOps<'a>: Sized {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize;

    fn additions(self) -> NodeAdditions<'a>;

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a>;

    fn tprops(self) -> impl Iterator<Item = (usize, impl TPropOps<'a>)>;

    fn prop(self, prop_id: usize) -> Option<Prop>;

    fn edges_iter(
        self,
        layers: &LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a;

    fn filtered_edges_iter<G: GraphViewOps<'a>>(
        self,
        view: G,
        dir: Direction,
    ) -> BoxedLIter<'a, EdgeRef> {
        let iter = self.edges_iter(view.layer_ids(), dir);
        if view.edges_filtered() {
            iter.filter(move |e| {
                view.filter_edge(view.core_edge(e.pid()).as_ref(), view.layer_ids())
            })
            .into_dyn_boxed()
        } else {
            iter.into_dyn_boxed()
        }
    }

    fn node_type_id(self) -> usize;

    fn vid(self) -> VID;

    fn id(self) -> GidRef<'a>;

    fn name(self) -> Option<Cow<'a, str>>;

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef>;
}

pub trait NodeStorageIntoOps: Sized {
    fn into_edges_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef>;

    fn into_neighbours_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = VID> {
        self.into_edges_iter(layers, dir)
            .map(|e| e.remote())
            .dedup()
    }
}

impl NodeStorageIntoOps for ArcNodeEntry {
    fn into_edges_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> {
        self.into_edges(&layers, dir)
    }

    fn into_neighbours_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = VID> {
        self.into_neighbours(&layers, dir)
    }
}
