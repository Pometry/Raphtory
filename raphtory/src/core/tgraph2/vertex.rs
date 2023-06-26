use std::{ops::Range, sync::Arc};

use itertools::Itertools;

use crate::{
    core::{
        edge_ref::EdgeRef,
        tgraph_shard::LockedView,
        timeindex::{TimeIndex, TimeIndexOps},
        tprop::TProp,
        Direction, Prop,
    },
    storage::{iter::RefT, ArcEntry, Entry},
};

use super::{
    edge::EdgeView,
    edge_store::EdgeStore,
    iter::{Paged, PagedIter},
    node_store::NodeStore,
    tgraph::TGraph,
    tgraph_storage::GraphEntry,
    VRef, VID,
};

pub struct Vertex<'a, const N: usize> {
    node: VRef<'a, N>,
    graph: &'a TGraph<N>,
}

impl<'a, const N: usize> Vertex<'a, N> {
    pub fn id(&self) -> VID {
        self.node.index().into()
    }

    pub(crate) fn new(node: VRef<'a, N>, graph: &'a TGraph<N>) -> Self {
        Vertex { node, graph }
    }

    pub(crate) fn from_ref(node: RefT<'a, NodeStore<N>, N>, graph: &'a TGraph<N>) -> Self {
        Self::new(VRef::RefT(node), graph)
    }

    pub(crate) fn from_entry(node: Entry<'a, NodeStore<N>, N>, graph: &'a TGraph<N>) -> Self {
        Self::new(VRef::Entry(node), graph)
    }

    pub(crate) fn from_ge(ge: GraphEntry<NodeStore<N>, N>, graph: &'a TGraph<N>) -> Self {
        Self::new(VRef::LockedEntry(ge), graph)
    }

    pub fn temporal_properties(
        &'a self,
        name: &str,
        window: Option<Range<i64>>,
    ) -> impl Iterator<Item = (i64, Prop)> + 'a {
        let prop_id = self.graph.vertex_props_meta.resolve_prop_id(name, false);
        (&self.node).temporal_properties(prop_id, window)
    }

    pub fn edges(
        self,
        layer: Option<&str>,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeView<'a, N>> + 'a {
        let layer = layer
            .map(|layer| {
                self.graph
                    .vertex_props_meta
                    .get_or_create_layer_id(layer.to_owned())
            })
            .unwrap_or_default();

        let src = self.node.index().into();

        match dir {
            Direction::OUT | Direction::IN => {
                PagedIter::Page(Paged::new(Arc::new(self.node), dir, layer, src, self.graph))
            }
            Direction::BOTH => {
                let node = Arc::new(self.node);
                let out = Paged::new(node.clone(), Direction::OUT, layer, src, self.graph);
                let in_ = Paged::new(node, Direction::IN, layer, src, self.graph);
                PagedIter::Merged(out.merge(in_))
            }
        }
    }

    pub fn neighbours<'b>(
        &'a self,
        layer: &'b str,
        dir: Direction,
    ) -> impl Iterator<Item = Vertex<'a, N>> + 'a {
        let layer = self
            .graph
            .vertex_props_meta
            .get_or_create_layer_id(layer.to_owned());

        (*self.node)
            .neighbours(Some(layer), dir)
            .map(move |dst| self.graph.vertex(dst))
    }

    pub(crate) fn additions(self) -> Option<LockedView<'a, TimeIndex>> {
        match self.node {
            VRef::Entry(entry) => {
                let t_index = entry.map(|entry| entry.timestamps());
                Some(t_index)
            }
            _ => None,
        }
    }

    pub(crate) fn temporal_property(self, prop_id: usize) -> Option<LockedView<'a, TProp>> {
        match self.node {
            VRef::Entry(entry) => {
                if entry.temporal_property(prop_id).is_none() {
                    return None;
                }

                let t_index = entry.map(|entry| entry.temporal_property(prop_id).unwrap());
                Some(t_index)
            }
            _ => None,
        }
    }
}

impl<'a, const N: usize> IntoIterator for Vertex<'a, N> {
    type Item = Vertex<'a, N>;
    type IntoIter = std::iter::Once<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self)
    }
}

pub struct ArcVertex<const N: usize> {
    e: ArcEntry<NodeStore<N>, N>,
}

impl<const N: usize> ArcVertex<N> {
    pub(crate) fn from_entry(e: ArcEntry<NodeStore<N>, N>) -> Self {
        ArcVertex { e }
    }

    pub fn edge_tuples(
        &self,
        layer: Option<usize>,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + '_ {
        self.e.edge_tuples(layer, dir)
    }

    pub fn neighbours(
        &self,
        layer: Option<usize>,
        dir: Direction,
    ) -> impl Iterator<Item = VID> + '_ {
        self.e.neighbours(layer, dir)
    }
}

pub(crate) struct ArcEdge<const N: usize> {
    e: ArcEntry<EdgeStore<N>, N>,
}

impl<const N: usize> ArcEdge<N> {
    pub(crate) fn from_entry(e: ArcEntry<EdgeStore<N>, N>) -> Self {
        ArcEdge { e }
    }

    pub(crate) fn timestamps(&self, layer: usize) -> impl Iterator<Item = &i64> + '_ {
        self.e.layer_timestamps(layer).iter()
    }

    pub(crate) fn timestamps_window(
        &self,
        layer: usize,
        w: Range<i64>,
    ) -> impl Iterator<Item = &i64> + '_ {
        self.e.layer_timestamps(layer).range_iter(w)
    }

    pub(crate) fn time_index(&self, layer: usize) -> &TimeIndex {
        self.e.layer_timestamps(layer)
    }
}
