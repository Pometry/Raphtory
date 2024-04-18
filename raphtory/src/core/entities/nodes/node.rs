use crate::core::{
    entities::{
        edges::{edge::EdgeView, edge_ref::EdgeRef, edge_store::EdgeStore},
        graph::tgraph::TGraph,
        nodes::{
            node_store::NodeStore,
            structure::iter::{Paged, PagedIter},
        },
        properties::{
            props::{DictMapper, Meta},
            tprop::TProp,
        },
        LayerIds, VRef, VID,
    },
    storage::{
        locked_view::LockedView,
        timeindex::{TimeIndex, TimeIndexEntry, TimeIndexOps},
        ArcEntry, Entry,
    },
    Direction, Prop,
};
use itertools::Itertools;
use std::{ops::Range, sync::Arc};

pub struct Node<'a, const N: usize> {
    node: VRef<'a>,
    pub graph: &'a TGraph<N>,
}

impl<'a, const N: usize> Node<'a, N> {
    pub(crate) fn new(node: VRef<'a>, graph: &'a TGraph<N>) -> Self {
        Node { node, graph }
    }

    pub(crate) fn from_entry(node: Entry<'a, NodeStore>, graph: &'a TGraph<N>) -> Self {
        Self::new(VRef::Entry(node), graph)
    }

    pub fn temporal_properties(
        &'a self,
        prop_id: usize,
        window: Option<Range<i64>>,
    ) -> impl Iterator<Item = (i64, Prop)> + 'a {
        self.node.temporal_properties(prop_id, window)
    }

    pub(crate) fn additions(self) -> Option<LockedView<'a, TimeIndex<i64>>> {
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
                entry.temporal_property(prop_id)?;

                let t_index = entry.map(|entry| entry.temporal_property(prop_id).unwrap());
                Some(t_index)
            }
            _ => None,
        }
    }
}

impl<'a, const N: usize> IntoIterator for Node<'a, N> {
    type Item = Node<'a, N>;
    type IntoIter = std::iter::Once<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self)
    }
}

pub struct ArcNode {
    e: ArcEntry<NodeStore>,
    meta: Arc<Meta>,
}

impl ArcNode {
    pub(crate) fn from_entry(e: ArcEntry<NodeStore>, meta: Arc<Meta>) -> Self {
        ArcNode { e, meta }
    }

    pub fn edge_tuples(
        &self,
        layers: LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + '_ {
        self.e.edge_tuples(&layers, dir)
    }
}

pub(crate) struct ArcEdge {
    e: ArcEntry<EdgeStore>,
    meta: Arc<Meta>,
}

impl ArcEdge {
    pub(crate) fn from_entry(e: ArcEntry<EdgeStore>, meta: Arc<Meta>) -> Self {
        ArcEdge { e, meta }
    }

    pub(crate) fn timestamps_and_layers(
        &self,
        layer: LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexEntry)> + Send + '_ {
        self.e
            .additions
            .iter()
            .enumerate()
            .filter_map(|(layer_id, t)| {
                layer
                    .find(layer_id)
                    .map(|l| t.iter().map(move |tt| (l, tt)))
            })
            .kmerge_by(|a, b| a.1 < b.1)
    }

    pub(crate) fn layers(&self) -> impl Iterator<Item = usize> + '_ {
        self.e.layer_ids_iter()
    }

    pub(crate) fn layers_window(&self, w: Range<i64>) -> impl Iterator<Item = usize> + '_ {
        self.e.layer_ids_window_iter(w)
    }

    pub(crate) fn timestamps_and_layers_window(
        &self,
        layer: LayerIds,
        w: Range<i64>,
    ) -> impl Iterator<Item = (usize, TimeIndexEntry)> + '_ {
        self.e
            .additions
            .iter()
            .enumerate()
            .filter_map(|(layer_id, t)| {
                layer
                    .find(layer_id)
                    .map(|l| t.range_iter(w.clone()).map(move |tt| (l, tt)))
            })
            .kmerge_by(|a, b| a.1 < b.1)
    }
}
