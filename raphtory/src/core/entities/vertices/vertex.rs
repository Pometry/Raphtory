use crate::core::{
    entities::{
        edges::{edge::EdgeView, edge_ref::EdgeRef, edge_store::EdgeStore},
        graph::tgraph::TGraph,
        properties::{
            props::{DictMapper, Meta},
            tprop::TProp,
        },
        vertices::{
            structure::iter::{Paged, PagedIter},
            vertex_store::VertexStore,
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

pub struct Vertex<'a, const N: usize> {
    node: VRef<'a, N>,
    pub graph: &'a TGraph<N>,
}

impl<'a, const N: usize> Vertex<'a, N> {
    pub fn id(&self) -> VID {
        self.node.index().into()
    }

    pub(crate) fn new(node: VRef<'a, N>, graph: &'a TGraph<N>) -> Self {
        Vertex { node, graph }
    }

    pub(crate) fn from_entry(node: Entry<'a, VertexStore, N>, graph: &'a TGraph<N>) -> Self {
        Self::new(VRef::Entry(node), graph)
    }

    pub fn temporal_properties(
        &'a self,
        name: &str,
        window: Option<Range<i64>>,
    ) -> impl Iterator<Item = (i64, Prop)> + 'a {
        let prop_id = self.graph.vertex_meta.find_prop_id(name, false);
        prop_id
            .into_iter()
            .flat_map(move |prop_id| self.node.temporal_properties(prop_id, window.clone()))
    }

    pub fn neighbours<'b>(
        &'a self,
        layers: Vec<&'b str>,
        dir: Direction,
    ) -> impl Iterator<Item = Vertex<'a, N>> + 'a {
        let layer_ids = layers
            .iter()
            .filter_map(|str| self.graph.vertex_meta.get_layer_id(str))
            .collect_vec();

        (*self.node)
            .neighbours(layer_ids.into(), dir)
            .map(move |dst| self.graph.vertex(dst))
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

impl<'a, const N: usize> IntoIterator for Vertex<'a, N> {
    type Item = Vertex<'a, N>;
    type IntoIter = std::iter::Once<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self)
    }
}

pub struct ArcVertex {
    e: ArcEntry<VertexStore>,
    meta: Arc<Meta>,
}

impl ArcVertex {
    pub(crate) fn from_entry(e: ArcEntry<VertexStore>, meta: Arc<Meta>) -> Self {
        ArcVertex { e, meta }
    }

    pub fn edge_tuples(
        &self,
        layers: LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + '_ {
        self.e.edge_tuples(&layers, dir)
    }

    pub fn neighbours(&self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = VID> + '_ {
        self.e.neighbours(layers, dir)
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
    ) -> impl Iterator<Item = (usize, &TimeIndexEntry)> + Send + '_ {
        let adds = self.e.additions();
        adds.iter()
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
    ) -> impl Iterator<Item = (usize, &TimeIndexEntry)> + '_ {
        let adds = self.e.additions();
        adds.iter()
            .enumerate()
            .filter_map(|(layer_id, t)| {
                layer
                    .find(layer_id)
                    .map(|l| t.range_iter(w.clone()).map(move |tt| (l, tt)))
            })
            .kmerge_by(|a, b| a.1 < b.1)
    }
}
