use crate::core::{
    entities::{
        edges::{edge::EdgeView, edge_ref::EdgeRef, edge_store::EdgeStore},
        graph::tgraph::TGraph,
        properties::tprop::TProp,
        vertices::{
            structure::iter::{Paged, PagedIter},
            vertex_store::VertexStore,
        },
        VRef, VID, LayerIds,
    },
    storage::{
        locked_view::LockedView,
        timeindex::{TimeIndex, TimeIndexOps},
        ArcEntry, Entry,
    },
    Direction, Prop,
};
use itertools::Itertools;
use std::{ops::Range, sync::Arc};

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

    pub(crate) fn from_entry(node: Entry<'a, VertexStore<N>, N>, graph: &'a TGraph<N>) -> Self {
        Self::new(VRef::Entry(node), graph)
    }

    pub fn temporal_properties(
        &'a self,
        name: &str,
        window: Option<Range<i64>>,
    ) -> impl Iterator<Item = (i64, Prop)> + 'a {
        let prop_id = self.graph.vertex_meta.resolve_prop_id(name, false);
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
                    .vertex_meta
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
    e: ArcEntry<VertexStore<N>, N>,
}

impl<const N: usize> ArcVertex<N> {
    pub(crate) fn from_entry(e: ArcEntry<VertexStore<N>, N>) -> Self {
        ArcVertex { e }
    }

    pub fn edge_tuples(
        &self,
        layers: LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + '_ {
        self.e.edge_tuples(layers, dir)
    }

    pub fn neighbours(&self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = VID> + '_ {
        self.e.neighbours(layers, dir)
    }
}

pub(crate) struct ArcEdge<const N: usize> {
    e: ArcEntry<EdgeStore<N>, N>,
}

impl<const N: usize> ArcEdge<N> {
    pub(crate) fn from_entry(e: ArcEntry<EdgeStore<N>, N>) -> Self {
        ArcEdge { e }
    }

    pub(crate) fn timestamps(&self, layer: LayerIds) -> impl Iterator<Item = &i64> + Send + '_ {
        let adds = self.e.additions();
        adds.iter()
            .enumerate()
            .filter_map(|(layer_id, t)| layer.find(layer_id).map(|_| t))
            .map(|t| t.iter())
            .kmerge()
            .dedup()
    }

    pub(crate) fn timestamps_window(
        &self,
        layer: LayerIds,
        w: Range<i64>,
    ) -> impl Iterator<Item = &i64> + '_ {
        let adds = self.e.additions();
        adds.iter()
            .enumerate()
            .filter_map(|(layer_id, t)| layer.find(layer_id).map(|_| t))
            .map(|t| t.range_iter(w.clone()))
            .kmerge()
            .dedup()
    }
}
