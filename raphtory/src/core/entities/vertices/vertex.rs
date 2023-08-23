use crate::{
    core::{
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
    },
    db::api::properties::internal::CorePropertiesOps,
};
use itertools::Itertools;
use std::{ops::Range, sync::Arc};

pub struct Vertex<'a, const N: usize> {
    node: VRef<'a, N>,
    pub graph: &'a TGraph<N>,
}

impl<'b, const N: usize> CorePropertiesOps for Vertex<'b, N> {
    fn const_prop_meta(&self) -> &DictMapper<String> {
        self.graph.vertex_meta.static_prop_meta()
    }

    fn temporal_prop_meta(&self) -> &DictMapper<String> {
        self.graph.vertex_meta.temporal_prop_meta()
    }

    fn temporal_prop(&self, id: usize) -> Option<&TProp> {
        self.node.props.as_ref().and_then(|p| p.temporal_prop(id))
    }

    fn const_prop(&self, id: usize) -> Option<&Prop> {
        self.node.props.as_ref().and_then(|p| p.static_prop(id))
    }
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
        let prop_id = self.graph.vertex_meta.resolve_prop_id(name, false);
        self.node.temporal_properties(prop_id, window)
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

impl CorePropertiesOps for ArcVertex {
    fn const_prop_meta(&self) -> &DictMapper<String> {
        self.meta.static_prop_meta()
    }

    fn temporal_prop_meta(&self) -> &DictMapper<String> {
        self.meta.temporal_prop_meta()
    }

    fn temporal_prop(&self, id: usize) -> Option<&TProp> {
        self.e.temporal_property(id)
    }

    fn const_prop(&self, id: usize) -> Option<&Prop> {
        self.e.static_property(id)
    }
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
