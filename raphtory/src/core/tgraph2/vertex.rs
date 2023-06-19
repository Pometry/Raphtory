use std::rc::Rc;

use itertools::Itertools;

use crate::{
    core::{Direction, Prop},
    storage::{iter::RefT, Entry},
};

use super::{
    edge::EdgeView,
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
        Self::new ( VRef::RefT(node), graph,)
    }

    pub(crate) fn from_entry(node: Entry<'a, NodeStore<N>, N>, graph: &'a TGraph<N>) -> Self {
        Self::new ( VRef::Entry(node), graph,)
    }

    pub(crate) fn from_ge(ge: GraphEntry<'a, NodeStore<N>, N>, graph: &'a TGraph<N>) -> Self {
        Self::new(VRef::LockedEntry(ge), graph)
    }

    pub fn temporal_properties(&'a self, name: &str) -> impl Iterator<Item = (i64, Prop)> + 'a {
        let prop_id = self.graph.props_meta.resolve_prop_id(name, false);
        (&self.node).temporal_properties(prop_id)
    }

    pub fn edges(self, layer: &str, dir: Direction) -> impl Iterator<Item = EdgeView<'a, N>> + 'a {
        let layer = self
            .graph
            .props_meta
            .get_or_create_layer_id(layer.to_owned());

        let src = self.node.index().into();

        match dir {
            Direction::OUT | Direction::IN => {
                PagedIter::Page(Paged::new(Rc::new(self.node), dir, layer, src, self.graph))
            }
            Direction::BOTH => {
                let node = Rc::new(self.node);
                let out = Paged::new(node.clone(), Direction::OUT, layer, src, self.graph);
                let in_ = Paged::new(node, Direction::IN, layer, src, self.graph);
                PagedIter::Merged(out.merge(in_))
            }
        }
    }

    pub fn edges_iter(
        &'a self,
        layer: &str,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeView<'a, N>> + Send + 'a {
        let layer = self
            .graph
            .props_meta
            .get_or_create_layer_id(layer.to_owned());
        (*self.node)
            .edge_tuples(layer, dir)
            .map(move |(dst, e_id)| {
                EdgeView::new(
                    self.node.index().into(),
                    dst,
                    self.node.edge_ref(e_id),
                    dir,
                    self.graph,
                )
            })
    }

    pub fn neighbours<'b>(
        &'a self,
        layer: &'b str,
        dir: Direction,
    ) -> impl Iterator<Item = Vertex<'a, N>> + 'a {
        let layer = self
            .graph
            .props_meta
            .get_or_create_layer_id(layer.to_owned());

        (*self.node)
            .edge_tuples(layer, dir)
            .map(move |(dst, _)| self.graph.vertex(dst))
    }
}


impl<'a, const N: usize> IntoIterator for Vertex<'a, N> {
    type Item = Vertex<'a, N>;
    type IntoIter = std::iter::Once<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self)
    }
}