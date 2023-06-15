use std::rc::Rc;

use itertools::Itertools;

use crate::{
    core::{Direction, Prop},
    storage::{iter::RefT, Entry},
};

use super::{
    edge::Edge,
    iter::{Paged, PagedIter},
    node_store::NodeStore,
    tgraph::TGraph,
    VRef, VID,
};

pub struct Vertex<'a, const N: usize, L: lock_api::RawRwLock> {
    node: VRef<'a, N, L>, //RefT<'a, NodeStore<N>, L, N>,
    graph: &'a TGraph<N, L>,
}

impl<'a, const N: usize, L: lock_api::RawRwLock> Vertex<'a, N, L> {
    pub fn id(&self) -> VID {
        self.node.index().into()
    }

    pub(crate) fn from_ref(node: RefT<'a, NodeStore<N>, L, N>, graph: &'a TGraph<N, L>) -> Self {
        Vertex {
            node: VRef::RefT(node),
            graph,
        }
    }

    pub(crate) fn from_entry(node: Entry<'a, NodeStore<N>, L, N>, graph: &'a TGraph<N, L>) -> Self {
        Vertex {
            node: VRef::Entry(node),
            graph,
        }
    }

    pub fn temporal_properties(&'a self, name: &str) -> impl Iterator<Item = (i64, Prop)> + 'a {
        let prop_id = self.graph.inner.props_meta.resolve_prop_id(name);
        (&self.node).temporal_properties(prop_id)
    }

    pub fn edges(self, layer: &str, dir: Direction) -> impl Iterator<Item = Edge<'a, N, L>> + 'a {
        let layer = self
            .graph
            .inner
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
    ) -> impl Iterator<Item = Edge<'a, N, L>> + 'a {
        let layer = self
            .graph
            .inner
            .props_meta
            .get_or_create_layer_id(layer.to_owned());
        (*self.node)
            .edges(layer, dir)
            .map(move |(dst, e_id)| Edge::new(self.node.index().into(), dst, e_id, dir, self.graph))
    }

    pub fn neighbours(
        &'a self,
        layer: &str,
        dir: Direction,
    ) -> impl Iterator<Item = Vertex<'a, N, L>> + 'a {
        let layer = self
            .graph
            .inner
            .props_meta
            .get_or_create_layer_id(layer.to_owned());

        (*self.node)
            .edges(layer, dir)
            .map(move |(dst, _)| self.graph.vertex(dst))
    }
}
