use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            nodes::node_store::NodeStore,
            LayerIds, EID, VID,
        },
        storage::ReadLockedStorage,
        Direction,
    },
    db::api::view::internal::{FilterOps, FilterState},
    prelude::GraphViewOps,
};
use itertools::Itertools;
use rayon::prelude::*;
use std::{iter, sync::Arc};

#[derive(Clone, Debug)]
pub struct LockedGraph {
    pub(crate) nodes: Arc<ReadLockedStorage<NodeStore, VID>>,
    pub(crate) edges: Arc<ReadLockedStorage<EdgeStore, EID>>,
}

impl LockedGraph {
    pub fn layer_ids_iter<'graph, G: GraphViewOps<'graph>>(
        &self,
        view: &G,
    ) -> Box<dyn Iterator<Item = usize>> {
        let layer_ids = view.layer_ids().clone();
        match layer_ids {
            LayerIds::None => Box::new(iter::empty()),
            LayerIds::All => Box::new(0..view.unfiltered_num_layers()),
            LayerIds::One(id) => Box::new(iter::once(id)),
            LayerIds::Multiple(ids) => Box::new((0..ids.len()).map(move |i| ids[i])),
        }
    }

    pub fn count_nodes<'graph, G: GraphViewOps<'graph>>(&self, view: &G) -> usize {
        if view.node_list_trusted() {
            view.node_list().len()
        } else {
            let node_list = view.node_list();
            let layer_ids = view.layer_ids();
            node_list
                .par_iter()
                .filter(|id| view.filter_node(self.nodes.get(*id), layer_ids))
                .count()
        }
    }

    pub fn nodes_iter<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        view: &'graph G,
    ) -> Box<dyn Iterator<Item = VID> + Send + 'graph> {
        let iter = view.node_list().into_iter();
        if view.node_list_trusted() {
            iter
        } else {
            Box::new(iter.filter(|&vid| view.filter_node(self.nodes.get(vid), view.layer_ids())))
        }
    }

    pub fn into_nodes_iter<'graph, G: GraphViewOps<'graph>>(
        self,
        view: G,
    ) -> Box<dyn Iterator<Item = VID> + Send + 'graph> {
        let iter = view.node_list().into_iter();
        if view.node_list_trusted() {
            iter
        } else {
            Box::new(
                iter.filter(move |&vid| view.filter_node(self.nodes.get(vid), view.layer_ids())),
            )
        }
    }

    pub fn nodes_par<'a, 'graph: 'a, G: GraphViewOps<'graph>>(
        &'a self,
        view: &'a G,
    ) -> impl ParallelIterator<Item = VID> + 'a {
        view.node_list()
            .into_par_iter()
            .filter(|&vid| view.filter_node(self.nodes.get(vid), view.layer_ids()))
    }

    pub fn into_nodes_par<'graph, G: GraphViewOps<'graph>>(
        self,
        view: G,
    ) -> impl ParallelIterator<Item = VID> + 'graph {
        view.node_list()
            .into_par_iter()
            .filter(move |&vid| view.filter_node(self.nodes.get(vid), view.layer_ids()))
    }

    pub fn edges_iter<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        view: &'graph G,
    ) -> impl Iterator<Item = EdgeRef> + Send + 'graph {
        view.edge_list().into_iter().filter_map(|id| {
            let edge = self.edges.get(id);
            view.filter_edge(edge, view.layer_ids())
                .then(|| edge.into())
        })
    }

    pub fn into_edges_iter<'graph, G: GraphViewOps<'graph>>(
        self,
        view: G,
    ) -> impl Iterator<Item = EdgeRef> + Send + 'graph {
        view.edge_list().into_iter().filter_map(move |id| {
            let edge = self.edges.get(id);
            view.filter_edge(edge, view.layer_ids())
                .then(|| edge.into())
        })
    }

    pub fn edges_par<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        view: &'graph G,
    ) -> impl ParallelIterator<Item = EdgeRef> + 'graph {
        view.edge_list().into_par_iter().filter_map(|eid| {
            let edge = self.edges.get(eid);
            let src = self.nodes.get(edge.src);
            let dst = self.nodes.get(edge.dst);
            let layer_ids = view.layer_ids();
            (view.filter_edge(edge, layer_ids)
                && view.filter_node(src, layer_ids)
                && view.filter_node(dst, layer_ids))
            .then_some(EdgeRef::new_outgoing(eid, edge.src, edge.dst))
        })
    }

    pub fn into_edges_par<'graph, G: GraphViewOps<'graph>>(
        self,
        view: G,
    ) -> impl ParallelIterator<Item = EdgeRef> + 'graph {
        view.edge_list().into_par_iter().filter_map(move |eid| {
            let edge = self.edges.get(eid);
            let src = self.nodes.get(edge.src);
            let dst = self.nodes.get(edge.dst);
            let layer_ids = view.layer_ids();
            (view.filter_edge(edge, layer_ids)
                && view.filter_node(src, layer_ids)
                && view.filter_node(dst, layer_ids))
            .then_some(EdgeRef::new_outgoing(eid, edge.src, edge.dst))
        })
    }

    pub fn node_neighbours_iter<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        node: VID,
        dir: Direction,
        view: &'graph G,
    ) -> Box<dyn Iterator<Item = VID> + Send + 'graph> {
        let source = self.nodes.get(node);
        match view.filter_state() {
            FilterState::Both => {
                let iter = source
                    .edge_tuples(view.layer_ids(), dir)
                    .filter(|eref| {
                        view.filter_node(self.nodes.get(eref.remote()), view.layer_ids())
                            && view.filter_edge(self.edges.get(eref.pid()), view.layer_ids())
                    })
                    .map(|e| e.remote());
                if matches!(dir, Direction::BOTH) {
                    Box::new(iter.dedup())
                } else {
                    Box::new(iter)
                }
            }
            FilterState::Edges | FilterState::BothIndependent => {
                let iter = source
                    .edge_tuples(view.layer_ids(), dir)
                    .filter(|eref| view.filter_edge(self.edges.get(eref.pid()), view.layer_ids()))
                    .map(|e| e.remote());
                if matches!(dir, Direction::BOTH) {
                    Box::new(iter.dedup())
                } else {
                    Box::new(iter)
                }
            }
            FilterState::Nodes => Box::new(
                source
                    .neighbours(view.layer_ids(), dir)
                    .filter(|&n| view.filter_node(self.nodes.get(n), view.layer_ids())),
            ),
            FilterState::Neither => Box::new(source.neighbours(view.layer_ids(), dir)),
        }
    }

    pub fn into_node_neighbours_iter<'graph, G: GraphViewOps<'graph>>(
        self,
        node: VID,
        dir: Direction,
        view: G,
    ) -> Box<dyn Iterator<Item = VID> + Send + 'graph> {
        match view.filter_state() {
            FilterState::Both => {
                let source = self.nodes.arc_entry(node);
                let iter = source
                    .into_edges(view.layer_ids(), dir)
                    .filter(move |eref| {
                        view.filter_node(self.nodes.get(eref.remote()), view.layer_ids())
                            && view.filter_edge(self.edges.get(eref.pid()), view.layer_ids())
                    })
                    .map(|e| e.remote());
                if matches!(dir, Direction::BOTH) {
                    Box::new(iter.dedup())
                } else {
                    Box::new(iter)
                }
            }
            FilterState::Edges | FilterState::BothIndependent => {
                let source = self.nodes.arc_entry(node);
                let iter = source
                    .into_edges(view.layer_ids(), dir)
                    .filter(move |eref| {
                        view.filter_edge(self.edges.get(eref.pid()), view.layer_ids())
                    })
                    .map(|e| e.remote());
                if matches!(dir, Direction::BOTH) {
                    Box::new(iter.dedup())
                } else {
                    Box::new(iter)
                }
            }
            FilterState::Nodes => Box::new(
                self.nodes
                    .arc_entry(node)
                    .into_neighbours(view.layer_ids(), dir)
                    .filter(move |&n| view.filter_node(self.nodes.get(n), view.layer_ids())),
            ),
            FilterState::Neither => Box::new(
                self.nodes
                    .arc_entry(node)
                    .into_neighbours(view.layer_ids(), dir),
            ),
        }
    }

    #[inline]
    pub fn node_degree<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: VID,
        dir: Direction,
        view: &G,
    ) -> usize {
        let core_edges = &self.edges;
        let core_nodes = &self.nodes;
        match view.filter_state() {
            FilterState::Both => {
                let edges_iter = core_nodes
                    .get(node)
                    .edge_tuples(view.layer_ids(), dir)
                    .filter(|eref| {
                        view.filter_node(core_nodes.get(eref.remote()), view.layer_ids())
                            && view.filter_edge(core_edges.get(eref.pid()), view.layer_ids())
                    });
                if matches!(dir, Direction::BOTH) {
                    edges_iter
                        .dedup_by(|e1, e2| e1.remote() == e2.remote())
                        .count()
                } else {
                    edges_iter.count()
                }
            }
            FilterState::Edges | FilterState::BothIndependent => {
                let edges_iter = core_nodes
                    .get(node)
                    .edge_tuples(view.layer_ids(), dir)
                    .filter(|eref| view.filter_edge(core_edges.get(eref.pid()), view.layer_ids()));
                if matches!(dir, Direction::BOTH) {
                    edges_iter
                        .dedup_by(|e1, e2| e1.remote() == e2.remote())
                        .count()
                } else {
                    edges_iter.count()
                }
            }
            FilterState::Nodes => core_nodes
                .get(node)
                .neighbours(view.layer_ids(), dir)
                .filter(|v| view.filter_node(core_nodes.get(*v), view.layer_ids()))
                .count(),
            FilterState::Neither => {
                let core_node = core_nodes.get(node);
                core_node.degree(view.layer_ids(), dir)
            }
        }
    }

    pub fn node_edges_iter<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        node: VID,
        dir: Direction,
        view: &'graph G,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send + 'graph> {
        let layers = view.layer_ids();
        let local = self.nodes.get(node);
        let iter = local.edge_tuples(layers, dir);
        match view.filter_state() {
            FilterState::Neither => Box::new(iter),
            FilterState::Both => Box::new(iter.filter(|e| {
                view.filter_edge(self.edges.get(e.pid()), view.layer_ids())
                    && view.filter_node(self.nodes.get(e.remote()), view.layer_ids())
            })),
            FilterState::Nodes => Box::new(
                iter.filter(|e| view.filter_node(self.nodes.get(e.remote()), view.layer_ids())),
            ),
            FilterState::Edges | FilterState::BothIndependent => Box::new(
                iter.filter(|e| view.filter_edge(self.edges.get(e.pid()), view.layer_ids())),
            ),
        }
    }

    pub fn into_node_edges_iter<'graph, G: GraphViewOps<'graph>>(
        self,
        node: VID,
        dir: Direction,
        view: G,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send + 'graph> {
        let layers = view.layer_ids();
        let local = self.nodes.arc_entry(node);
        let iter = local.into_edges(layers, dir);
        match view.filter_state() {
            FilterState::Neither => Box::new(iter),
            FilterState::Both => Box::new(iter.filter(move |e| {
                view.filter_edge(self.edges.get(e.pid()), view.layer_ids())
                    && view.filter_node(self.nodes.get(e.remote()), view.layer_ids())
            })),
            FilterState::Nodes => {
                Box::new(iter.filter(move |e| {
                    view.filter_node(self.nodes.get(e.remote()), view.layer_ids())
                }))
            }
            FilterState::Edges | FilterState::BothIndependent => Box::new(
                iter.filter(move |e| view.filter_edge(self.edges.get(e.pid()), view.layer_ids())),
            ),
        }
    }
}
