use std::{iter, sync::Arc};

use itertools::Itertools;
use rayon::prelude::*;

use crate::{
    core::{
        entities::{
            edges::{
                edge_ref::{Dir, EdgeRef},
                edge_store::EdgeStore,
            },
            nodes::node_store::NodeStore,
            LayerIds, EID, VID,
        },
        storage::ReadLockedStorage,
        Direction,
    },
    db::api::view::internal::{FilterOps, FilterState},
    prelude::GraphViewOps,
};

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
        &self,
        view: &G,
    ) -> Box<dyn Iterator<Item = VID> + Send + 'graph> {
        let iter = view.node_list().into_iter();
        if view.node_list_trusted() {
            iter
        } else {
            let nodes = self.nodes.clone();
            let view = view.clone();
            Box::new(iter.filter(move |&vid| view.filter_node(nodes.get(vid), view.layer_ids())))
        }
    }

    pub fn edges_iter<'graph, G: GraphViewOps<'graph>>(
        &self,
        view: &G,
    ) -> impl Iterator<Item = EdgeRef> + Send + 'graph {
        let core_edges = self.edges.clone();
        let view = view.clone();
        view.edge_list().into_iter().filter_map(move |id| {
            let edge = core_edges.get(id);
            view.filter_edge(edge, view.layer_ids())
                .then(move || EdgeRef::new_outgoing(edge.eid, edge.src, edge.dst))
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
            FilterState::Edges => {
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
            FilterState::Edges => {
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
        &self,
        node: VID,
        dir: Direction,
        view: &G,
    ) -> impl Iterator<Item = EdgeRef> + Send + 'graph {
        let layers = view.layer_ids();
        let local = self.nodes.arc_entry(node);
        match dir {
            Direction::OUT => {
                let iter: Box<dyn Iterator<Item = EdgeRef> + Send> = match layers {
                    LayerIds::None => Box::new(iter::empty()),
                    LayerIds::All => Box::new(
                        local
                            .into_layers()
                            .map(move |layer| {
                                layer
                                    .into_tuples(Dir::Out)
                                    .map(move |(n, e)| EdgeRef::new_outgoing(e, node, n))
                            })
                            .kmerge()
                            .dedup(),
                    ),
                    LayerIds::One(layer) => {
                        Box::new(local.into_layer(*layer).into_iter().flat_map(move |it| {
                            it.into_tuples(Dir::Out)
                                .map(move |(n, e)| EdgeRef::new_outgoing(e, node, n))
                        }))
                    }
                    LayerIds::Multiple(ids) => Box::new(
                        ids.iter()
                            .map(move |&layer| {
                                local
                                    .clone()
                                    .into_layer(layer)
                                    .into_iter()
                                    .flat_map(move |it| {
                                        it.into_tuples(Dir::Out)
                                            .map(move |(n, e)| EdgeRef::new_outgoing(e, node, n))
                                    })
                            })
                            .kmerge()
                            .dedup(),
                    ),
                };
                match view.filter_state() {
                    FilterState::Neither => iter,
                    FilterState::Both => {
                        let nodes = self.nodes.clone();
                        let edges = self.edges.clone();
                        let view = view.clone();
                        Box::new(iter.filter(move |e| {
                            view.filter_edge(edges.get(e.pid()), view.layer_ids())
                                && view.filter_node(nodes.get(e.remote()), view.layer_ids())
                        }))
                    }
                    FilterState::Nodes => {
                        let nodes = self.nodes.clone();
                        let view = view.clone();
                        Box::new(iter.filter(move |e| {
                            view.filter_node(nodes.get(e.remote()), view.layer_ids())
                        }))
                    }
                    FilterState::Edges => {
                        let edges = self.edges.clone();
                        let view = view.clone();
                        Box::new(iter.filter(move |e| {
                            view.filter_edge(edges.get(e.pid()), view.layer_ids())
                        }))
                    }
                }
            }
            Direction::IN => {
                let iter: Box<dyn Iterator<Item = EdgeRef> + Send> = match &layers {
                    LayerIds::None => Box::new(iter::empty()),
                    LayerIds::All => Box::new(
                        local
                            .into_layers()
                            .map(move |layer| {
                                layer
                                    .into_tuples(Dir::Into)
                                    .map(move |(n, e)| EdgeRef::new_incoming(e, n, node))
                            })
                            .kmerge()
                            .dedup(),
                    ),
                    LayerIds::One(layer) => {
                        Box::new(local.into_layer(*layer).into_iter().flat_map(move |it| {
                            it.into_tuples(Dir::Into)
                                .map(move |(n, e)| EdgeRef::new_incoming(e, n, node))
                        }))
                    }
                    LayerIds::Multiple(ids) => Box::new(
                        ids.iter()
                            .map(move |&layer| {
                                local
                                    .clone()
                                    .into_layer(layer)
                                    .into_iter()
                                    .flat_map(move |it| {
                                        it.into_tuples(Dir::Into)
                                            .map(move |(n, e)| EdgeRef::new_incoming(e, n, node))
                                    })
                            })
                            .kmerge()
                            .dedup(),
                    ),
                };
                match view.filter_state() {
                    FilterState::Neither => iter,
                    FilterState::Both => {
                        let nodes = self.nodes.clone();
                        let edges = self.edges.clone();
                        let view = view.clone();
                        Box::new(iter.filter(move |e| {
                            view.filter_edge(edges.get(e.pid()), view.layer_ids())
                                && view.filter_node(nodes.get(e.remote()), view.layer_ids())
                        }))
                    }
                    FilterState::Nodes => {
                        let nodes = self.nodes.clone();
                        let view = view.clone();
                        Box::new(iter.filter(move |e| {
                            view.filter_node(nodes.get(e.remote()), view.layer_ids())
                        }))
                    }
                    FilterState::Edges => {
                        let edges = self.edges.clone();
                        let view = view.clone();
                        Box::new(iter.filter(move |e| {
                            view.filter_edge(edges.get(e.pid()), view.layer_ids())
                        }))
                    }
                }
            }
            Direction::BOTH => Box::new(
                self.node_edges_iter(node, Direction::IN, view)
                    .filter(|e| e.src() != e.dst())
                    .merge(self.node_edges_iter(node, Direction::OUT, view)),
            ),
        }
    }
}
