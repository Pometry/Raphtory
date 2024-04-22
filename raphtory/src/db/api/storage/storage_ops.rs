use crate::{
    arrow::graph::TemporalGraph,
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, EID, VID},
        Direction,
    },
    db::api::{
        storage::{
            arrow::{
                edges::ArrowEdgesRef,
                nodes::{ArrowNode, ArrowNodesRef},
            },
            direction_variants::DirectionVariants,
            edges::{edge_ref::EdgeStorageRef, edges::EdgesStorageRef},
            filter_variants::FilterVariants,
            layer_variants::LayerVariants,
            locked::LockedGraph,
            nodes::{node_ref::NodeStorageRef, nodes::NodesStorageRef},
        },
        view::{
            internal::{
                CoreGraphOps, EdgeFilterOps, FilterOps, FilterState, InternalLayerOps, ListOps,
                NodeFilterOps, NodeList,
            },
            IntoDynBoxed,
        },
    },
    prelude::GraphViewOps,
};
use arrow2::Either;
use itertools::Itertools;
use rayon::prelude::*;
use std::{iter, sync::Arc};

pub enum GraphStorage {
    Mem(LockedGraph),
    #[cfg(feature = "arrow")]
    Arrow(Arc<TemporalGraph>),
}

impl GraphStorage {
    pub fn nodes(&self) -> NodesStorageRef {
        match self {
            GraphStorage::Mem(storage) => NodesStorageRef::Mem(&storage.nodes),
            #[cfg(feature = "arrow")]
            GraphStorage::Arrow(storage) => NodesStorageRef::Arrow(ArrowNodesRef::new(storage)),
        }
    }

    pub fn node(&self, vid: VID) -> NodeStorageRef {
        match self {
            GraphStorage::Mem(storage) => NodeStorageRef::Mem(storage.nodes.get(vid)),
            #[cfg(feature = "arrow")]
            GraphStorage::Arrow(storage) => NodeStorageRef::Arrow(ArrowNode::new(storage, vid)),
        }
    }

    pub fn edges(&self) -> EdgesStorageRef {
        match self {
            GraphStorage::Mem(storage) => EdgesStorageRef::Mem(&storage.edges),
            GraphStorage::Arrow(storage) => EdgesStorageRef::Arrow(ArrowEdgesRef::new(storage)),
        }
    }

    pub fn edge_layer(&self, eid: EID, layer_id: usize) -> EdgeStorageRef {
        match self {
            GraphStorage::Mem(storage) => EdgeStorageRef::Mem(storage.edges.get(eid)),
            #[cfg(feature = "arrow")]
            GraphStorage::Arrow(storage) => {
                EdgeStorageRef::Arrow(storage.layer(layer_id).edge(eid))
            }
        }
    }

    pub fn edge(&self, eid: EID) -> EdgeStorageRef {
        match self {
            GraphStorage::Mem(storage) => EdgeStorageRef::Mem(storage.edges.get(eid)),
            #[cfg(feature = "arrow")]
            GraphStorage::Arrow(storage) => {
                todo!("edge without layer not implemented for arrow graph")
            }
        }
    }

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
            match node_list {
                NodeList::All { .. } => self
                    .nodes()
                    .par_iter()
                    .filter(|node| view.filter_node(*node, layer_ids))
                    .count(),
                NodeList::List { nodes } => {
                    let nodes_storage = self.nodes();
                    nodes
                        .par_iter()
                        .filter(|vid| view.filter_node(nodes_storage.node(**vid), layer_ids))
                        .count()
                }
            }
        }
    }

    pub fn nodes_iter<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        view: &'graph G,
    ) -> Box<dyn Iterator<Item = VID> + Send + 'graph> {
        if view.node_list_trusted() {
            view.node_list().into_iter()
        } else {
            match view.node_list() {
                NodeList::All { .. } => self
                    .nodes()
                    .iter()
                    .enumerate()
                    .filter(|(_, node)| view.filter_node(*node, view.layer_ids()))
                    .map(|(vid, _)| VID(vid))
                    .into_dyn_boxed(),
                nodes @ NodeList::List { .. } => {
                    let node_storage = self.nodes();
                    nodes
                        .into_iter()
                        .filter(|&vid| view.filter_node(node_storage.node(vid), view.layer_ids()))
                        .into_dyn_boxed()
                }
            }
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
            Box::new(iter.filter(move |&vid| view.filter_node(self.node(vid), view.layer_ids())))
        }
    }

    pub fn nodes_par<'a, 'graph: 'a, G: GraphViewOps<'graph>>(
        &'a self,
        view: &'a G,
    ) -> impl ParallelIterator<Item = VID> + 'a {
        view.node_list()
            .into_par_iter()
            .filter(|&vid| view.filter_node(self.nodes().node(vid), view.layer_ids()))
    }

    pub fn into_nodes_par<'graph, G: GraphViewOps<'graph>>(
        self,
        view: G,
    ) -> impl ParallelIterator<Item = VID> + 'graph {
        view.node_list()
            .into_par_iter()
            .filter(move |&vid| view.filter_node(self.node(vid), view.layer_ids()))
    }

    pub fn edges_iter<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        view: &'graph G,
    ) -> impl Iterator<Item = EdgeRef> + Send + 'graph {
        let iter = self.edges().iter(view.layer_ids().clone());

        let filtered = match view.filter_state() {
            FilterState::Neither => FilterVariants::Neither(iter),
            FilterState::Both => {
                let nodes = self.nodes();
                FilterVariants::Both(iter.filter(move |&e| {
                    view.filter_edge(e, view.layer_ids())
                        && view.filter_node(nodes.node(e.src()), view.layer_ids())
                        && view.filter_node(nodes.node(e.dst()), view.layer_ids())
                }))
            }
            FilterState::Nodes => {
                let nodes = self.nodes();
                FilterVariants::Nodes(iter.filter(move |&e| {
                    view.filter_node(nodes.node(e.src()), view.layer_ids())
                        && view.filter_node(nodes.node(e.dst()), view.layer_ids())
                }))
            }
            FilterState::Edges | FilterState::BothIndependent => {
                FilterVariants::Edges(iter.filter(|&e| view.filter_edge(e, view.layer_ids())))
            }
        };
        filtered.map(|e| e.out_ref())
    }

    pub fn into_edges_iter<'graph, G: GraphViewOps<'graph>>(
        self,
        view: G,
    ) -> impl Iterator<Item = EdgeRef> + Send + 'graph {
        match self {
            GraphStorage::Mem(storage) => {
                let nodes = storage.nodes;
                let edges = storage.edges;
                let iter = edges.into_iter();
                let filtered = match view.filter_state() {
                    FilterState::Neither => FilterVariants::Neither(iter),
                    FilterState::Both => FilterVariants::Both(iter.filter(move |e| {
                        let e = EdgeStorageRef::Mem(e);
                        view.filter_edge(e, view.layer_ids())
                            && view.filter_node(
                                NodeStorageRef::Mem(nodes.get(e.src())),
                                view.layer_ids(),
                            )
                            && view.filter_node(
                                NodeStorageRef::Mem(nodes.get(e.dst())),
                                view.layer_ids(),
                            )
                    })),
                    FilterState::Nodes => FilterVariants::Nodes(iter.filter(move |e| {
                        let e = EdgeStorageRef::Mem(e);
                        view.filter_node(NodeStorageRef::Mem(nodes.get(e.src())), view.layer_ids())
                            && view.filter_node(
                                NodeStorageRef::Mem(nodes.get(e.dst())),
                                view.layer_ids(),
                            )
                    })),
                    FilterState::Edges | FilterState::BothIndependent => {
                        FilterVariants::Edges(iter.filter(move |e| {
                            let e = EdgeStorageRef::Mem(e);
                            view.filter_edge(e, view.layer_ids())
                        }))
                    }
                };
                Either::Left(filtered.map(|e| EdgeRef::from(e)))
            }
            #[cfg(feature = "arrow")]
            GraphStorage::Arrow(storage) => {
                let storage_clone = storage.clone();
                let layer_ids = view.layer_ids().clone();
                let iter = match layer_ids {
                    LayerIds::None => LayerVariants::None,
                    LayerIds::All => {
                        LayerVariants::All((0..storage.layers.len()).flat_map(move |layer_id| {
                            storage_clone
                                .layer(layer_id)
                                .all_edge_ids()
                                .map(move |e| (layer_id, e))
                        }))
                    }
                    LayerIds::One(layer_id) => LayerVariants::One(
                        storage
                            .layer(layer_id)
                            .all_edge_ids()
                            .map(move |e| (layer_id, e)),
                    ),
                    LayerIds::Multiple(ids) => {
                        let ids = ids.clone();
                        LayerVariants::Multiple((0..ids.len()).flat_map(move |i| {
                            let layer_id = ids[i];
                            storage_clone
                                .layer(layer_id)
                                .all_edge_ids()
                                .map(move |e| (layer_id, e))
                        }))
                    }
                };
                let filtered = match view.filter_state() {
                    FilterState::Neither => {
                        FilterVariants::Neither(iter.map(move |(layer_id, eid)| {
                            EdgeStorageRef::Arrow(storage.layer(layer_id).edge(eid)).out_ref()
                        }))
                    }
                    FilterState::Both => {
                        FilterVariants::Both(iter.filter_map(move |(layer_id, eid)| {
                            let e = EdgeStorageRef::Arrow(storage.layer(layer_id).edge(eid));
                            if !view.filter_edge(e, view.layer_ids()) {
                                return None;
                            }
                            let src = NodeStorageRef::Arrow(ArrowNode::new(&storage, e.src()));
                            if !view.filter_node(src, view.layer_ids()) {
                                return None;
                            }
                            let dst = NodeStorageRef::Arrow(ArrowNode::new(&storage, e.dst()));
                            if !view.filter_node(dst, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                    FilterState::Nodes => {
                        FilterVariants::Nodes(iter.filter_map(move |(layer_id, eid)| {
                            let e = EdgeStorageRef::Arrow(storage.layer(layer_id).edge(eid));
                            let src = NodeStorageRef::Arrow(ArrowNode::new(&storage, e.src()));
                            if !view.filter_node(src, view.layer_ids()) {
                                return None;
                            }
                            let dst = NodeStorageRef::Arrow(ArrowNode::new(&storage, e.dst()));
                            if !view.filter_node(dst, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                    FilterState::Edges | FilterState::BothIndependent => {
                        FilterVariants::Edges(iter.filter_map(move |(layer_id, eid)| {
                            let e = EdgeStorageRef::Arrow(storage.layer(layer_id).edge(eid));
                            if !view.filter_edge(e, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                };
                Either::Right(filtered)
            }
        }
    }

    pub fn edges_par<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        view: &'graph G,
    ) -> impl ParallelIterator<Item = EdgeRef> + 'graph {
        self.edges()
            .par_iter(view.layer_ids().clone())
            .filter(|&edge| match view.filter_state() {
                FilterState::Neither => true,
                FilterState::Both => {
                    let layer_ids = view.layer_ids();
                    let src = self.node(edge.src());
                    let dst = self.node(edge.dst());
                    view.filter_edge(edge, view.layer_ids())
                        && view.filter_node(src, layer_ids)
                        && view.filter_node(dst, layer_ids)
                }
                FilterState::Nodes => {
                    let layer_ids = view.layer_ids();
                    let src = self.node(edge.src());
                    let dst = self.node(edge.dst());
                    view.filter_node(src, layer_ids) && view.filter_node(dst, layer_ids)
                }
                FilterState::Edges | FilterState::BothIndependent => {
                    view.filter_edge(edge, view.layer_ids())
                }
            })
            .map(|e| e.out_ref())
    }

    pub fn into_edges_par<'graph, G: GraphViewOps<'graph>>(
        self,
        view: G,
    ) -> impl ParallelIterator<Item = EdgeRef> + 'graph {
        match self {
            GraphStorage::Mem(storage) => {
                let nodes = storage.nodes;
                let edges = storage.edges;
                let iter = edges.into_par_iter();
                let filtered = match view.filter_state() {
                    FilterState::Neither => FilterVariants::Neither(iter),
                    FilterState::Both => FilterVariants::Both(iter.filter(move |e| {
                        let e = EdgeStorageRef::Mem(e);
                        view.filter_edge(e, view.layer_ids())
                            && view.filter_node(
                                NodeStorageRef::Mem(nodes.get(e.src())),
                                view.layer_ids(),
                            )
                            && view.filter_node(
                                NodeStorageRef::Mem(nodes.get(e.dst())),
                                view.layer_ids(),
                            )
                    })),
                    FilterState::Nodes => FilterVariants::Nodes(iter.filter(move |e| {
                        let e = EdgeStorageRef::Mem(e);
                        view.filter_node(NodeStorageRef::Mem(nodes.get(e.src())), view.layer_ids())
                            && view.filter_node(
                                NodeStorageRef::Mem(nodes.get(e.dst())),
                                view.layer_ids(),
                            )
                    })),
                    FilterState::Edges | FilterState::BothIndependent => {
                        FilterVariants::Edges(iter.filter(move |e| {
                            let e = EdgeStorageRef::Mem(e);
                            view.filter_edge(e, view.layer_ids())
                        }))
                    }
                };
                Either::Left(filtered.map(|e| EdgeRef::from(e)))
            }
            #[cfg(feature = "arrow")]
            GraphStorage::Arrow(storage) => {
                let storage_clone = storage.clone();
                let layer_ids = view.layer_ids().clone();
                let iter = match layer_ids {
                    LayerIds::None => LayerVariants::None,
                    LayerIds::All => {
                        LayerVariants::All((0..storage.layers.len()).into_par_iter().flat_map(
                            move |layer_id| {
                                storage_clone
                                    .layer(layer_id)
                                    .all_edge_ids_par()
                                    .map(move |e| (layer_id, e))
                            },
                        ))
                    }
                    LayerIds::One(layer_id) => LayerVariants::One(
                        storage
                            .layer(layer_id)
                            .all_edge_ids_par()
                            .map(move |e| (layer_id, e)),
                    ),
                    LayerIds::Multiple(ids) => {
                        let ids = ids.clone();
                        LayerVariants::Multiple((0..ids.len()).into_par_iter().flat_map(move |i| {
                            let layer_id = ids[i];
                            storage_clone
                                .layer(layer_id)
                                .all_edge_ids_par()
                                .map(move |e| (layer_id, e))
                        }))
                    }
                };
                let filtered = match view.filter_state() {
                    FilterState::Neither => {
                        FilterVariants::Neither(iter.map(move |(layer_id, eid)| {
                            EdgeStorageRef::Arrow(storage.layer(layer_id).edge(eid)).out_ref()
                        }))
                    }
                    FilterState::Both => {
                        FilterVariants::Both(iter.filter_map(move |(layer_id, eid)| {
                            let e = EdgeStorageRef::Arrow(storage.layer(layer_id).edge(eid));
                            if !view.filter_edge(e, view.layer_ids()) {
                                return None;
                            }
                            let src = NodeStorageRef::Arrow(ArrowNode::new(&storage, e.src()));
                            if !view.filter_node(src, view.layer_ids()) {
                                return None;
                            }
                            let dst = NodeStorageRef::Arrow(ArrowNode::new(&storage, e.dst()));
                            if !view.filter_node(dst, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                    FilterState::Nodes => {
                        FilterVariants::Nodes(iter.filter_map(move |(layer_id, eid)| {
                            let e = EdgeStorageRef::Arrow(storage.layer(layer_id).edge(eid));
                            let src = NodeStorageRef::Arrow(ArrowNode::new(&storage, e.src()));
                            if !view.filter_node(src, view.layer_ids()) {
                                return None;
                            }
                            let dst = NodeStorageRef::Arrow(ArrowNode::new(&storage, e.dst()));
                            if !view.filter_node(dst, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                    FilterState::Edges | FilterState::BothIndependent => {
                        FilterVariants::Edges(iter.filter_map(move |(layer_id, eid)| {
                            let e = EdgeStorageRef::Arrow(storage.layer(layer_id).edge(eid));
                            if !view.filter_edge(e, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                };
                Either::Right(filtered)
            }
        }
    }

    pub fn node_neighbours_iter<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        node: VID,
        dir: Direction,
        view: &'graph G,
    ) -> impl Iterator<Item = VID> + Send + 'graph {
        self.node_edges_iter(node, dir, view)
            .map(|e| e.remote())
            .dedup()
    }

    pub fn into_node_neighbours_iter<'graph, G: GraphViewOps<'graph>>(
        self,
        node: VID,
        dir: Direction,
        view: G,
    ) -> impl Iterator<Item = VID> + 'graph {
        self.into_node_edges_iter(node, dir, view)
            .map(|e| e.remote())
            .dedup()
    }

    #[inline]
    pub fn node_degree<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: VID,
        dir: Direction,
        view: &G,
    ) -> usize {
        if matches!(view.filter_state(), FilterState::Neither) {
            self.node(node).degree(dir, view.layer_ids())
        } else {
            self.node_neighbours_iter(node, dir, view).count()
        }
    }

    pub fn node_edges_iter<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        node: VID,
        dir: Direction,
        view: &'graph G,
    ) -> impl Iterator<Item = EdgeRef> + 'graph {
        let source = self.node(node);
        let layers = view.layer_ids();
        let iter = source.edges_iter(dir, layers);
        match view.filter_state() {
            FilterState::Neither => FilterVariants::Neither(iter),
            FilterState::Both => FilterVariants::Both(iter.filter(|e| {
                view.filter_edge(self.edge(e.pid()), view.layer_ids())
                    && view.filter_node(self.node(e.remote()), view.layer_ids())
            })),
            FilterState::Nodes => FilterVariants::Nodes(
                iter.filter(|e| view.filter_node(self.node(e.remote()), view.layer_ids())),
            ),
            FilterState::Edges | FilterState::BothIndependent => FilterVariants::Edges(
                iter.filter(|e| view.filter_edge(self.edge(e.pid()), view.layer_ids())),
            ),
        }
    }

    pub fn into_node_edges_iter<'graph, G: GraphViewOps<'graph>>(
        self,
        node: VID,
        dir: Direction,
        view: G,
    ) -> impl Iterator<Item = EdgeRef> + 'graph {
        let layers = view.layer_ids().clone();
        let iter = match &self {
            GraphStorage::Mem(storage) => {
                let local = storage.nodes.arc_entry(node);
                Either::Left(local.into_edges(&layers, dir))
            }
            GraphStorage::Arrow(storage) => {
                let node = ArrowNode::new(&storage, node);
                Either::Right(match dir {
                    Direction::OUT => DirectionVariants::Out(node.into_out_edges(layers)),
                    Direction::IN => DirectionVariants::In(node.into_in_edges(layers)),
                    Direction::BOTH => DirectionVariants::Both(node.into_edges(layers)),
                })
            }
        };

        match view.filter_state() {
            FilterState::Neither => FilterVariants::Neither(iter),
            FilterState::Both => FilterVariants::Both(iter.filter(move |e| {
                view.filter_edge(self.edge(e.pid()), view.layer_ids())
                    && view.filter_node(self.node(e.remote()), view.layer_ids())
            })),
            FilterState::Nodes => FilterVariants::Nodes(
                iter.filter(move |e| view.filter_node(self.node(e.remote()), view.layer_ids())),
            ),
            FilterState::Edges | FilterState::BothIndependent => FilterVariants::Edges(
                iter.filter(move |e| view.filter_edge(self.edge(e.pid()), view.layer_ids())),
            ),
        }
    }
}
