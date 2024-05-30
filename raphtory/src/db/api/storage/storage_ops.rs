#[cfg(feature = "arrow")]
use crate::{
    arrow::storage_interface::{
        edges::ArrowEdges,
        edges_ref::ArrowEdgesRef,
        node::{ArrowNode, ArrowOwnedNode},
        nodes::ArrowNodesOwned,
        nodes_ref::ArrowNodesRef,
    },
    db::api::storage::variants::storage_variants::StorageVariants,
};
#[cfg(feature = "arrow")]
use raphtory_arrow::graph::TemporalGraph;

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, EID, VID},
        Direction,
    },
    db::api::{
        storage::{
            edges::{
                edge_ref::EdgeStorageRef,
                edge_storage_ops::EdgeStorageOps,
                edges::{EdgesStorage, EdgesStorageRef},
            },
            locked::LockedGraph,
            nodes::{
                node_owned_entry::NodeOwnedEntry,
                node_ref::NodeStorageRef,
                node_storage_ops::{NodeStorageIntoOps, NodeStorageOps},
                nodes::NodesStorage,
                nodes_ref::NodesStorageRef,
            },
            variants::filter_variants::FilterVariants,
        },
        view::{
            internal::{FilterOps, FilterState, NodeList},
            IntoDynBoxed,
        },
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use rayon::prelude::*;
use std::{iter};

#[derive(Debug, Clone)]
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

    pub fn owned_nodes(&self) -> NodesStorage {
        match self {
            GraphStorage::Mem(storage) => NodesStorage::Mem(storage.nodes.clone()),
            #[cfg(feature = "arrow")]
            GraphStorage::Arrow(storage) => NodesStorage::Arrow(ArrowNodesOwned::new(storage)),
        }
    }

    pub fn node(&self, vid: VID) -> NodeStorageRef {
        match self {
            GraphStorage::Mem(storage) => NodeStorageRef::Mem(storage.nodes.get(vid)),
            #[cfg(feature = "arrow")]
            GraphStorage::Arrow(storage) => NodeStorageRef::Arrow(ArrowNode::new(storage, vid)),
        }
    }

    pub fn owned_node(&self, vid: VID) -> NodeOwnedEntry {
        match self {
            GraphStorage::Mem(storage) => NodeOwnedEntry::Mem(storage.nodes.arc_entry(vid)),
            #[cfg(feature = "arrow")]
            GraphStorage::Arrow(storage) => {
                NodeOwnedEntry::Arrow(ArrowOwnedNode::new(storage, vid))
            }
        }
    }

    pub fn edges(&self) -> EdgesStorageRef {
        match self {
            GraphStorage::Mem(storage) => EdgesStorageRef::Mem(&storage.edges),
            #[cfg(feature = "arrow")]
            GraphStorage::Arrow(storage) => EdgesStorageRef::Arrow(ArrowEdgesRef::new(storage)),
        }
    }

    pub fn owned_edges(&self) -> EdgesStorage {
        match self {
            GraphStorage::Mem(storage) => EdgesStorage::Mem(storage.edges.clone()),
            #[cfg(feature = "arrow")]
            GraphStorage::Arrow(storage) => EdgesStorage::Arrow(ArrowEdges::new(storage)),
        }
    }

    pub fn edge(&self, eid: EdgeRef) -> EdgeStorageRef {
        match self {
            GraphStorage::Mem(storage) => EdgeStorageRef::Mem(storage.edges.get(eid.pid())),
            #[cfg(feature = "arrow")]
            GraphStorage::Arrow(storage) => {
                let layer = eid
                    .layer()
                    .expect("arrow EdgeRefs should always have layer set");
                EdgeStorageRef::Arrow(storage.layers()[*layer].edge(eid.pid()))
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
                        .filter(move |&vid| {
                            view.filter_node(node_storage.node(vid), view.layer_ids())
                        })
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
        let edges = self.owned_edges();
        let nodes = self.owned_nodes();

        match edges {
            EdgesStorage::Mem(edges) => {
                let iter = (0..edges.len()).map(EID);
                let filtered = match view.filter_state() {
                    FilterState::Neither => {
                        FilterVariants::Neither(iter.map(move |eid| EdgeRef::from(edges.get(eid))))
                    }
                    FilterState::Both => FilterVariants::Both(iter.filter_map(move |e| {
                        let e = EdgeStorageRef::Mem(edges.get(e));
                        (view.filter_edge(e, view.layer_ids())
                            && view.filter_node(nodes.node_ref(e.src()), view.layer_ids())
                            && view.filter_node(nodes.node_ref(e.dst()), view.layer_ids()))
                        .then(|| e.out_ref())
                    })),
                    FilterState::Nodes => FilterVariants::Nodes(iter.filter_map(move |e| {
                        let e = EdgeStorageRef::Mem(edges.get(e));
                        (view.filter_node(nodes.node_ref(e.src()), view.layer_ids())
                            && view.filter_node(nodes.node_ref(e.dst()), view.layer_ids()))
                        .then(|| e.out_ref())
                    })),
                    FilterState::Edges | FilterState::BothIndependent => {
                        FilterVariants::Edges(iter.filter_map(move |e| {
                            let e = EdgeStorageRef::Mem(edges.get(e));
                            view.filter_edge(e, view.layer_ids()).then(|| e.out_ref())
                        }))
                    }
                };
                #[cfg(feature = "arrow")]
                {
                    StorageVariants::Mem(filtered)
                }
                #[cfg(not(feature = "arrow"))]
                {
                    filtered
                }
            }
            #[cfg(feature = "arrow")]
            EdgesStorage::Arrow(edges) => {
                let edges_clone = edges.clone();
                let iter = edges_clone.into_iter_refs(view.layer_ids().clone());
                let filtered = match view.filter_state() {
                    FilterState::Neither => FilterVariants::Neither(
                        iter.map(move |(eid, layer_id)| edges.get(eid, layer_id).out_ref()),
                    ),
                    FilterState::Both => {
                        FilterVariants::Both(iter.filter_map(move |(eid, layer_id)| {
                            let e = EdgeStorageRef::Arrow(edges.get(eid, layer_id));
                            if !view.filter_edge(e, view.layer_ids()) {
                                return None;
                            }
                            let src = nodes.node_ref(e.src());
                            if !view.filter_node(src, view.layer_ids()) {
                                return None;
                            }
                            let dst = nodes.node_ref(e.dst());
                            if !view.filter_node(dst, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                    FilterState::Nodes => {
                        FilterVariants::Nodes(iter.filter_map(move |(eid, layer_id)| {
                            let e = EdgeStorageRef::Arrow(edges.get(eid, layer_id));
                            let src = nodes.node_ref(e.src());
                            if !view.filter_node(src, view.layer_ids()) {
                                return None;
                            }
                            let dst = nodes.node_ref(e.dst());
                            if !view.filter_node(dst, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                    FilterState::Edges | FilterState::BothIndependent => {
                        FilterVariants::Edges(iter.filter_map(move |(eid, layer_id)| {
                            let e = EdgeStorageRef::Arrow(edges.get(eid, layer_id));
                            if !view.filter_edge(e, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                };
                StorageVariants::Arrow(filtered)
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
        let edges = self.owned_edges();
        let nodes = self.owned_nodes();

        match edges {
            EdgesStorage::Mem(edges) => {
                let iter = (0..edges.len()).into_par_iter().map(EID);
                let filtered = match view.filter_state() {
                    FilterState::Neither => {
                        FilterVariants::Neither(iter.map(move |eid| EdgeRef::from(edges.get(eid))))
                    }
                    FilterState::Both => FilterVariants::Both(iter.filter_map(move |e| {
                        let e = EdgeStorageRef::Mem(edges.get(e));
                        (view.filter_edge(e, view.layer_ids())
                            && view.filter_node(nodes.node_ref(e.src()), view.layer_ids())
                            && view.filter_node(nodes.node_ref(e.dst()), view.layer_ids()))
                        .then(|| e.out_ref())
                    })),
                    FilterState::Nodes => FilterVariants::Nodes(iter.filter_map(move |e| {
                        let e = EdgeStorageRef::Mem(edges.get(e));
                        (view.filter_node(nodes.node_ref(e.src()), view.layer_ids())
                            && view.filter_node(nodes.node_ref(e.dst()), view.layer_ids()))
                        .then(|| e.out_ref())
                    })),
                    FilterState::Edges | FilterState::BothIndependent => {
                        FilterVariants::Edges(iter.filter_map(move |e| {
                            let e = EdgeStorageRef::Mem(edges.get(e));
                            view.filter_edge(e, view.layer_ids()).then(|| e.out_ref())
                        }))
                    }
                };
                #[cfg(feature = "arrow")]
                {
                    StorageVariants::Mem(filtered)
                }
                #[cfg(not(feature = "arrow"))]
                {
                    filtered
                }
            }
            #[cfg(feature = "arrow")]
            EdgesStorage::Arrow(edges) => {
                let edges_clone = edges.clone();
                let iter = edges_clone.into_par_iter_refs(view.layer_ids().clone());
                let filtered = match view.filter_state() {
                    FilterState::Neither => {
                        FilterVariants::Neither(iter.map(move |(eid, layer_id)| {
                            EdgeStorageRef::Arrow(edges.get(eid, layer_id)).out_ref()
                        }))
                    }
                    FilterState::Both => {
                        FilterVariants::Both(iter.filter_map(move |(eid, layer_id)| {
                            let e = EdgeStorageRef::Arrow(edges.get(eid, layer_id));
                            if !view.filter_edge(e, view.layer_ids()) {
                                return None;
                            }
                            let src = nodes.node_ref(e.src());
                            if !view.filter_node(src, view.layer_ids()) {
                                return None;
                            }
                            let dst = nodes.node_ref(e.dst());
                            if !view.filter_node(dst, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                    FilterState::Nodes => {
                        FilterVariants::Nodes(iter.filter_map(move |(eid, layer_id)| {
                            let e = EdgeStorageRef::Arrow(edges.get(eid, layer_id));
                            let src = nodes.node_ref(e.src());
                            if !view.filter_node(src, view.layer_ids()) {
                                return None;
                            }
                            let dst = nodes.node_ref(e.dst());
                            if !view.filter_node(dst, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                    FilterState::Edges | FilterState::BothIndependent => {
                        FilterVariants::Edges(iter.filter_map(move |(eid, layer_id)| {
                            let e = EdgeStorageRef::Arrow(edges.get(eid, layer_id));
                            if !view.filter_edge(e, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                };
                StorageVariants::Arrow(filtered)
            }
        }
    }

    pub fn node_neighbours_iter<'a, 'graph: 'a, G: GraphViewOps<'graph>>(
        &'a self,
        node: VID,
        dir: Direction,
        view: &'a G,
    ) -> impl Iterator<Item = VID> + Send + '_ {
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
            self.node(node).degree(view.layer_ids(), dir)
        } else {
            self.node_neighbours_iter(node, dir, view).count()
        }
    }

    pub fn node_edges_iter<'a, 'graph: 'a, G: GraphViewOps<'graph>>(
        &'a self,
        node: VID,
        dir: Direction,
        view: &'a G,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        let source = self.node(node);
        let layers = view.layer_ids();
        let iter = source.edges_iter(layers, dir);
        match view.filter_state() {
            FilterState::Neither => FilterVariants::Neither(iter),
            FilterState::Both => FilterVariants::Both(iter.filter(|&e| {
                view.filter_edge(self.edge(e), view.layer_ids())
                    && view.filter_node(self.node(e.remote()), view.layer_ids())
            })),
            FilterState::Nodes => FilterVariants::Nodes(
                iter.filter(|e| view.filter_node(self.node(e.remote()), view.layer_ids())),
            ),
            FilterState::Edges | FilterState::BothIndependent => FilterVariants::Edges(
                iter.filter(|&e| view.filter_edge(self.edge(e), view.layer_ids())),
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
        let local = self.owned_node(node);
        let iter = local.into_edges_iter(layers, dir);

        match view.filter_state() {
            FilterState::Neither => FilterVariants::Neither(iter),
            FilterState::Both => FilterVariants::Both(iter.filter(move |&e| {
                view.filter_edge(self.edge(e), view.layer_ids())
                    && view.filter_node(self.node(e.remote()), view.layer_ids())
            })),
            FilterState::Nodes => FilterVariants::Nodes(
                iter.filter(move |e| view.filter_node(self.node(e.remote()), view.layer_ids())),
            ),
            FilterState::Edges | FilterState::BothIndependent => FilterVariants::Edges(
                iter.filter(move |&e| view.filter_edge(self.edge(e), view.layer_ids())),
            ),
        }
    }
}
