use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, graph::tgraph::InternalGraph, LayerIds, EID, VID},
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
                node_storage_ops::{NodeStorageIntoOps, NodeStorageOps},
                nodes::NodesStorage,
                nodes_ref::NodesStorageEntry,
            },
            variants::filter_variants::FilterVariants,
        },
        view::internal::{FilterOps, FilterState, NodeList},
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use rayon::prelude::*;
use std::{iter, sync::Arc};

#[cfg(feature = "storage")]
use crate::{
    db::api::storage::variants::storage_variants::StorageVariants,
    disk_graph::storage_interface::{
        edges::DiskEdges,
        edges_ref::DiskEdgesRef,
        node::{DiskNode, DiskOwnedNode},
        nodes::DiskNodesOwned,
        nodes_ref::DiskNodesRef,
    },
};
#[cfg(feature = "storage")]
use pometry_storage::graph::TemporalGraph;

use super::{
    edges::edge_entry::EdgeStorageEntry,
    nodes::{node_entry::NodeStorageEntry, unlocked::UnlockedEdges},
};

#[derive(Debug, Clone)]
pub enum GraphStorage {
    Mem(LockedGraph),
    Unlocked(InternalGraph),
    #[cfg(feature = "storage")]
    Disk(Arc<TemporalGraph>),
}

impl GraphStorage {
    pub fn lock(self) -> Self {
        match self {
            GraphStorage::Unlocked(storage) => GraphStorage::Mem(storage.lock()),
            _ => self,
        }
    }

    pub fn nodes(&self) -> NodesStorageEntry {
        match self {
            GraphStorage::Mem(storage) => NodesStorageEntry::Mem(&storage.nodes),
            GraphStorage::Unlocked(storage) => {
                NodesStorageEntry::Unlocked(storage.inner().storage.nodes.read_lock())
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => NodesStorageEntry::Disk(DiskNodesRef::new(storage)),
        }
    }

    pub fn owned_nodes(&self) -> NodesStorage {
        match self {
            GraphStorage::Mem(storage) => NodesStorage::Mem(storage.nodes.clone()),
            GraphStorage::Unlocked(storage) => NodesStorage::Mem(storage.lock().nodes.clone()),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => NodesStorage::Disk(DiskNodesOwned::new(storage.clone())),
        }
    }

    #[inline(always)]
    pub fn node(&self, vid: VID) -> NodeStorageEntry {
        match self {
            GraphStorage::Mem(storage) => NodeStorageEntry::Mem(storage.nodes.get(vid)),
            GraphStorage::Unlocked(storage) => {
                NodeStorageEntry::Unlocked(storage.inner().node_entry(vid))
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => NodeStorageEntry::Disk(DiskNode::new(storage, vid)),
        }
    }

    pub fn owned_node(&self, vid: VID) -> NodeOwnedEntry {
        match self {
            GraphStorage::Mem(storage) => NodeOwnedEntry::Mem(storage.nodes.arc_entry(vid)),
            GraphStorage::Unlocked(storage) => {
                NodeOwnedEntry::Mem(storage.inner().storage.nodes.entry_arc(vid))
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => {
                NodeOwnedEntry::Disk(DiskOwnedNode::new(storage.clone(), vid))
            }
        }
    }

    pub fn edges(&self) -> EdgesStorageRef {
        match self {
            GraphStorage::Mem(storage) => EdgesStorageRef::Mem(&storage.edges),
            GraphStorage::Unlocked(storage) => EdgesStorageRef::Unlocked(UnlockedEdges(storage)),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => EdgesStorageRef::Disk(DiskEdgesRef::new(storage)),
        }
    }

    pub fn owned_edges(&self) -> EdgesStorage {
        match self {
            GraphStorage::Mem(storage) => EdgesStorage::Mem(storage.edges.clone()),
            GraphStorage::Unlocked(storage) => GraphStorage::Mem(storage.lock()).owned_edges(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => EdgesStorage::Disk(DiskEdges::new(storage)),
        }
    }

    pub fn edge(&self, eid: EdgeRef) -> EdgeStorageEntry {
        match self {
            GraphStorage::Mem(storage) => EdgeStorageEntry::Mem(storage.edges.get(eid.pid())),
            GraphStorage::Unlocked(storage) => {
                EdgeStorageEntry::Unlocked(storage.inner().edge_entry(eid.pid()))
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => {
                let layer = eid
                    .layer()
                    .expect("disk_graph EdgeRefs should always have layer set");
                EdgeStorageEntry::Disk(storage.layers()[*layer].edge(eid.pid()))
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

    pub fn into_nodes_iter<'graph, G: GraphViewOps<'graph>>(
        self,
        view: G,
        type_filter: Option<Arc<[bool]>>,
    ) -> Box<dyn Iterator<Item = VID> + Send + 'graph> {
        let iter = view.node_list().into_iter();
        match type_filter {
            None => {
                if view.node_list_trusted() {
                    iter
                } else {
                    Box::new(iter.filter(move |&vid| {
                        view.filter_node(self.node(vid).as_ref(), view.layer_ids())
                    }))
                }
            }
            Some(type_filter) => {
                if view.node_list_trusted() {
                    Box::new(iter.filter(move |&vid| type_filter[self.node(vid).node_type_id()]))
                } else {
                    Box::new(iter.filter(move |&vid| {
                        let node = self.node(vid);
                        type_filter[node.node_type_id()]
                            && view.filter_node(node.as_ref(), view.layer_ids())
                    }))
                }
            }
        }
    }

    pub fn nodes_par<'a, 'graph: 'a, G: GraphViewOps<'graph>>(
        &'a self,
        view: &'a G,
        type_filter: Option<&'a Arc<[bool]>>,
    ) -> impl ParallelIterator<Item = VID> + 'a {
        view.node_list().into_par_iter().filter(move |&vid| {
            let node = self.node(vid);
            type_filter.map_or(true, |type_filter| type_filter[node.node_type_id()])
                && view.filter_node(node.as_ref(), view.layer_ids())
        })
    }

    pub fn into_nodes_par<'graph, G: GraphViewOps<'graph>>(
        self,
        view: G,
        type_filter: Option<Arc<[bool]>>,
    ) -> impl ParallelIterator<Item = VID> + 'graph {
        view.node_list().into_par_iter().filter(move |&vid| {
            let node = self.node(vid);
            let n = node.name();
            let i = node.node_type_id();
            let r = type_filter
                .as_ref()
                .map_or(true, |type_filter| type_filter[node.node_type_id()]);
            let s = view.filter_node(self.node(vid).as_ref(), view.layer_ids());

            println!("name = {:?}, id = {}, r = {}, s = {}", n, i, r, s);

            r && s
        })
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
                FilterVariants::Both(iter.filter(move |e| {
                    view.filter_edge(e.as_ref(), view.layer_ids())
                        && view.filter_node(nodes.node(e.src()), view.layer_ids())
                        && view.filter_node(nodes.node(e.dst()), view.layer_ids())
                }))
            }
            FilterState::Nodes => {
                let nodes = self.nodes();
                FilterVariants::Nodes(iter.filter(move |e| {
                    view.filter_node(nodes.node(e.src()), view.layer_ids())
                        && view.filter_node(nodes.node(e.dst()), view.layer_ids())
                }))
            }
            FilterState::Edges | FilterState::BothIndependent => FilterVariants::Edges(
                iter.filter(|e| view.filter_edge(e.as_ref(), view.layer_ids())),
            ),
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
                        FilterVariants::Neither(iter.map(move |eid| edges.get(eid).as_edge_ref()))
                    }
                    FilterState::Both => FilterVariants::Both(iter.filter_map(move |e| {
                        let e = EdgeStorageRef::Mem(edges.get(e));
                        (view.filter_edge(e, view.layer_ids())
                            && view.filter_node(nodes.node_entry(e.src()), view.layer_ids())
                            && view.filter_node(nodes.node_entry(e.dst()), view.layer_ids()))
                        .then(|| e.out_ref())
                    })),
                    FilterState::Nodes => FilterVariants::Nodes(iter.filter_map(move |e| {
                        let e = EdgeStorageRef::Mem(edges.get(e));
                        (view.filter_node(nodes.node_entry(e.src()), view.layer_ids())
                            && view.filter_node(nodes.node_entry(e.dst()), view.layer_ids()))
                        .then(|| e.out_ref())
                    })),
                    FilterState::Edges | FilterState::BothIndependent => {
                        FilterVariants::Edges(iter.filter_map(move |e| {
                            let e = EdgeStorageRef::Mem(edges.get(e));
                            view.filter_edge(e, view.layer_ids()).then(|| e.out_ref())
                        }))
                    }
                };
                #[cfg(feature = "storage")]
                {
                    StorageVariants::Mem(filtered)
                }
                #[cfg(not(feature = "storage"))]
                {
                    filtered
                }
            }
            #[cfg(feature = "storage")]
            EdgesStorage::Disk(edges) => {
                let edges_clone = edges.clone();
                let iter = edges_clone.into_iter_refs(view.layer_ids().clone());
                let filtered = match view.filter_state() {
                    FilterState::Neither => FilterVariants::Neither(
                        iter.map(move |(eid, layer_id)| edges.get(eid, layer_id).out_ref()),
                    ),
                    FilterState::Both => {
                        FilterVariants::Both(iter.filter_map(move |(eid, layer_id)| {
                            let e = EdgeStorageRef::Disk(edges.get(eid, layer_id));
                            if !view.filter_edge(e, view.layer_ids()) {
                                return None;
                            }
                            let src = nodes.node_entry(e.src());
                            if !view.filter_node(src, view.layer_ids()) {
                                return None;
                            }
                            let dst = nodes.node_entry(e.dst());
                            if !view.filter_node(dst, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                    FilterState::Nodes => {
                        FilterVariants::Nodes(iter.filter_map(move |(eid, layer_id)| {
                            let e = EdgeStorageRef::Disk(edges.get(eid, layer_id));
                            let src = nodes.node_entry(e.src());
                            if !view.filter_node(src, view.layer_ids()) {
                                return None;
                            }
                            let dst = nodes.node_entry(e.dst());
                            if !view.filter_node(dst, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                    FilterState::Edges | FilterState::BothIndependent => {
                        FilterVariants::Edges(iter.filter_map(move |(eid, layer_id)| {
                            let e = EdgeStorageRef::Disk(edges.get(eid, layer_id));
                            if !view.filter_edge(e, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                };
                StorageVariants::Disk(filtered)
            }
        }
    }

    pub fn edges_par<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        view: &'graph G,
    ) -> impl ParallelIterator<Item = EdgeRef> + 'graph {
        self.edges()
            .par_iter(view.layer_ids().clone())
            .filter(|edge| match view.filter_state() {
                FilterState::Neither => true,
                FilterState::Both => {
                    let layer_ids = view.layer_ids();
                    let src = self.node(edge.src());
                    let dst = self.node(edge.dst());
                    view.filter_edge(edge.as_ref(), view.layer_ids())
                        && view.filter_node(src.as_ref(), layer_ids)
                        && view.filter_node(dst.as_ref(), layer_ids)
                }
                FilterState::Nodes => {
                    let layer_ids = view.layer_ids();
                    let src = self.node(edge.src());
                    let dst = self.node(edge.dst());
                    view.filter_node(src.as_ref(), layer_ids)
                        && view.filter_node(dst.as_ref(), layer_ids)
                }
                FilterState::Edges | FilterState::BothIndependent => {
                    view.filter_edge(edge.as_ref(), view.layer_ids())
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
                        FilterVariants::Neither(iter.map(move |eid| edges.get(eid).as_edge_ref()))
                    }
                    FilterState::Both => FilterVariants::Both(iter.filter_map(move |e| {
                        let e = EdgeStorageRef::Mem(edges.get(e));
                        (view.filter_edge(e, view.layer_ids())
                            && view.filter_node(nodes.node_entry(e.src()), view.layer_ids())
                            && view.filter_node(nodes.node_entry(e.dst()), view.layer_ids()))
                        .then(|| e.out_ref())
                    })),
                    FilterState::Nodes => FilterVariants::Nodes(iter.filter_map(move |e| {
                        let e = EdgeStorageRef::Mem(edges.get(e));
                        (view.filter_node(nodes.node_entry(e.src()), view.layer_ids())
                            && view.filter_node(nodes.node_entry(e.dst()), view.layer_ids()))
                        .then(|| e.out_ref())
                    })),
                    FilterState::Edges | FilterState::BothIndependent => {
                        FilterVariants::Edges(iter.filter_map(move |e| {
                            let e = EdgeStorageRef::Mem(edges.get(e));
                            view.filter_edge(e, view.layer_ids()).then(|| e.out_ref())
                        }))
                    }
                };
                #[cfg(feature = "storage")]
                {
                    StorageVariants::Mem(filtered)
                }
                #[cfg(not(feature = "storage"))]
                {
                    filtered
                }
            }
            #[cfg(feature = "storage")]
            EdgesStorage::Disk(edges) => {
                let edges_clone = edges.clone();
                let iter = edges_clone.into_par_iter_refs(view.layer_ids().clone());
                let filtered = match view.filter_state() {
                    FilterState::Neither => {
                        FilterVariants::Neither(iter.map(move |(eid, layer_id)| {
                            EdgeStorageRef::Disk(edges.get(eid, layer_id)).out_ref()
                        }))
                    }
                    FilterState::Both => {
                        FilterVariants::Both(iter.filter_map(move |(eid, layer_id)| {
                            let e = EdgeStorageRef::Disk(edges.get(eid, layer_id));
                            if !view.filter_edge(e, view.layer_ids()) {
                                return None;
                            }
                            let src = nodes.node_entry(e.src());
                            if !view.filter_node(src, view.layer_ids()) {
                                return None;
                            }
                            let dst = nodes.node_entry(e.dst());
                            if !view.filter_node(dst, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                    FilterState::Nodes => {
                        FilterVariants::Nodes(iter.filter_map(move |(eid, layer_id)| {
                            let e = EdgeStorageRef::Disk(edges.get(eid, layer_id));
                            let src = nodes.node_entry(e.src());
                            if !view.filter_node(src, view.layer_ids()) {
                                return None;
                            }
                            let dst = nodes.node_entry(e.dst());
                            if !view.filter_node(dst, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                    FilterState::Edges | FilterState::BothIndependent => {
                        FilterVariants::Edges(iter.filter_map(move |(eid, layer_id)| {
                            let e = EdgeStorageRef::Disk(edges.get(eid, layer_id));
                            if !view.filter_edge(e, view.layer_ids()) {
                                return None;
                            }
                            Some(e.out_ref())
                        }))
                    }
                };
                StorageVariants::Disk(filtered)
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
        let iter = source.into_edges_iter(layers, dir);
        match view.filter_state() {
            FilterState::Neither => FilterVariants::Neither(iter),
            FilterState::Both => FilterVariants::Both(iter.filter(|&e| {
                view.filter_edge(self.edge(e).as_ref(), view.layer_ids())
                    && view.filter_node(self.node(e.remote()).as_ref(), view.layer_ids())
            })),
            FilterState::Nodes => FilterVariants::Nodes(
                iter.filter(|e| view.filter_node(self.node(e.remote()).as_ref(), view.layer_ids())),
            ),
            FilterState::Edges | FilterState::BothIndependent => FilterVariants::Edges(
                iter.filter(|&e| view.filter_edge(self.edge(e).as_ref(), view.layer_ids())),
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
                view.filter_edge(self.edge(e).as_ref(), view.layer_ids())
                    && view.filter_node(self.node(e.remote()).as_ref(), view.layer_ids())
            })),
            FilterState::Nodes => FilterVariants::Nodes(iter.filter(move |e| {
                view.filter_node(self.node(e.remote()).as_ref(), view.layer_ids())
            })),
            FilterState::Edges | FilterState::BothIndependent => FilterVariants::Edges(
                iter.filter(move |&e| view.filter_edge(self.edge(e).as_ref(), view.layer_ids())),
            ),
        }
    }
}
