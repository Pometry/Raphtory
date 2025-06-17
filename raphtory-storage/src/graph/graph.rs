use super::{
    edges::{edge_entry::EdgeStorageEntry, unlocked::UnlockedEdges},
    nodes::node_entry::NodeStorageEntry,
};
use crate::graph::{
    edges::edges::{EdgesStorage, EdgesStorageRef},
    locked::LockedGraph,
    nodes::{nodes::NodesStorage, nodes_ref::NodesStorageEntry},
};
use db4_graph::{ReadLockedTemporalGraph, TemporalGraph};
use raphtory_api::core::entities::{properties::meta::Meta, LayerIds, LayerVariants, EID, VID};
use raphtory_core::entities::{nodes::node_ref::NodeRef, properties::graph_meta::GraphMeta};
use std::{fmt::Debug, iter, sync::Arc};
use thiserror::Error;

#[cfg(feature = "storage")]
use crate::disk::{
    storage_interface::{
        edges::DiskEdges, edges_ref::DiskEdgesRef, node::DiskNode, nodes::DiskNodesOwned,
        nodes_ref::DiskNodesRef,
    },
    DiskGraphStorage,
};
use crate::mutation::MutationError;

#[derive(Clone, Debug)]
pub enum GraphStorage {
    Mem(Arc<ReadLockedTemporalGraph>),
    Unlocked(Arc<TemporalGraph>),
    #[cfg(feature = "storage")]
    Disk(Arc<DiskGraphStorage>),
}

#[derive(Error, Debug)]
pub enum Immutable {
    #[error("The graph is locked and cannot be mutated")]
    ReadLockedImmutable,
    #[cfg(feature = "storage")]
    #[error("DiskGraph cannot be mutated")]
    DiskGraphImmutable,
}

impl From<TemporalGraph> for GraphStorage {
    fn from(value: TemporalGraph) -> Self {
        Self::Unlocked(Arc::new(value))
    }
}

impl Default for GraphStorage {
    fn default() -> Self {
        // GraphStorage::Unlocked(Arc::new(TemporalGraph::default()))
        todo!("does this even make sense? GraphStorage::default() is not a valid graph, it should be created with a valid graph directory");
    }
}

impl std::fmt::Display for GraphStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Graph(num_nodes={}, num_edges={})",
            self.unfiltered_num_nodes(),
            self.unfiltered_num_edges(),
        )
    }
}

impl GraphStorage {
    /// Check if two storage instances point at the same underlying storage
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match self {
            GraphStorage::Mem(locked_graph) => match other {
                GraphStorage::Mem(other_graph) => {
                    Arc::ptr_eq(locked_graph.graph(), other_graph.graph())
                }
                #[cfg(feature = "storage")]
                GraphStorage::Disk(_) => false,
                GraphStorage::Unlocked(other_graph) => {
                    Arc::ptr_eq(locked_graph.graph(), other_graph)
                }
            },
            GraphStorage::Unlocked(this_graph) => match other {
                GraphStorage::Mem(other_graph) => Arc::ptr_eq(this_graph, other_graph.graph()),
                GraphStorage::Unlocked(other_graph) => Arc::ptr_eq(this_graph, other_graph),
                #[cfg(feature = "storage")]
                GraphStorage::Disk(_) => false,
            },
            #[cfg(feature = "storage")]
            GraphStorage::Disk(this_graph) => match other {
                GraphStorage::Disk(other_graph) => Arc::ptr_eq(this_graph, other_graph),
                _ => false,
            },
        }
    }

    pub fn mutable(&self) -> Result<&Arc<TemporalGraph>, MutationError> {
        match self {
            GraphStorage::Mem(_) => Err(Immutable::ReadLockedImmutable)?,
            GraphStorage::Unlocked(graph) => Ok(graph),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(_) => Err(Immutable::DiskGraphImmutable)?,
        }
    }

    #[inline(always)]
    pub fn is_immutable(&self) -> bool {
        match self {
            GraphStorage::Mem(_) => true,
            GraphStorage::Unlocked(_) => false,
            #[cfg(feature = "storage")]
            GraphStorage::Disk(_) => true,
        }
    }

    #[inline(always)]
    pub fn lock(&self) -> Self {
        match self {
            GraphStorage::Unlocked(storage) => GraphStorage::Mem(storage.read_locked().into()),
            _ => self.clone(),
        }
    }

    #[inline(always)]
    pub fn nodes(&self) -> NodesStorageEntry {
        match self {
            GraphStorage::Mem(storage) => NodesStorageEntry::Mem(storage.as_ref()),
            GraphStorage::Unlocked(storage) => NodesStorageEntry::Unlocked(storage.read_locked()),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => {
                NodesStorageEntry::Disk(DiskNodesRef::new(&storage.inner))
            }
        }
    }

    #[inline(always)]
    pub fn internalise_node(&self, v: NodeRef) -> Option<VID> {
        match v {
            NodeRef::Internal(vid) => Some(vid),
            node_ref => match self {
                GraphStorage::Mem(locked) => locked.graph().resolve_node_ref(node_ref),
                GraphStorage::Unlocked(unlocked) => unlocked.resolve_node_ref(node_ref),
                #[cfg(feature = "storage")]
                GraphStorage::Disk(storage) => match v {
                    NodeRef::External(id) => storage.inner.find_node(id),
                    _ => unreachable!("VID is handled above!"),
                },
            },
        }
    }

    #[inline(always)]
    pub fn unfiltered_num_nodes(&self) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph().internal_num_nodes(),
            GraphStorage::Unlocked(storage) => storage.internal_num_nodes(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.num_nodes(),
        }
    }

    #[inline(always)]
    pub fn unfiltered_num_edges(&self) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph().internal_num_edges(),
            GraphStorage::Unlocked(storage) => storage.internal_num_edges(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.count_edges(),
        }
    }

    #[inline(always)]
    pub fn unfiltered_num_layers(&self) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph().num_layers(),
            GraphStorage::Unlocked(storage) => storage.num_layers(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.layers().len(),
        }
    }

    #[inline(always)]
    pub fn core_nodes(&self) -> NodesStorage {
        match self {
            GraphStorage::Mem(storage) => NodesStorage::Mem(storage.clone()),
            GraphStorage::Unlocked(storage) => NodesStorage::Mem(storage.read_locked().into()),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => {
                NodesStorage::Disk(DiskNodesOwned::new(storage.inner.clone()))
            }
        }
    }

    #[inline(always)]
    pub fn core_node<'a>(&'a self, vid: VID) -> NodeStorageEntry<'a> {
        match self {
            GraphStorage::Mem(storage) => NodeStorageEntry::Mem(storage.node(vid)),
            GraphStorage::Unlocked(storage) => NodeStorageEntry::Unlocked(storage.node(vid)),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => {
                NodeStorageEntry::Disk(DiskNode::new(&storage.inner, vid))
            }
        }
    }

    #[inline(always)]
    pub fn edges(&self) -> EdgesStorageRef {
        match self {
            GraphStorage::Mem(storage) => EdgesStorageRef::Mem(&storage.edges),
            GraphStorage::Unlocked(storage) => {
                EdgesStorageRef::Unlocked(UnlockedEdges(&storage.storage))
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => EdgesStorageRef::Disk(DiskEdgesRef::new(&storage.inner)),
        }
    }

    #[inline(always)]
    pub fn owned_edges(&self) -> EdgesStorage {
        match self {
            GraphStorage::Mem(storage) => EdgesStorage::Mem(storage.edges.clone()),
            GraphStorage::Unlocked(storage) => {
                GraphStorage::Mem(LockedGraph::new(storage.clone())).owned_edges()
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => EdgesStorage::Disk(DiskEdges::new(storage)),
        }
    }

    #[inline(always)]
    pub fn edge_entry(&self, eid: EID) -> EdgeStorageEntry {
        match self {
            GraphStorage::Mem(storage) => EdgeStorageEntry::Mem(storage.edges.get_mem(eid)),
            GraphStorage::Unlocked(storage) => {
                EdgeStorageEntry::Unlocked(storage.storage.edge_entry(eid))
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => EdgeStorageEntry::Disk(storage.inner.edge(eid)),
        }
    }

    pub fn layer_ids_iter(&self, layer_ids: &LayerIds) -> impl Iterator<Item = usize> {
        match layer_ids {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => LayerVariants::All(0..self.unfiltered_num_layers()),
            LayerIds::One(id) => LayerVariants::One(iter::once(*id)),
            LayerIds::Multiple(ids) => LayerVariants::Multiple(ids.clone().into_iter()),
        }
    }
    //
    // pub fn into_nodes_iter<'graph, G: GraphViewOps<'graph>>(
    //     self,
    //     view: G,
    //     node_list: NodeList,
    //     type_filter: Option<Arc<[bool]>>,
    // ) -> BoxedLIter<'graph, VID> {
    //     node_list
    //         .into_iter()
    //         .filter(move |&vid| {
    //             let node = self.node_entry(vid);
    //             type_filter
    //                 .as_ref()
    //                 .map_or(true, |type_filter| type_filter[node.node_type_id()])
    //                 && view.filter_node(node.as_ref())
    //         })
    //         .into_dyn_boxed()
    // }
    //
    // pub fn nodes_par<'a, 'graph: 'a, G: GraphViewOps<'graph>>(
    //     &'a self,
    //     view: &'a G,
    //     type_filter: Option<&'a Arc<[bool]>>,
    // ) -> impl ParallelIterator<Item = VID> + 'a {
    //     let nodes = self.nodes();
    //     view.node_list().into_par_iter().filter(move |&vid| {
    //         let node = nodes.node(vid);
    //         type_filter.map_or(true, |type_filter| type_filter[node.node_type_id()])
    //             && view.filter_node(node)
    //     })
    // }
    //
    // pub fn into_nodes_par<'graph, G: GraphViewOps<'graph>>(
    //     self,
    //     view: G,
    //     node_list: NodeList,
    //     type_filter: Option<Arc<[bool]>>,
    // ) -> impl ParallelIterator<Item = VID> + 'graph {
    //     node_list.into_par_iter().filter(move |&vid| {
    //         let node = self.node_entry(vid);
    //         type_filter
    //             .as_ref()
    //             .map_or(true, |type_filter| type_filter[node.node_type_id()])
    //             && view.filter_node(node.as_ref())
    //     })
    // }
    //
    // pub fn edges_iter<'graph, G: GraphViewOps<'graph>>(
    //     &'graph self,
    //     view: &'graph G,
    // ) -> impl Iterator<Item = EdgeRef> + Send + 'graph {
    //     let iter = self.edges().iter(view.layer_ids());
    //
    //     let filtered = match view.filter_state() {
    //         FilterState::Neither => FilterVariants::Neither(iter),
    //         FilterState::Both => {
    //             let nodes = self.nodes();
    //             FilterVariants::Both(iter.filter(move |e| {
    //                 view.filter_edge(e.as_ref(), view.layer_ids())
    //                     && view.filter_node(nodes.node(e.src()))
    //                     && view.filter_node(nodes.node(e.dst()))
    //             }))
    //         }
    //         FilterState::Nodes => {
    //             let nodes = self.nodes();
    //             FilterVariants::Nodes(iter.filter(move |e| {
    //                 view.filter_node(nodes.node(e.src())) && view.filter_node(nodes.node(e.dst()))
    //             }))
    //         }
    //         FilterState::Edges | FilterState::BothIndependent => FilterVariants::Edges(
    //             iter.filter(|e| view.filter_edge(e.as_ref(), view.layer_ids())),
    //         ),
    //     };
    //     filtered.map(|e| e.out_ref())
    // }
    //
    // pub fn into_edges_iter<'graph, G: GraphViewOps<'graph>>(
    //     self,
    //     view: G,
    // ) -> impl Iterator<Item = EdgeRef> + Send + 'graph {
    //     match view.node_list() {
    //         NodeList::List { elems } => {
    //             return elems
    //                 .into_iter()
    //                 .flat_map(move |v| {
    //                     self.clone()
    //                         .into_node_edges_iter(v, Direction::OUT, view.clone())
    //                 })
    //                 .into_dyn_boxed()
    //         }
    //         _ => {}
    //     }
    //     let edges = self.owned_edges();
    //     let nodes = self.owned_nodes();
    //
    //     match edges {
    //         EdgesStorage::Mem(edges) => {
    //             let iter = (0..edges.len()).map(EID);
    //             let filtered = match view.filter_state() {
    //                 FilterState::Neither => {
    //                     FilterVariants::Neither(iter.map(move |eid| edges.get_mem(eid).out_ref()))
    //                 }
    //                 FilterState::Both => FilterVariants::Both(iter.filter_map(move |e| {
    //                     let e = EdgeStorageRef::Mem(edges.get_mem(e));
    //                     (view.filter_edge(e, view.layer_ids())
    //                         && view.filter_node(nodes.node_entry(e.src()))
    //                         && view.filter_node(nodes.node_entry(e.dst())))
    //                     .then(|| e.out_ref())
    //                 })),
    //                 FilterState::Nodes => FilterVariants::Nodes(iter.filter_map(move |e| {
    //                     let e = EdgeStorageRef::Mem(edges.get_mem(e));
    //                     (view.filter_node(nodes.node_entry(e.src()))
    //                         && view.filter_node(nodes.node_entry(e.dst())))
    //                     .then(|| e.out_ref())
    //                 })),
    //                 FilterState::Edges | FilterState::BothIndependent => {
    //                     FilterVariants::Edges(iter.filter_map(move |e| {
    //                         let e = EdgeStorageRef::Mem(edges.get_mem(e));
    //                         view.filter_edge(e, view.layer_ids()).then(|| e.out_ref())
    //                     }))
    //                 }
    //             };
    //             filtered.into_dyn_boxed()
    //         }
    //         #[cfg(feature = "storage")]
    //         EdgesStorage::Disk(edges) => {
    //             let edges_clone = edges.clone();
    //             let iter = edges_clone.into_iter_refs(view.layer_ids().clone());
    //             let filtered = match view.filter_state() {
    //                 FilterState::Neither => FilterVariants::Neither(iter),
    //                 FilterState::Both => FilterVariants::Both(iter.filter_map(move |e| {
    //                     let edge = EdgeStorageRef::Disk(edges.get(e.pid()));
    //                     if !view.filter_edge(edge, view.layer_ids()) {
    //                         return None;
    //                     }
    //                     let src = nodes.node_entry(e.src());
    //                     if !view.filter_node(src) {
    //                         return None;
    //                     }
    //                     let dst = nodes.node_entry(e.dst());
    //                     if !view.filter_node(dst) {
    //                         return None;
    //                     }
    //                     Some(e)
    //                 })),
    //                 FilterState::Nodes => FilterVariants::Nodes(iter.filter_map(move |e| {
    //                     let src = nodes.node_entry(e.src());
    //                     if !view.filter_node(src) {
    //                         return None;
    //                     }
    //                     let dst = nodes.node_entry(e.dst());
    //                     if !view.filter_node(dst) {
    //                         return None;
    //                     }
    //                     Some(e)
    //                 })),
    //                 FilterState::Edges | FilterState::BothIndependent => {
    //                     FilterVariants::Edges(iter.filter_map(move |e| {
    //                         let edge = EdgeStorageRef::Disk(edges.get(e.pid()));
    //                         if !view.filter_edge(edge, view.layer_ids()) {
    //                             return None;
    //                         }
    //                         Some(e)
    //                     }))
    //                 }
    //             };
    //             filtered.into_dyn_boxed()
    //         }
    //     }
    // }
    //
    // pub fn edges_par<'graph, G: GraphViewOps<'graph>>(
    //     &'graph self,
    //     view: &'graph G,
    // ) -> impl ParallelIterator<Item = EdgeRef> + 'graph {
    //     self.edges()
    //         .par_iter(view.layer_ids())
    //         .filter(|edge| match view.filter_state() {
    //             FilterState::Neither => true,
    //             FilterState::Both => {
    //                 let src = self.node_entry(edge.src());
    //                 let dst = self.node_entry(edge.dst());
    //                 view.filter_edge(edge.as_ref(), view.layer_ids())
    //                     && view.filter_node(src.as_ref())
    //                     && view.filter_node(dst.as_ref())
    //             }
    //             FilterState::Nodes => {
    //                 let src = self.node_entry(edge.src());
    //                 let dst = self.node_entry(edge.dst());
    //                 view.filter_node(src.as_ref()) && view.filter_node(dst.as_ref())
    //             }
    //             FilterState::Edges | FilterState::BothIndependent => {
    //                 view.filter_edge(edge.as_ref(), view.layer_ids())
    //             }
    //         })
    //         .map(|e| e.out_ref())
    // }
    //
    // pub fn into_edges_par<'graph, G: GraphViewOps<'graph>>(
    //     self,
    //     view: G,
    // ) -> impl ParallelIterator<Item = EdgeRef> + 'graph {
    //     let edges = self.owned_edges();
    //     let nodes = self.owned_nodes();
    //
    //     match edges {
    //         EdgesStorage::Mem(edges) => {
    //             let iter = (0..edges.len()).into_par_iter().map(EID);
    //             let filtered = match view.filter_state() {
    //                 FilterState::Neither => FilterVariants::Neither(
    //                     iter.map(move |eid| edges.get_mem(eid).as_edge_ref()),
    //                 ),
    //                 FilterState::Both => FilterVariants::Both(iter.filter_map(move |e| {
    //                     let e = EdgeStorageRef::Mem(edges.get_mem(e));
    //                     (view.filter_edge(e, view.layer_ids())
    //                         && view.filter_node(nodes.node_entry(e.src()))
    //                         && view.filter_node(nodes.node_entry(e.dst())))
    //                     .then(|| e.out_ref())
    //                 })),
    //                 FilterState::Nodes => FilterVariants::Nodes(iter.filter_map(move |e| {
    //                     let e = EdgeStorageRef::Mem(edges.get_mem(e));
    //                     (view.filter_node(nodes.node_entry(e.src()))
    //                         && view.filter_node(nodes.node_entry(e.dst())))
    //                     .then(|| e.out_ref())
    //                 })),
    //                 FilterState::Edges | FilterState::BothIndependent => {
    //                     FilterVariants::Edges(iter.filter_map(move |e| {
    //                         let e = EdgeStorageRef::Mem(edges.get_mem(e));
    //                         view.filter_edge(e, view.layer_ids()).then(|| e.out_ref())
    //                     }))
    //                 }
    //             };
    //             #[cfg(feature = "storage")]
    //             {
    //                 StorageVariants::Mem(filtered)
    //             }
    //             #[cfg(not(feature = "storage"))]
    //             {
    //                 filtered
    //             }
    //         }
    //         #[cfg(feature = "storage")]
    //         EdgesStorage::Disk(edges) => {
    //             let edges_clone = edges.clone();
    //             let iter = edges_clone.into_par_iter_refs(view.layer_ids().clone());
    //             let filtered = match view.filter_state() {
    //                 FilterState::Neither => FilterVariants::Neither(
    //                     iter.map(move |eid| EdgeStorageRef::Disk(edges.get(eid)).out_ref()),
    //                 ),
    //                 FilterState::Both => FilterVariants::Both(iter.filter_map(move |eid| {
    //                     let e = EdgeStorageRef::Disk(edges.get(eid));
    //                     if !view.filter_edge(e, view.layer_ids()) {
    //                         return None;
    //                     }
    //                     let src = nodes.node_entry(e.src());
    //                     if !view.filter_node(src) {
    //                         return None;
    //                     }
    //                     let dst = nodes.node_entry(e.dst());
    //                     if !view.filter_node(dst) {
    //                         return None;
    //                     }
    //                     Some(e.out_ref())
    //                 })),
    //                 FilterState::Nodes => FilterVariants::Nodes(iter.filter_map(move |eid| {
    //                     let e = EdgeStorageRef::Disk(edges.get(eid));
    //                     let src = nodes.node_entry(e.src());
    //                     if !view.filter_node(src) {
    //                         return None;
    //                     }
    //                     let dst = nodes.node_entry(e.dst());
    //                     if !view.filter_node(dst) {
    //                         return None;
    //                     }
    //                     Some(e.out_ref())
    //                 })),
    //                 FilterState::Edges | FilterState::BothIndependent => {
    //                     FilterVariants::Edges(iter.filter_map(move |eid| {
    //                         let e = EdgeStorageRef::Disk(edges.get(eid));
    //                         if !view.filter_edge(e, view.layer_ids()) {
    //                             return None;
    //                         }
    //                         Some(e.out_ref())
    //                     }))
    //                 }
    //             };
    //             StorageVariants::Disk(filtered)
    //         }
    //     }
    // }
    //
    // pub fn node_neighbours_iter<'a, 'graph: 'a, G: GraphViewOps<'graph>>(
    //     &'a self,
    //     node: VID,
    //     dir: Direction,
    //     view: &'a G,
    // ) -> impl Iterator<Item = VID> + Send + 'a {
    //     self.node_edges_iter(node, dir, view)
    //         .map(|e| e.remote())
    //         .dedup()
    // }
    //
    // pub fn into_node_neighbours_iter<'graph, G: GraphViewOps<'graph>>(
    //     self,
    //     node: VID,
    //     dir: Direction,
    //     view: G,
    // ) -> impl Iterator<Item = VID> + 'graph {
    //     self.into_node_edges_iter(node, dir, view)
    //         .map(|e| e.remote())
    //         .dedup()
    // }
    //
    // #[inline]
    // pub fn node_degree<'graph, G: GraphViewOps<'graph>>(
    //     &self,
    //     node: VID,
    //     dir: Direction,
    //     view: &G,
    // ) -> usize {
    //     if matches!(view.filter_state(), FilterState::Neither) {
    //         self.node_entry(node).degree(view.layer_ids(), dir)
    //     } else {
    //         self.node_neighbours_iter(node, dir, view).count()
    //     }
    // }
    //
    // pub fn node_edges_iter<'a, 'graph: 'a, G: GraphViewOps<'graph>>(
    //     &'a self,
    //     node: VID,
    //     dir: Direction,
    //     view: &'a G,
    // ) -> impl Iterator<Item = EdgeRef> + 'a {
    //     let source = self.node_entry(node);
    //     let layers = view.layer_ids();
    //     let iter = source.into_edges_iter(layers, dir);
    //     match view.filter_state() {
    //         FilterState::Neither => FilterVariants::Neither(iter),
    //         FilterState::Both => FilterVariants::Both(iter.filter(|&e| {
    //             view.filter_edge(self.edge_entry(e.pid()).as_ref(), view.layer_ids())
    //                 && view.filter_node(self.node_entry(e.remote()).as_ref())
    //         })),
    //         FilterState::Nodes => FilterVariants::Nodes(
    //             iter.filter(|e| view.filter_node(self.node_entry(e.remote()).as_ref())),
    //         ),
    //         FilterState::Edges | FilterState::BothIndependent => {
    //             FilterVariants::Edges(iter.filter(|&e| {
    //                 view.filter_edge(self.edge_entry(e.pid()).as_ref(), view.layer_ids())
    //             }))
    //         }
    //     }
    // }
    //
    // pub fn into_node_edges_iter<'graph, G: GraphViewOps<'graph>>(
    //     self,
    //     node: VID,
    //     dir: Direction,
    //     view: G,
    // ) -> impl Iterator<Item = EdgeRef> + 'graph {
    //     let layers = view.layer_ids().clone();
    //     let local = self.owned_node(node);
    //     let iter = local.into_edges_iter(layers, dir);
    //
    //     match view.filter_state() {
    //         FilterState::Neither => FilterVariants::Neither(iter),
    //         FilterState::Both => FilterVariants::Both(iter.filter(move |&e| {
    //             view.filter_edge(self.edge_entry(e.pid()).as_ref(), view.layer_ids())
    //                 && view.filter_node(self.node_entry(e.remote()).as_ref())
    //         })),
    //         FilterState::Nodes => FilterVariants::Nodes(
    //             iter.filter(move |e| view.filter_node(self.node_entry(e.remote()).as_ref())),
    //         ),
    //         FilterState::Edges | FilterState::BothIndependent => {
    //             FilterVariants::Edges(iter.filter(move |&e| {
    //                 view.filter_edge(self.edge_entry(e.pid()).as_ref(), view.layer_ids())
    //             }))
    //         }
    //     }
    // }

    pub fn node_meta(&self) -> &Meta {
        match self {
            GraphStorage::Mem(storage) => storage.graph().node_meta(),
            GraphStorage::Unlocked(storage) => storage.node_meta(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.node_meta(),
        }
    }

    pub fn edge_meta(&self) -> &Meta {
        match self {
            GraphStorage::Mem(storage) => storage.graph().edge_meta(),
            GraphStorage::Unlocked(storage) => storage.edge_meta(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.edge_meta(),
        }
    }

    pub fn graph_meta(&self) -> &GraphMeta {
        match self {
            GraphStorage::Mem(storage) => storage.graph().graph_meta(),
            GraphStorage::Unlocked(storage) => storage.graph_meta(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.graph_meta(),
        }
    }
}
