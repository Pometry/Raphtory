use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            nodes::{node_ref::NodeRef, node_store::NodeTimestamps},
            properties::{graph_meta::GraphMeta, props::Meta, tcell::TCell, tprop::TProp},
            LayerIds, VID,
        },
        storage::{
            locked_view::LockedView,
            timeindex::{TimeIndexOps, TimeIndexWindow},
        },
        Prop,
    },
    db::api::{
        storage::graph::{
            edges::{edge_entry::EdgeStorageEntry, edges::EdgesStorage},
            nodes::{
                node_entry::NodeStorageEntry, node_storage_ops::NodeStorageOps, nodes::NodesStorage,
            },
            storage_ops::GraphStorage,
        },
        view::{internal::Base, BoxedIter, BoxedLIter},
    },
};
use enum_dispatch::enum_dispatch;
use itertools::Itertools;
use raphtory_api::core::{
    entities::{EID, GID},
    storage::{
        arc_str::ArcStr,
        timeindex::{TimeIndexEntry, TimeIndexIntoOps, TimeIndexLike},
    },
};
use std::{iter, ops::Range};

#[cfg(feature = "storage")]
use pometry_storage::timestamps::TimeStamps;
#[cfg(feature = "storage")]
use rayon::prelude::*;

/// Core functions that should (almost-)always be implemented by pointing at the underlying graph.
#[enum_dispatch]
pub trait CoreGraphOps: Send + Sync {
    /// get the number of nodes in the main graph
    #[inline]
    fn unfiltered_num_nodes(&self) -> usize {
        self.core_graph().internal_num_nodes()
    }

    /// get the number of edges in the main graph
    #[inline]
    fn unfiltered_num_edges(&self) -> usize {
        self.core_graph().internal_num_edges()
    }

    /// get the number of layers in the main graph
    #[inline]
    fn unfiltered_num_layers(&self) -> usize {
        self.core_graph().internal_num_layers()
    }

    fn core_graph(&self) -> &GraphStorage;

    #[inline]
    fn core_edges(&self) -> EdgesStorage {
        self.core_graph().owned_edges()
    }
    #[inline]
    fn core_edge(&self, eid: EID) -> EdgeStorageEntry {
        self.core_graph().edge_entry(eid)
    }

    #[inline]
    fn core_nodes(&self) -> NodesStorage {
        self.core_graph().owned_nodes()
    }

    #[inline]
    fn core_node_entry(&self, vid: VID) -> NodeStorageEntry {
        self.core_graph().node_entry(vid)
    }

    #[inline]
    fn node_meta(&self) -> &Meta {
        self.core_graph().node_meta()
    }

    #[inline]
    fn edge_meta(&self) -> &Meta {
        self.core_graph().edge_meta()
    }

    #[inline]
    fn graph_meta(&self) -> &GraphMeta {
        self.core_graph().graph_meta()
    }

    #[inline]
    fn get_layer_name(&self, layer_id: usize) -> ArcStr {
        self.edge_meta().layer_meta().get_name(layer_id).clone()
    }

    #[inline]
    fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.edge_meta().get_layer_id(name)
    }

    /// Get the layer name for a given id
    #[inline]
    fn get_layer_names_from_ids(&self, layer_ids: &LayerIds) -> BoxedIter<ArcStr> {
        let layer_ids = layer_ids.clone();
        match layer_ids {
            LayerIds::None => Box::new(iter::empty()),
            LayerIds::All => Box::new(self.edge_meta().layer_meta().get_keys().into_iter()),
            LayerIds::One(id) => {
                let name = self.edge_meta().layer_meta().get_name(id).clone();
                Box::new(iter::once(name))
            }
            LayerIds::Multiple(ids) => {
                let keys = self.edge_meta().layer_meta().get_keys();
                Box::new(ids.into_iter().map(move |id| keys[id].clone()))
            }
        }
    }

    /// Get all node types
    #[inline]
    fn get_all_node_types(&self) -> Vec<ArcStr> {
        self.node_meta().get_all_node_types()
    }

    /// Returns the external ID for a node
    #[inline]
    fn node_id(&self, v: VID) -> GID {
        self.core_graph().node_entry(v).id().into()
    }

    /// Returns the string name for a node
    #[inline]
    fn node_name(&self, v: VID) -> String {
        let node = self.core_node_entry(v);
        node.name()
            .map(|name| name.to_string())
            .unwrap_or_else(|| node.id().to_str().to_string())
    }

    /// Returns the type of node
    #[inline]
    fn node_type(&self, v: VID) -> Option<ArcStr> {
        let type_id = self.node_type_id(v);
        self.node_meta().get_node_type_name_by_id(type_id)
    }

    /// Returns the type id of a node
    #[inline]
    fn node_type_id(&self, v: VID) -> usize {
        let node = self.core_node_entry(v);
        node.node_type_id()
    }

    /// Gets the internal reference for an external node reference and keeps internal references unchanged.
    #[inline]
    fn internalise_node(&self, v: NodeRef) -> Option<VID> {
        self.core_graph().internalise_node(v)
    }

    /// Gets the internal reference for an external node reference and keeps internal references unchanged. Assumes node exists!
    #[inline]
    fn internalise_node_unchecked(&self, v: NodeRef) -> VID {
        self.core_graph().internalise_node(v).unwrap()
    }

    /// Gets a static graph property.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// Returns:
    ///
    /// Option<Prop> - The property value if it exists.
    fn constant_prop(&self, id: usize) -> Option<Prop> {
        self.graph_meta().get_constant(id)
    }

    /// Gets a temporal graph property.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// Returns:
    ///
    /// Option<LockedView<TProp>> - The history of property values if it exists.
    fn temporal_prop(&self, id: usize) -> Option<LockedView<TProp>> {
        self.graph_meta().get_temporal_prop(id)
    }

    /// Gets a static property of a given node given the name and node reference.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the node for which the property is being queried.
    /// * `name` - The name of the property.
    ///
    /// Returns:
    ///
    /// Option<Prop> - The property value if it exists.
    fn constant_node_prop(&self, v: VID, id: usize) -> Option<Prop> {
        let core_node_entry = self.core_node_entry(v);
        core_node_entry.prop(id)
    }

    /// Gets the keys of constant properties of a given node
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the node for which the property is being queried.
    ///
    /// Returns:
    ///
    /// The keys of the constant properties.
    fn constant_node_prop_ids(&self, v: VID) -> BoxedLIter<usize> {
        let core_node_entry = self.core_node_entry(v);
        core_node_entry.prop_ids()
    }

    /// Returns a vector of all ids of temporal properties within the given node
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the node for which to retrieve the names.
    ///
    /// Returns:
    ///
    /// the ids of the temporal properties
    fn temporal_node_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        let core_node_entry = self.core_node_entry(v);
        core_node_entry.temporal_prop_ids()
    }

    /// Returns the static edge property with the given name for the
    /// given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    ///
    /// Returns:
    ///
    /// A property if it exists
    fn get_const_edge_prop(&self, e: EdgeRef, id: usize, layer_ids: LayerIds) -> Option<Prop> {
        match self.core_graph() {
            GraphStorage::Mem(storage) => storage.graph.core_get_const_edge_prop(e, id, layer_ids),
            GraphStorage::Unlocked(storage) => storage.core_get_const_edge_prop(e, id, layer_ids),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(_) => None,
        }
    }

    /// Returns a vector of keys for the static properties of the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    ///
    /// Returns:
    ///
    /// the keys for the constant properties of the given edge.
    fn const_edge_prop_ids(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedLIter<usize> {
        match self.core_graph() {
            GraphStorage::Mem(storage) => storage.graph.core_const_edge_prop_ids(e, layer_ids),
            GraphStorage::Unlocked(storage) => storage.core_const_edge_prop_ids(e, layer_ids),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(_) => Box::new(std::iter::empty()),
        }
    }

    /// Returns a vector of keys for the temporal properties of the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    ///
    /// Returns:
    ///
    /// * keys for the temporal properties of the given edge.
    fn temporal_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        // FIXME once the disk storage can handle multiple layers this can be implemented generically over the EdgeStorageEntry
        match self.core_graph() {
            GraphStorage::Mem(storage) => storage.graph.core_temporal_edge_prop_ids(e, layer_ids),
            GraphStorage::Unlocked(storage) => storage.core_temporal_edge_prop_ids(e, layer_ids),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.core_temporal_edge_prop_ids(e, layer_ids),
        }
    }
}

pub trait InheritCoreOps: Base {}

impl<G: InheritCoreOps> DelegateCoreOps for G
where
    G::Base: CoreGraphOps,
{
    type Internal = G::Base;

    #[inline]
    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateCoreOps {
    type Internal: CoreGraphOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateCoreOps + ?Sized + Send + Sync> CoreGraphOps for G {
    #[inline]
    fn core_graph(&self) -> &GraphStorage {
        self.graph().core_graph()
    }
}

impl TimeIndexOps for NodeTimestamps {
    type IndexType = TimeIndexEntry;

    type RangeType<'b>
        = TimeIndexWindow<'b, TimeIndexEntry, Self>
    where
        Self: 'b;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.edge_ts.active(w.clone()) || self.props_ts.active(w)
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType<'_> {
        TimeIndexWindow::TimeIndexRange {
            timeindex: self,
            range: w,
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        let first = self.edge_ts.first();
        let other = self.props_ts.first();

        first
            .zip(other)
            .map(|(a, b)| a.min(b))
            .or_else(|| first.or(other))
    }

    fn last(&self) -> Option<Self::IndexType> {
        let last = self.edge_ts.last();
        let other = self.props_ts.last();

        last.zip(other)
            .map(|(a, b)| a.max(b))
            .or_else(|| last.or(other))
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Self::IndexType> + Send + '_> {
        Box::new(
            self.edge_ts
                .iter()
                .map(|(t, _)| *t)
                .merge(self.props_ts.iter().map(|(t, _)| *t)),
        )
    }

    fn len(&self) -> usize {
        self.edge_ts.len() + self.props_ts.len()
    }
}

impl TimeIndexLike for NodeTimestamps {
    fn range_iter(
        &self,
        w: Range<Self::IndexType>,
    ) -> Box<dyn Iterator<Item = Self::IndexType> + Send + '_> {
        Box::new(
            self.edge_ts
                .range_iter(w.clone())
                .merge(self.props_ts.range_iter(w)),
        )
    }

    fn first_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        let first = self.edge_ts.iter_window(w.clone()).next().map(|(t, _)| *t);
        let other = self.props_ts.iter_window(w).next().map(|(t, _)| *t);

        first
            .zip(other)
            .map(|(a, b)| a.min(b))
            .or_else(|| first.or(other))
    }

    fn last_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        let last = self
            .edge_ts
            .iter_window(w.clone())
            .next_back()
            .map(|(t, _)| *t);
        let other = self.props_ts.iter_window(w).next_back().map(|(t, _)| *t);

        last.zip(other)
            .map(|(a, b)| a.max(b))
            .or_else(|| last.or(other))
    }
}

fn chain_my_iters<'a, A: 'a, I: DoubleEndedIterator<Item = A> + Send + 'a>(
    is: impl Iterator<Item = I>,
) -> Box<dyn DoubleEndedIterator<Item = A> + Send + 'a> {
    is.map(|i| {
        let i: Box<dyn DoubleEndedIterator<Item = A> + Send + 'a> = Box::new(i);
        i
    })
    .reduce(|a, b| Box::new(a.chain(b)) as Box<dyn DoubleEndedIterator<Item = A> + Send + 'a>)
    .unwrap_or_else(|| Box::new(iter::empty()))
}

#[derive(Clone)]
pub enum NodeEvents<'a, A> {
    Mem(&'a TCell<A>),
    Range(TimeIndexWindow<'a, TimeIndexEntry, TCell<A>>),
    #[cfg(feature = "storage")]
    Disk(TimeStamps<'a, TimeIndexEntry>),
}

impl<'a, A: Send + Sync> TimeIndexOps for NodeEvents<'a, A> {
    type IndexType = TimeIndexEntry;
    type RangeType<'b>
        = NodeEvents<'b, A>
    where
        Self: 'b;

    #[inline]
    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        match self {
            NodeEvents::Mem(index) => index.active(w),
            NodeEvents::Range(index) => index.active(w),
            #[cfg(feature = "storage")]
            NodeEvents::Disk(index) => index.active(w),
        }
    }

    fn range(&self, w: Range<TimeIndexEntry>) -> Self::RangeType<'_> {
        match self {
            NodeEvents::Mem(index) => NodeEvents::Range(index.range(w)),
            NodeEvents::Range(index) => NodeEvents::Range(index.range(w)),
            #[cfg(feature = "storage")]
            NodeEvents::Disk(index) => NodeEvents::Disk(index.range(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            NodeEvents::Mem(index) => index.first(),
            NodeEvents::Range(index) => index.first(),
            #[cfg(feature = "storage")]
            NodeEvents::Disk(index) => index.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            NodeEvents::Mem(index) => index.last(),
            NodeEvents::Range(index) => index.last(),
            #[cfg(feature = "storage")]
            NodeEvents::Disk(index) => index.last(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = TimeIndexEntry> + Send + '_> {
        match self {
            NodeEvents::Mem(index) => <TCell<A> as TimeIndexOps>::iter(index),
            NodeEvents::Range(index) => index.iter(),
            #[cfg(feature = "storage")]
            NodeEvents::Disk(index) => Box::new(index.iter()),
        }
    }

    fn len(&self) -> usize {
        match self {
            NodeEvents::Mem(index) => index.len(),
            NodeEvents::Range(index) => index.len(),
            #[cfg(feature = "storage")]
            NodeEvents::Disk(index) => index.len(),
        }
    }
}

impl<'a, A: Send + Sync> TimeIndexIntoOps for NodeEvents<'a, A> {
    type IndexType = TimeIndexEntry;

    type RangeType = Self;

    fn into_range(self, w: Range<Self::IndexType>) -> Self::RangeType {
        match self {
            NodeEvents::Mem(index) => NodeEvents::Range(index.range(w)),
            NodeEvents::Range(index) => NodeEvents::Range(index.into_range(w)),
            #[cfg(feature = "storage")]
            NodeEvents::Disk(index) => NodeEvents::Disk(index.into_range(w)),
        }
    }

    fn into_iter(self) -> impl Iterator<Item = Self::IndexType> + Send {
        match self {
            NodeEvents::Mem(index) => <TCell<A> as TimeIndexOps>::iter(index),
            NodeEvents::Range(index) => Box::new(index.into_iter()),
            #[cfg(feature = "storage")]
            NodeEvents::Disk(index) => Box::new(index.into_iter()),
        }
    }
}

impl<'a, A: Copy + Send + Sync> NodeEvents<'a, A> {
    pub(crate) fn iter_values(self) -> Box<dyn Iterator<Item = (TimeIndexEntry, A)> + Send + 'a> {
        match self {
            NodeEvents::Mem(index) => Box::new(index.iter().map(|(time, value)| (*time, *value))),
            NodeEvents::Range(index) => Box::new(index.iter_values()),
            // #[cfg(feature = "storage")]
            // NodeEvents::Disk(index) => index.iter().map(|time| (time, index.get(time))),
        }
    }
}

pub enum NodeAdditions<'a> {
    Mem(&'a NodeTimestamps),
    Range(TimeIndexWindow<'a, TimeIndexEntry, NodeTimestamps>),
    #[cfg(feature = "storage")]
    Col(Vec<TimeStamps<'a, TimeIndexEntry>>),
}

impl<'a> NodeAdditions<'a> {
    pub fn into_prop_events(self) -> NodeEvents<'a, Option<usize>> {
        match self {
            NodeAdditions::Mem(index) => NodeEvents::Mem(&index.props_ts),
            NodeAdditions::Range(index) => match index {
                TimeIndexWindow::Empty => NodeEvents::Range(TimeIndexWindow::Empty),
                TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                    NodeEvents::Range(TimeIndexWindow::TimeIndexRange {
                        timeindex: &timeindex.props_ts,
                        range,
                    })
                }
                TimeIndexWindow::All(index) => NodeEvents::Mem(&index.props_ts),
            },
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => NodeEvents::Disk(index),
        }
    }

    pub fn into_edge_events(self) -> NodeEvents<'a, EID> {
        match self {
            NodeAdditions::Mem(index) => NodeEvents::Mem(&index.edge_ts),
            NodeAdditions::Range(index) => match index {
                TimeIndexWindow::Empty => NodeEvents::Range(TimeIndexWindow::Empty),
                TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                    NodeEvents::Range(TimeIndexWindow::TimeIndexRange {
                        timeindex: &timeindex.edge_ts,
                        range,
                    })
                }
                TimeIndexWindow::All(index) => NodeEvents::Mem(&index.edge_ts),
            },
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => NodeEvents::Disk(index),
        }
    }
}

impl<'b> TimeIndexOps for NodeAdditions<'b> {
    type IndexType = TimeIndexEntry;
    type RangeType<'a>
        = NodeAdditions<'a>
    where
        Self: 'a;

    #[inline]
    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        match self {
            NodeAdditions::Mem(index) => index.active(w),
            NodeAdditions::Range(index) => index.active(w),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.par_iter().any(|index| index.active(w.clone())),
        }
    }

    fn range(&self, w: Range<TimeIndexEntry>) -> Self::RangeType<'_> {
        match self {
            NodeAdditions::Mem(index) => NodeAdditions::Range(index.range(w)),
            NodeAdditions::Range(index) => NodeAdditions::Range(index.range(w)),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => {
                let mut ranges = Vec::with_capacity(index.len());
                index
                    .par_iter()
                    .map(|index| index.range(w.clone()))
                    .collect_into_vec(&mut ranges);
                NodeAdditions::Col(ranges)
            }
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            NodeAdditions::Mem(index) => index.first(),
            NodeAdditions::Range(index) => index.first(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.par_iter().flat_map(|index| index.first()).min(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            NodeAdditions::Mem(index) => index.last(),
            NodeAdditions::Range(index) => index.last(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.par_iter().flat_map(|index| index.last()).max(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = TimeIndexEntry> + Send + '_> {
        match self {
            NodeAdditions::Mem(index) => <NodeTimestamps as TimeIndexOps>::iter(index),
            NodeAdditions::Range(index) => index.iter(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => Box::new(index.iter().flat_map(|index| index.iter())),
        }
    }

    fn len(&self) -> usize {
        match self {
            NodeAdditions::Mem(index) => index.len(),
            NodeAdditions::Range(range) => range.len(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(col) => col.len(),
        }
    }
}

impl<'a> TimeIndexIntoOps for NodeAdditions<'a> {
    type IndexType = TimeIndexEntry;

    type RangeType = Self;

    fn into_range(self, w: Range<Self::IndexType>) -> Self::RangeType {
        match self {
            NodeAdditions::Mem(index) => NodeAdditions::Range(index.range(w)),
            NodeAdditions::Range(index) => NodeAdditions::Range(index.into_range(w)),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => {
                let mut ranges = Vec::with_capacity(index.len());
                index
                    .par_iter()
                    .map(|index| index.into_range(w.clone()))
                    .collect_into_vec(&mut ranges);
                NodeAdditions::Col(ranges)
            }
        }
    }

    fn into_iter(self) -> impl Iterator<Item = Self::IndexType> + Send {
        match self {
            NodeAdditions::Mem(index) => <NodeTimestamps as TimeIndexOps>::iter(index),
            NodeAdditions::Range(index) => Box::new(index.into_iter()),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => {
                Box::new(index.into_iter().flat_map(|index| index.into_iter()))
            }
        }
    }
}
