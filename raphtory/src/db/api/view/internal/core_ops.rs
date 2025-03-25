use crate::{
    core::{
        entities::{
            nodes::{node_ref::NodeRef, node_store::NodeTimestamps},
            properties::{graph_meta::GraphMeta, props::Meta, tprop::TProp},
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
    prelude::GraphViewOps,
};
use enum_dispatch::enum_dispatch;
use itertools::Itertools;
use raphtory_api::{
    core::{
        entities::{EID, ELID, GID},
        storage::{
            arc_str::ArcStr,
            timeindex::{TimeIndexEntry, TimeIndexIntoOps, TimeIndexLike},
        },
    },
    iter::IntoDynBoxed,
};
use std::{iter, ops::Range};

#[cfg(feature = "storage")]
use pometry_storage::timestamps::LayerAdditions;

/// Check if two Graph views point at the same underlying storage
pub fn is_view_compatible(g1: &impl CoreGraphOps, g2: &impl CoreGraphOps) -> bool {
    g1.core_graph().ptr_eq(&g2.core_graph())
}

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

impl<'a> TimeIndexOps<'a> for &'a NodeTimestamps {
    type IndexType = TimeIndexEntry;
    type RangeType = TimeIndexWindow<'a, TimeIndexEntry, NodeTimestamps>;

    #[inline]
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.edge_ts().active(w.clone()) || self.props_ts().active(w)
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        TimeIndexWindow::TimeIndexRange {
            timeindex: *self,
            range: w,
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        let first = self.edge_ts().first();
        let other = self.props_ts().first();

        first
            .zip(other)
            .map(|(a, b)| a.min(b))
            .or_else(|| first.or(other))
    }

    fn last(&self) -> Option<Self::IndexType> {
        let last = self.edge_ts().last();
        let other = self.props_ts().last();

        last.zip(other)
            .map(|(a, b)| a.max(b))
            .or_else(|| last.or(other))
    }

    fn iter(&self) -> BoxedLIter<'a, Self::IndexType> {
        self.edge_ts
            .iter()
            .map(|(t, _)| *t)
            .merge(self.props_ts.iter().map(|(t, _)| *t))
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'a, Self::IndexType> {
        self.edge_ts
            .iter()
            .rev()
            .map(|(t, _)| *t)
            .merge_by(self.props_ts.iter().rev().map(|(t, _)| *t), |lt, rt| {
                lt >= rt
            })
            .into_dyn_boxed()
    }

    fn len(&self) -> usize {
        self.edge_ts.len() + self.props_ts.len()
    }
}

impl<'a> TimeIndexLike<'a> for &'a NodeTimestamps {
    fn range_iter(&self, w: Range<Self::IndexType>) -> BoxedLIter<'a, Self::IndexType> {
        Box::new(
            self.edge_ts()
                .range_iter(w.clone())
                .merge(self.props_ts().range_iter(w)),
        )
    }

    fn range_iter_rev(&self, w: Range<Self::IndexType>) -> BoxedLIter<'a, Self::IndexType> {
        self.edge_ts()
            .range_iter_rev(w.clone())
            .merge_by(self.props_ts().range_iter_rev(w), |lt, rt| lt >= rt)
            .into_dyn_boxed()
    }

    fn first_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        let first = self
            .edge_ts()
            .iter_window(w.clone())
            .next()
            .map(|(t, _)| *t);
        let other = self.props_ts().iter_window(w).next().map(|(t, _)| *t);

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

pub enum NodeAdditions<'a> {
    Mem(&'a NodeTimestamps),
    Range(TimeIndexWindow<'a, TimeIndexEntry, NodeTimestamps>),
    #[cfg(feature = "storage")]
    Col(LayerAdditions<'a>),
}

impl<'a> NodeAdditions<'a> {
    pub fn prop_events(&self) -> impl Iterator<Item = TimeIndexEntry> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => index.props_ts.iter().map(|(t, _)| *t).into_dyn_boxed(),
            NodeAdditions::Range(index) => match index {
                TimeIndexWindow::Empty => iter::empty().into_dyn_boxed(),
                TimeIndexWindow::TimeIndexRange { timeindex, range } => timeindex
                    .props_ts
                    .iter_window(range.clone())
                    .map(|(t, _)| *t)
                    .into_dyn_boxed(),
                TimeIndexWindow::All(index) => {
                    index.props_ts.iter().map(|(t, _)| *t).into_dyn_boxed()
                }
            },
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index
                .clone()
                .prop_events()
                .map(|t| t.into_iter())
                .kmerge()
                .into_dyn_boxed(),
        }
    }

    pub fn prop_events_rev(&self) -> impl Iterator<Item = TimeIndexEntry> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => index
                .props_ts
                .iter()
                .map(|(t, _)| *t)
                .rev()
                .into_dyn_boxed(),
            NodeAdditions::Range(index) => match index {
                TimeIndexWindow::Empty => iter::empty().into_dyn_boxed(),
                TimeIndexWindow::TimeIndexRange { timeindex, range } => timeindex
                    .props_ts
                    .iter_window(range.clone())
                    .map(|(t, _)| *t)
                    .rev()
                    .into_dyn_boxed(),
                TimeIndexWindow::All(index) => index
                    .props_ts
                    .iter()
                    .map(|(t, _)| *t)
                    .rev()
                    .into_dyn_boxed(),
            },
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index
                .clone()
                .prop_events()
                .map(|t| t.into_iter().rev())
                .kmerge_by(|t1, t2| t1 >= t2)
                .into_dyn_boxed(),
        }
    }

    pub fn edge_events(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                index.edge_ts.iter().map(|(t, e)| (*t, *e)).into_dyn_boxed()
            }
            NodeAdditions::Range(index) => match index {
                TimeIndexWindow::Empty => iter::empty().into_dyn_boxed(),
                TimeIndexWindow::TimeIndexRange { timeindex, range } => timeindex
                    .edge_ts
                    .iter_window(range.clone())
                    .map(|(t, e)| (*t, *e))
                    .into_dyn_boxed(),
                TimeIndexWindow::All(index) => {
                    index.edge_ts.iter().map(|(t, e)| (*t, *e)).into_dyn_boxed()
                }
            },
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.edge_history().into_dyn_boxed(),
        }
    }

    pub fn edge_events_rev(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => index
                .edge_ts
                .iter()
                .map(|(t, e)| (*t, *e))
                .rev()
                .into_dyn_boxed(),
            NodeAdditions::Range(index) => match index {
                TimeIndexWindow::Empty => iter::empty().into_dyn_boxed(),
                TimeIndexWindow::TimeIndexRange { timeindex, range } => timeindex
                    .edge_ts
                    .iter_window(range.clone())
                    .map(|(t, e)| (*t, *e))
                    .rev()
                    .into_dyn_boxed(),
                TimeIndexWindow::All(index) => index
                    .edge_ts
                    .iter()
                    .map(|(t, e)| (*t, *e))
                    .rev()
                    .into_dyn_boxed(),
            },
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.edge_history_rev().into_dyn_boxed(),
        }
    }

    pub fn with_range(&self, w: Range<TimeIndexEntry>) -> Self {
        match self {
            NodeAdditions::Mem(index) => NodeAdditions::Range(TimeIndexWindow::TimeIndexRange {
                timeindex: index,
                range: w,
            }),
            NodeAdditions::Range(index) => NodeAdditions::Range(index.with_range(w)),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => NodeAdditions::Col(index.with_range(w)),
        }
    }
}

impl<'b> TimeIndexOps<'b> for NodeAdditions<'b> {
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    #[inline]
    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        match self {
            NodeAdditions::Mem(index) => index.active(w),
            NodeAdditions::Range(index) => index.active(w),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.iter().any(|index| index.active(w.clone())),
        }
    }

    fn range(&self, w: Range<TimeIndexEntry>) -> Self {
        match self {
            NodeAdditions::Mem(index) => NodeAdditions::Range(index.range(w)),
            NodeAdditions::Range(index) => NodeAdditions::Range(index.range(w)),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => NodeAdditions::Col(index.with_range(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            NodeAdditions::Mem(index) => index.first(),
            NodeAdditions::Range(index) => index.first(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.iter().flat_map(|index| index.first()).min(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            NodeAdditions::Mem(index) => index.last(),
            NodeAdditions::Range(index) => index.last(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.iter().flat_map(|index| index.last()).max(),
        }
    }

    fn iter(&self) -> BoxedLIter<'b, TimeIndexEntry> {
        match self {
            NodeAdditions::Mem(index) => index.iter(),
            NodeAdditions::Range(index) => index.iter(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index
                .iter()
                .map(|index| index.into_iter())
                .kmerge()
                .into_dyn_boxed(),
        }
    }

    fn iter_rev(&self) -> BoxedLIter<'b, TimeIndexEntry> {
        match self {
            NodeAdditions::Mem(index) => index.iter_rev(),
            NodeAdditions::Range(index) => index.iter_rev(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index
                .iter()
                .map(|index| index.into_iter().rev())
                .kmerge_by(|lt, rt| lt >= rt)
                .into_dyn_boxed(),
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
            NodeAdditions::Mem(index) => NodeAdditions::Range(TimeIndexWindow::TimeIndexRange {
                timeindex: index,
                range: w,
            }),
            NodeAdditions::Range(index) => NodeAdditions::Range(index.with_range(w)),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => NodeAdditions::Col(index.with_range(w)),
        }
    }

    fn into_iter(self) -> impl Iterator<Item = Self::IndexType> + Send {
        match self {
            NodeAdditions::Mem(index) => index.iter().into_dyn_boxed(),
            NodeAdditions::Range(index) => index.iter().into_dyn_boxed(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index
                .into_iter()
                .flat_map(|index| index.into_iter())
                .into_dyn_boxed(),
        }
    }
}

pub struct NodeHistory<'a, G> {
    pub(crate) additions: NodeAdditions<'a>,
    pub(crate) view: G,
}

impl<'b, G: GraphViewOps<'b>> TimeIndexOps<'b> for NodeHistory<'b, G> {
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.additions
            .with_range(w.clone())
            .prop_events()
            .next()
            .is_some()
            || self
                .additions
                .with_range(w)
                .edge_events()
                .filter(|(t, e)| self.view.filter_edge_history(*e, *t, self.view.layer_ids()))
                .next()
                .is_some()
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self {
        let additions = self.additions.with_range(w);
        let view = self.view.clone();
        NodeHistory { additions, view }
    }

    fn iter(&self) -> BoxedLIter<'b, TimeIndexEntry> {
        let view = self.view.clone();
        self.additions
            .prop_events()
            .merge(
                self.additions
                    .edge_events()
                    .filter(move |(t, e)| view.filter_edge_history(*e, *t, view.layer_ids()))
                    .map(|(t, _)| t),
            )
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'b, TimeIndexEntry> {
        let view = self.view.clone();
        self.additions
            .prop_events_rev()
            .merge_by(
                self.additions
                    .edge_events_rev()
                    .filter(move |(t, e)| view.filter_edge_history(*e, *t, view.layer_ids()))
                    .map(|(t, _)| t),
                |t1, t2| t1 >= t2,
            )
            .into_dyn_boxed()
    }

    fn len(&self) -> usize {
        self.iter().count()
    }
}
