use crate::{
    core::{
        entities::{
            nodes::{node_ref::NodeRef, node_store::NodeTimestamps},
            properties::{graph_meta::GraphMeta, props::Meta, tprop::TProp},
            LayerIds, VID,
        },
        storage::{
            locked_view::LockedView,
            timeindex::{TimeIndexOps, TimeIndexWindow, TimeIndexWindowVariants},
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
use iter_enum::{DoubleEndedIterator, ExactSizeIterator, FusedIterator, Iterator};
use itertools::Itertools;
#[cfg(feature = "storage")]
use pometry_storage::timestamps::LayerAdditions;
use raphtory_api::{
    core::{
        entities::{EID, ELID, GID},
        storage::{
            arc_str::ArcStr,
            timeindex::{TimeIndexEntry, TimeIndexLike},
        },
    },
    iter::IntoDynBoxed,
};
use std::{iter, ops::Range};

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

    #[inline]
    fn get_default_layer_id(&self) -> Option<usize> {
        self.edge_meta().get_default_layer_id()
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
    /// # Returns
    /// The property value if it exists.
    fn constant_prop(&self, id: usize) -> Option<Prop> {
        self.graph_meta().get_constant(id)
    }

    /// Gets a temporal graph property.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// # Returns
    /// The history of property values if it exists.
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
    /// # Returns
    /// The property value if it exists.
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
    /// # Returns
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
    /// # Returns
    /// The ids of the temporal properties
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
        TimeIndexWindow::Range {
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

    fn range_count(&self, w: Range<Self::IndexType>) -> usize {
        self.edge_ts().range_count(w.clone()) + self.props_ts().range_count(w)
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

#[derive(Clone, Debug)]
pub enum NodeAdditions<'a> {
    Mem(&'a NodeTimestamps),
    Range(TimeIndexWindow<'a, TimeIndexEntry, NodeTimestamps>),
    #[cfg(feature = "storage")]
    Col(LayerAdditions<'a>),
}

#[cfg(feature = "storage")]
#[derive(Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator, Debug)]
pub enum AdditionVariants<Mem, Range, Col> {
    Mem(Mem),
    Range(Range),
    Col(Col),
}

#[cfg(not(feature = "storage"))]
#[derive(Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator, Debug)]
pub enum AdditionVariants<Mem, Range> {
    Mem(Mem),
    Range(Range),
}

impl<'a> NodeAdditions<'a> {
    #[inline]
    pub fn prop_events(&self) -> impl Iterator<Item = TimeIndexEntry> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                AdditionVariants::Mem(index.props_ts.iter().map(|(t, _)| *t))
            }
            NodeAdditions::Range(index) => AdditionVariants::Range(match index {
                TimeIndexWindow::Empty => TimeIndexWindowVariants::Empty(iter::empty()),
                TimeIndexWindow::Range { timeindex, range } => TimeIndexWindowVariants::Range(
                    timeindex
                        .props_ts
                        .iter_window(range.clone())
                        .map(|(t, _)| *t),
                ),
                TimeIndexWindow::All(index) => TimeIndexWindowVariants::All(
                    index.props_ts.iter().map(|(t, _)| *t).into_dyn_boxed(),
                ),
            }),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => {
                AdditionVariants::Col(index.clone().prop_events().map(|t| t.into_iter()).kmerge())
            }
        }
    }

    #[inline]
    pub fn prop_events_rev(&self) -> impl Iterator<Item = TimeIndexEntry> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                AdditionVariants::Mem(index.props_ts.iter().map(|(t, _)| *t).rev())
            }
            NodeAdditions::Range(index) => AdditionVariants::Range(match index {
                TimeIndexWindow::Empty => TimeIndexWindowVariants::Empty(iter::empty()),
                TimeIndexWindow::Range { timeindex, range } => TimeIndexWindowVariants::Range(
                    timeindex
                        .props_ts
                        .iter_window(range.clone())
                        .map(|(t, _)| *t)
                        .rev(),
                ),
                TimeIndexWindow::All(index) => {
                    TimeIndexWindowVariants::All(index.props_ts.iter().map(|(t, _)| *t).rev())
                }
            }),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => AdditionVariants::Col(
                index
                    .clone()
                    .prop_events()
                    .map(|t| t.into_iter().rev())
                    .kmerge_by(|t1, t2| t1 >= t2),
            ),
        }
    }

    #[inline]
    pub fn edge_events(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                AdditionVariants::Mem(index.edge_ts.iter().map(|(t, e)| (*t, *e)))
            }
            NodeAdditions::Range(index) => AdditionVariants::Range(match index {
                TimeIndexWindow::Empty => TimeIndexWindowVariants::Empty(iter::empty()),
                TimeIndexWindow::Range { timeindex, range } => TimeIndexWindowVariants::Range(
                    timeindex
                        .edge_ts
                        .iter_window(range.clone())
                        .map(|(t, e)| (*t, *e)),
                ),
                TimeIndexWindow::All(index) => {
                    TimeIndexWindowVariants::All(index.edge_ts.iter().map(|(t, e)| (*t, *e)))
                }
            }),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => AdditionVariants::Col(index.edge_history()),
        }
    }

    #[inline]
    pub fn edge_events_rev(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                AdditionVariants::Mem(index.edge_ts.iter().map(|(t, e)| (*t, *e)).rev())
            }
            NodeAdditions::Range(index) => AdditionVariants::Range(match index {
                TimeIndexWindow::Empty => TimeIndexWindowVariants::Empty(iter::empty()),
                TimeIndexWindow::Range { timeindex, range } => TimeIndexWindowVariants::Range(
                    timeindex
                        .edge_ts
                        .iter_window(range.clone())
                        .map(|(t, e)| (*t, *e))
                        .rev(),
                ),
                TimeIndexWindow::All(index) => {
                    TimeIndexWindowVariants::All(index.edge_ts.iter().map(|(t, e)| (*t, *e)).rev())
                }
            }),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => AdditionVariants::Col(index.edge_history_rev()),
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

pub struct NodeHistory<'a, G> {
    pub(crate) additions: NodeAdditions<'a>,
    pub(crate) view: G,
}

pub struct NodeEdgeHistory<'a, G> {
    pub(crate) additions: NodeAdditions<'a>,
    pub(crate) view: G,
}

pub struct NodePropHistory<'a, G> {
    pub(crate) additions: NodeAdditions<'a>,
    pub(crate) view: G,
}

impl<'a, G: Clone> NodeHistory<'a, G> {
    pub fn edge_history(&self) -> NodeEdgeHistory<'a, G> {
        NodeEdgeHistory {
            additions: self.additions.clone(),
            view: self.view.clone(),
        }
    }

    pub fn prop_history(&self) -> NodePropHistory<'a, G> {
        NodePropHistory {
            additions: self.additions.clone(),
            view: self.view.clone(),
        }
    }
}

impl<'a, G: GraphViewOps<'a>> NodeEdgeHistory<'a, G> {
    pub fn history(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + use<'a, G> {
        let view = self.view.clone();
        self.additions
            .edge_events()
            .filter(move |(t, e)| view.filter_edge_history(*e, *t, view.layer_ids()))
    }

    pub fn history_rev(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + use<'a, G> {
        let view = self.view.clone();
        self.additions
            .edge_events_rev()
            .filter(move |(t, e)| view.filter_edge_history(*e, *t, view.layer_ids()))
    }
}

impl<'a, G: GraphViewOps<'a>> TimeIndexOps<'a> for NodePropHistory<'a, G> {
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        let history = &self.additions;
        match history {
            NodeAdditions::Mem(h) => h.props_ts().active(w),
            NodeAdditions::Range(h) => match h {
                TimeIndexWindow::Empty => false,
                TimeIndexWindow::Range { timeindex, range } => {
                    let start = range.start.max(w.start);
                    let end = range.end.min(w.end).max(start);
                    timeindex.props_ts().active(start..end)
                }
                TimeIndexWindow::All(h) => h.props_ts().active(w),
            },
            #[cfg(feature = "storage")]
            NodeAdditions::Col(h) => h.with_range(w).prop_events().any(|t| !t.is_empty()),
        }
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        let additions = self.additions.range(w);
        NodePropHistory {
            additions,
            view: self.view.clone(),
        }
    }

    fn iter(&self) -> BoxedLIter<'a, Self::IndexType> {
        self.additions.prop_events().into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'a, Self::IndexType> {
        self.additions.prop_events_rev().into_dyn_boxed()
    }

    fn len(&self) -> usize {
        match &self.additions {
            NodeAdditions::Mem(additions) => additions.props_ts.len(),
            NodeAdditions::Range(additions) => match additions {
                TimeIndexWindow::Empty => 0,
                TimeIndexWindow::Range { timeindex, range } => {
                    (&timeindex.props_ts).range(range.clone()).len()
                }
                TimeIndexWindow::All(timeindex) => timeindex.props_ts.len(),
            },
            #[cfg(feature = "storage")]
            NodeAdditions::Col(additions) => additions.clone().prop_events().map(|t| t.len()).sum(),
        }
    }

    fn is_empty(&self) -> bool {
        match &self.additions {
            NodeAdditions::Mem(additions) => additions.props_ts.is_empty(),
            NodeAdditions::Range(additions) => match additions {
                TimeIndexWindow::Empty => true,
                TimeIndexWindow::Range { timeindex, range } => {
                    (&timeindex.props_ts).range(range.clone()).is_empty()
                }
                TimeIndexWindow::All(timeindex) => timeindex.props_ts.is_empty(),
            },
            #[cfg(feature = "storage")]
            NodeAdditions::Col(additions) => additions.clone().prop_events().all(|t| t.is_empty()),
        }
    }
}

impl<'a, G: GraphViewOps<'a>> TimeIndexOps<'a> for NodeEdgeHistory<'a, G> {
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        if self.view.edge_history_filtered() {
            self.additions
                .range(w)
                .edge_events()
                .filter(|(t, e)| self.view.filter_edge_history(*e, *t, self.view.layer_ids()))
                .next()
                .is_some()
        } else {
            match &self.additions {
                NodeAdditions::Mem(h) => h.edge_ts().active(w),
                NodeAdditions::Range(h) => match h {
                    TimeIndexWindow::Empty => false,
                    TimeIndexWindow::Range { timeindex, range } => {
                        let start = range.start.max(w.start);
                        let end = range.end.min(w.end).max(start);
                        timeindex.edge_ts().active(start..end)
                    }
                    TimeIndexWindow::All(h) => h.edge_ts().active(w),
                },
                #[cfg(feature = "storage")]
                NodeAdditions::Col(h) => h.with_range(w).edge_events().any(|t| !t.is_empty()),
            }
        }
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        let additions = self.additions.range(w);
        NodeEdgeHistory {
            additions,
            view: self.view.clone(),
        }
    }

    fn iter(&self) -> BoxedLIter<'a, Self::IndexType> {
        self.history().map(|(t, _)| t).into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'a, Self::IndexType> {
        self.history_rev().map(|(t, _)| t).into_dyn_boxed()
    }

    fn len(&self) -> usize {
        if !self.view.edges_filtered() {
            match &self.additions {
                NodeAdditions::Mem(additions) => additions.edge_ts.len(),
                NodeAdditions::Range(additions) => match additions {
                    TimeIndexWindow::Empty => 0,
                    TimeIndexWindow::Range { timeindex, range } => {
                        (&timeindex.edge_ts).range(range.clone()).len()
                    }
                    TimeIndexWindow::All(timeindex) => timeindex.edge_ts.len(),
                },
                #[cfg(feature = "storage")]
                NodeAdditions::Col(additions) => {
                    additions.clone().edge_events().map(|t| t.len()).sum()
                }
            }
        } else {
            self.history().count()
        }
    }

    fn is_empty(&self) -> bool {
        if !self.view.edges_filtered() {
            match &self.additions {
                NodeAdditions::Mem(additions) => additions.edge_ts.is_empty(),
                NodeAdditions::Range(additions) => match additions {
                    TimeIndexWindow::Empty => true,
                    TimeIndexWindow::Range { timeindex, range } => {
                        (&timeindex.edge_ts).range(range.clone()).is_empty()
                    }
                    TimeIndexWindow::All(timeindex) => timeindex.edge_ts.is_empty(),
                },
                #[cfg(feature = "storage")]
                NodeAdditions::Col(additions) => {
                    additions.clone().edge_events().all(|t| t.is_empty())
                }
            }
        } else {
            self.history().next().is_none()
        }
    }
}

impl<'b, G: GraphViewOps<'b>> TimeIndexOps<'b> for NodeHistory<'b, G> {
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.prop_history().active(w.clone()) || self.edge_history().active(w)
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self {
        let additions = self.additions.range(w);
        let view = self.view.clone();
        NodeHistory { additions, view }
    }

    fn iter(&self) -> BoxedLIter<'b, TimeIndexEntry> {
        self.prop_history()
            .iter()
            .merge(self.edge_history().iter())
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'b, TimeIndexEntry> {
        self.prop_history()
            .iter_rev()
            .merge_by(self.edge_history().iter_rev(), |t1, t2| t1 >= t2)
            .into_dyn_boxed()
    }

    fn len(&self) -> usize {
        self.prop_history().len() + self.edge_history().len()
    }

    fn is_empty(&self) -> bool {
        self.prop_history().is_empty() && self.edge_history().is_empty()
    }
}
