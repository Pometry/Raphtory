use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            nodes::node_ref::NodeRef,
            properties::{graph_meta::GraphMeta, props::Meta, tprop::TProp},
            LayerIds, ELID, VID,
        },
        storage::{
            locked_view::LockedView,
            timeindex::{TimeIndex, TimeIndexOps, TimeIndexWindow},
        },
        ArcStr, Prop,
    },
    db::api::{
        storage::{
            edges::{
                edge_entry::EdgeStorageEntry, edge_owned_entry::EdgeOwnedEntry, edges::EdgesStorage,
            },
            nodes::{
                node_entry::NodeStorageEntry, node_owned_entry::NodeOwnedEntry, nodes::NodesStorage,
            },
            storage_ops::GraphStorage,
        },
        view::{internal::Base, BoxedIter},
    },
};
use enum_dispatch::enum_dispatch;
use std::ops::Range;

#[cfg(feature = "arrow")]
use raphtory_arrow::timestamps::TimeStamps;
#[cfg(feature = "arrow")]
use rayon::prelude::*;

/// Core functions that should (almost-)always be implemented by pointing at the underlying graph.
#[enum_dispatch]
pub trait CoreGraphOps {
    /// get the number of nodes in the main graph
    fn unfiltered_num_nodes(&self) -> usize;

    fn unfiltered_num_edges(&self) -> usize;

    fn unfiltered_num_layers(&self) -> usize;

    fn core_graph(&self) -> GraphStorage;

    fn core_edges(&self) -> EdgesStorage;
    fn core_edge(&self, eid: ELID) -> EdgeStorageEntry;

    fn core_edge_arc(&self, eid: ELID) -> EdgeOwnedEntry;
    fn core_nodes(&self) -> NodesStorage;

    fn core_node_entry(&self, vid: VID) -> NodeStorageEntry;

    fn core_node_arc(&self, vid: VID) -> NodeOwnedEntry;

    fn node_meta(&self) -> &Meta;

    fn edge_meta(&self) -> &Meta;

    fn graph_meta(&self) -> &GraphMeta;

    fn get_layer_name(&self, layer_id: usize) -> ArcStr;

    fn get_layer_id(&self, name: &str) -> Option<usize>;

    /// Get the layer name for a given id
    fn get_layer_names_from_ids(&self, layer_ids: &LayerIds) -> BoxedIter<ArcStr>;

    /// Get all node types
    fn get_all_node_types(&self) -> Vec<ArcStr>;

    /// Returns the external ID for a node
    fn node_id(&self, v: VID) -> u64;

    /// Returns the string name for a node
    fn node_name(&self, v: VID) -> String;

    /// Returns the type of node
    fn node_type(&self, v: VID) -> Option<ArcStr>;

    fn node_type_id(&self, v: VID) -> usize;

    /// Gets the internal reference for an external node reference and keeps internal references unchanged.
    fn internalise_node(&self, v: NodeRef) -> Option<VID>;

    /// Gets the internal reference for an external node reference and keeps internal references unchanged. Assumes node exists!
    fn internalise_node_unchecked(&self, v: NodeRef) -> VID;

    /// Gets a static graph property.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// Returns:
    ///
    /// Option<Prop> - The property value if it exists.
    fn constant_prop(&self, id: usize) -> Option<Prop>;

    /// Gets a temporal graph property.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// Returns:
    ///
    /// Option<LockedView<TProp>> - The history of property values if it exists.
    fn temporal_prop(&self, id: usize) -> Option<LockedView<TProp>>;

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
    fn constant_node_prop(&self, v: VID, id: usize) -> Option<Prop>;

    /// Gets the keys of constant properties of a given node
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the node for which the property is being queried.
    ///
    /// Returns:
    ///
    /// The keys of the constant properties.
    fn constant_node_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_>;

    /// Returns a vector of all ids of temporal properties within the given node
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the node for which to retrieve the names.
    ///
    /// Returns:
    ///
    /// the ids of the temporal properties
    fn temporal_node_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_>;

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
    fn get_const_edge_prop(&self, e: EdgeRef, id: usize, layer_ids: LayerIds) -> Option<Prop>;

    /// Returns a vector of keys for the static properties of the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    ///
    /// Returns:
    ///
    /// the keys for the constant properties of the given edge.
    fn const_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_>;

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
        layer_ids: &LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_>;
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

impl<G: DelegateCoreOps + ?Sized> CoreGraphOps for G {
    #[inline]
    fn unfiltered_num_nodes(&self) -> usize {
        self.graph().unfiltered_num_nodes()
    }

    #[inline]
    fn unfiltered_num_layers(&self) -> usize {
        self.graph().unfiltered_num_layers()
    }

    #[inline]
    fn core_graph(&self) -> GraphStorage {
        self.graph().core_graph()
    }

    #[inline]
    fn node_meta(&self) -> &Meta {
        self.graph().node_meta()
    }

    #[inline]
    fn edge_meta(&self) -> &Meta {
        self.graph().edge_meta()
    }

    #[inline]
    fn graph_meta(&self) -> &GraphMeta {
        self.graph().graph_meta()
    }

    #[inline]
    fn get_layer_name(&self, layer_id: usize) -> ArcStr {
        self.graph().get_layer_name(layer_id)
    }

    #[inline]
    fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.graph().get_layer_id(name)
    }

    #[inline]
    fn get_layer_names_from_ids(&self, layer_ids: &LayerIds) -> BoxedIter<ArcStr> {
        self.graph().get_layer_names_from_ids(layer_ids)
    }

    #[inline]
    fn get_all_node_types(&self) -> Vec<ArcStr> {
        self.graph().node_meta().get_all_node_types()
    }

    #[inline]
    fn node_id(&self, v: VID) -> u64 {
        self.graph().node_id(v)
    }

    #[inline]
    fn node_name(&self, v: VID) -> String {
        self.graph().node_name(v)
    }

    #[inline]
    fn node_type(&self, v: VID) -> Option<ArcStr> {
        self.graph().node_type(v)
    }

    #[inline]
    fn internalise_node(&self, v: NodeRef) -> Option<VID> {
        self.graph().internalise_node(v)
    }

    #[inline]
    fn internalise_node_unchecked(&self, v: NodeRef) -> VID {
        self.graph().internalise_node_unchecked(v)
    }

    #[inline]
    fn constant_prop(&self, id: usize) -> Option<Prop> {
        self.graph().constant_prop(id)
    }

    #[inline]
    fn temporal_prop(&self, id: usize) -> Option<LockedView<TProp>> {
        self.graph().temporal_prop(id)
    }

    #[inline]
    fn constant_node_prop(&self, v: VID, id: usize) -> Option<Prop> {
        self.graph().constant_node_prop(v, id)
    }

    #[inline]
    fn constant_node_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        self.graph().constant_node_prop_ids(v)
    }
    #[inline]
    fn temporal_node_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        self.graph().temporal_node_prop_ids(v)
    }

    #[inline]
    fn get_const_edge_prop(&self, e: EdgeRef, id: usize, layer_ids: LayerIds) -> Option<Prop> {
        self.graph().get_const_edge_prop(e, id, layer_ids)
    }

    #[inline]
    fn const_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        self.graph().const_edge_prop_ids(e, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: &LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        self.graph().temporal_edge_prop_ids(e, layer_ids)
    }

    #[inline]
    fn core_edges(&self) -> EdgesStorage {
        self.graph().core_edges()
    }

    #[inline]
    fn core_nodes(&self) -> NodesStorage {
        self.graph().core_nodes()
    }

    #[inline]
    fn core_edge(&self, eid: ELID) -> EdgeStorageEntry {
        self.graph().core_edge(eid)
    }

    #[inline]
    fn core_edge_arc(&self, eid: ELID) -> EdgeOwnedEntry {
        self.graph().core_edge_arc(eid)
    }

    #[inline]
    fn core_node_entry(&self, vid: VID) -> NodeStorageEntry {
        self.graph().core_node_entry(vid)
    }

    fn core_node_arc(&self, vid: VID) -> NodeOwnedEntry {
        self.graph().core_node_arc(vid)
    }

    #[inline]
    fn unfiltered_num_edges(&self) -> usize {
        self.graph().unfiltered_num_edges()
    }
}

pub enum NodeAdditions<'a> {
    Mem(&'a TimeIndex<i64>),
    Range(TimeIndexWindow<'a, i64>),
    #[cfg(feature = "arrow")]
    Col(Vec<TimeStamps<'a, i64>>),
}

impl<'b> TimeIndexOps for NodeAdditions<'b> {
    type IndexType = i64;
    type RangeType<'a> = NodeAdditions<'a> where Self: 'a;

    fn active(&self, w: Range<i64>) -> bool {
        match self {
            NodeAdditions::Mem(index) => index.active_t(w),
            #[cfg(feature = "arrow")]
            NodeAdditions::Col(index) => index.par_iter().any(|index| index.active_t(w.clone())),
            NodeAdditions::Range(index) => index.active_t(w),
        }
    }

    fn range(&self, w: Range<i64>) -> Self::RangeType<'_> {
        match self {
            NodeAdditions::Mem(index) => NodeAdditions::Range(index.range(w)),
            #[cfg(feature = "arrow")]
            NodeAdditions::Col(index) => {
                let mut ranges = Vec::with_capacity(index.len());
                index
                    .par_iter()
                    .map(|index| index.range_t(w.clone()))
                    .collect_into_vec(&mut ranges);
                NodeAdditions::Col(ranges)
            }
            NodeAdditions::Range(index) => NodeAdditions::Range(index.range(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            NodeAdditions::Mem(index) => index.first(),
            #[cfg(feature = "arrow")]
            NodeAdditions::Col(index) => index.par_iter().flat_map(|index| index.first()).min(),
            NodeAdditions::Range(index) => index.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            NodeAdditions::Mem(index) => index.last(),
            #[cfg(feature = "arrow")]
            NodeAdditions::Col(index) => index.par_iter().flat_map(|index| index.last()).max(),
            NodeAdditions::Range(index) => index.last(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = i64> + Send + '_> {
        match self {
            NodeAdditions::Mem(index) => index.iter(),
            #[cfg(feature = "arrow")]
            NodeAdditions::Col(index) => Box::new(index.iter().flat_map(|index| index.iter())),
            NodeAdditions::Range(index) => index.iter(),
        }
    }

    fn len(&self) -> usize {
        match self {
            NodeAdditions::Mem(index) => index.len(),
            NodeAdditions::Range(range) => range.len(),
            #[cfg(feature = "arrow")]
            NodeAdditions::Col(col) => col.len(),
        }
    }

    fn node_type_id(&self, v: VID) -> usize {
        self.graph().node_type_id(v)
    }
}
