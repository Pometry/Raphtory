use crate::graph::{
    edges::{edge_entry::EdgeStorageEntry, edges::EdgesStorage},
    graph::GraphStorage,
    locked::LockedGraph,
    nodes::{node_entry::NodeStorageEntry, node_storage_ops::NodeStorageOps, nodes::NodesStorage},
};
use raphtory_api::{
    core::{
        entities::{
            properties::{meta::Meta, prop::Prop},
            GidType, LayerIds, EID, GID, VID,
        },
        storage::arc_str::ArcStr,
    },
    inherit::Base,
    iter::{BoxedIter, BoxedLIter},
};
use raphtory_core::{
    entities::{
        nodes::node_ref::NodeRef,
        properties::{graph_meta::GraphMeta, tprop::TProp},
    },
    storage::locked_view::LockedView,
};
use std::{
    iter,
    sync::{atomic::Ordering, Arc},
};

/// Check if two Graph views point at the same underlying storage
pub fn is_view_compatible(g1: &impl CoreGraphOps, g2: &impl CoreGraphOps) -> bool {
    g1.core_graph().ptr_eq(g2.core_graph())
}

/// Core functions that should (almost-)always be implemented by pointing at the underlying graph.
pub trait CoreGraphOps: Send + Sync {
    fn id_type(&self) -> Option<GidType> {
        match self.core_graph() {
            GraphStorage::Mem(graph) => graph.inner().logical_to_physical.dtype(),
            GraphStorage::Unlocked(graph) => graph.logical_to_physical.dtype(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => Some(storage.inner().id_type()),
        }
    }

    // fn num_shards(&self) -> usize {
    //     match self.core_graph() {
    //         GraphStorage::Mem(LockedGraph { graph, .. }) | GraphStorage::Unlocked(graph) => {
    //             graph.storage.num_shards()
    //         }
    //         #[cfg(feature = "storage")]
    //         GraphStorage::Disk(_) => 1,
    //     }
    // }

    /// get the current sequence id without incrementing the counter
    fn read_event_id(&self) -> usize {
        match self.core_graph() {
            GraphStorage::Unlocked(graph) => graph.read_event_counter(),
            GraphStorage::Mem(graph) => graph.inner().read_event_counter(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.count_temporal_edges(),
        }
    }

    /// get the number of nodes in the main graph
    #[inline]
    fn unfiltered_num_nodes(&self) -> usize {
        self.core_graph().unfiltered_num_nodes()
    }

    /// get the number of edges in the main graph
    #[inline]
    fn unfiltered_num_edges(&self) -> usize {
        self.core_graph().unfiltered_num_edges()
    }

    /// get the number of layers in the main graph
    #[inline]
    fn unfiltered_num_layers(&self) -> usize {
        self.core_graph().unfiltered_num_layers()
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
        self.core_graph().core_nodes()
    }

    #[inline]
    fn core_node(&self, vid: VID) -> NodeStorageEntry {
        self.core_graph().core_node(vid)
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
        self.core_graph().core_node(v).id().into()
    }

    /// Returns the string name for a node
    #[inline]
    fn node_name(&self, v: VID) -> String {
        let node = self.core_node(v);
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
        let node = self.core_node(v);
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
        let core_node_entry = self.core_node(v);
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
        let core_node_entry = self.core_node(v);
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
        let core_node_entry = self.core_node(v);
        core_node_entry.temporal_prop_ids()
    }
}

impl CoreGraphOps for GraphStorage {
    #[inline]
    fn core_graph(&self) -> &GraphStorage {
        self
    }
}

pub trait InheritCoreGraphOps: Base {}

impl<G: InheritCoreGraphOps + Send + Sync> CoreGraphOps for G
where
    G::Base: CoreGraphOps,
{
    #[inline]
    fn core_graph(&self) -> &GraphStorage {
        self.base().core_graph()
    }
}

impl<T: ?Sized> InheritCoreGraphOps for Arc<T> {}
impl<T: ?Sized> InheritCoreGraphOps for &T {}
