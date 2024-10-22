use std::iter;

use enum_dispatch::enum_dispatch;
use raphtory_api::{core::{entities::{edges::edge_ref::EdgeRef, properties::props::Meta, LayerIds, NodeRef, EID, GID, VID}, storage::arc_str::ArcStr}, BoxedIter};

use crate::{core::{entities::{properties::{graph_meta::GraphMeta, tprop::TProp}, Prop}, storage::locked_view::LockedView}, db::api::storage::graph::{edges::{edge_entry::EdgeStorageEntry, edges::EdgesStorage}, nodes::{node_entry::NodeStorageEntry, nodes::NodesStorage}, GraphStorage}};

#[enum_dispatch]
pub trait CoreGraphOps {
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
                Box::new((0..ids.len()).map(move |index| {
                    let id = ids[index];
                    keys[id].clone()
                }))
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
        self.core_graph().node_entry(v).id()
    }

    /// Returns the string name for a node
    #[inline]
    fn node_name(&self, v: VID) -> String {
        let node = self.core_node_entry(v);
        node.name()
            // .map(|name| name.to_string())
            // .unwrap_or_else(|| node.id().to_str().to_string())
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
    fn constant_node_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
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
            GraphStorage::Mem(storage) => {
                storage.graph().core_get_const_edge_prop(e, id, layer_ids)
            }
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
    fn const_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        match self.core_graph() {
            GraphStorage::Mem(storage) => storage.graph().core_const_edge_prop_ids(e, layer_ids),
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
        layer_ids: &LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        // FIXME once the disk storage can handle multiple layers this can be implemented generically over the EdgeStorageEntry
        match self.core_graph() {
            GraphStorage::Mem(storage) => storage.graph().core_temporal_edge_prop_ids(e, layer_ids),
            GraphStorage::Unlocked(storage) => storage.core_temporal_edge_prop_ids(e, layer_ids),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.core_temporal_edge_prop_ids(e, layer_ids),
        }
    }
}