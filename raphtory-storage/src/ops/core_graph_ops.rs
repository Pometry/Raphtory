use raphtory::{
    core::entities::{
        edges::edge_ref::EdgeRef, properties::tprop::TProp, vertices::vertex_ref::VertexRef,
        LayerIds, VID,
    },
    db::api::view::internal::CoreGraphOps,
    prelude::*,
};

use crate::TemporalColumnarGraph;

// impl CoreGraphOps for TemporalColumnarGraph {
//     #[doc = " get the number of vertices in the main graph"]
//     fn unfiltered_num_vertices(&self) -> usize {
//         todo!()
//     }

//     fn get_layer_name(&self, layer_id: usize) -> Option<LockedView<String>> {
//         todo!()
//     }

//     fn get_layer_id(&self, name: &str) -> Option<usize> {
//         todo!()
//     }

//     #[doc = " Get the layer name for a given id"]
//     fn get_layer_names_from_ids(&self, layer_ids: LayerIds) -> Vec<String> {
//         todo!()
//     }

//     #[doc = " Returns the external ID for a vertex"]
//     fn vertex_id(&self, v: VID) -> u64 {
//         todo!()
//     }

//     #[doc = " Returns the string name for a vertex"]
//     fn vertex_name(&self, v: VID) -> String {
//         todo!()
//     }

//     #[doc = " Get all the addition timestamps for an edge"]
//     #[doc = " (this should always be global and not affected by windowing as deletion semantics may need information outside the current view!)"]
//     fn edge_additions(
//         &self,
//         eref: EdgeRef,
//         layer_ids: LayerIds,
//     ) -> LockedLayeredIndex<'_, TimeIndexEntry> {
//         todo!()
//     }

//     #[doc = " Get all the addition timestamps for a vertex"]
//     #[doc = " (this should always be global and not affected by windowing as deletion semantics may need information outside the current view!)"]
//     fn vertex_additions(&self, v: VID) -> LockedView<TimeIndex<i64>> {
//         todo!()
//     }

//     #[doc = " Gets the internal reference for an external vertex reference and keeps internal references unchanged."]
//     fn internalise_vertex(&self, v: VertexRef) -> Option<VID> {
//         todo!()
//     }

//     #[doc = " Gets the internal reference for an external vertex reference and keeps internal references unchanged. Assumes vertex exists!"]
//     fn internalise_vertex_unchecked(&self, v: VertexRef) -> VID {
//         todo!()
//     }

//     #[doc = " Lists the keys of all static properties of the graph"]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " Vec<String> - The keys of the static properties."]
//     fn static_prop_names(&self) -> Vec<String> {
//         todo!()
//     }

//     #[doc = " Gets a static graph property."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `name` - The name of the property."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " Option<Prop> - The property value if it exists."]
//     fn static_prop(&self, name: &str) -> Option<Prop> {
//         todo!()
//     }

//     #[doc = " Lists the keys of all temporal properties of the graph"]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " Vec<String> - The keys of the static properties."]
//     fn temporal_prop_names(&self) -> Vec<String> {
//         todo!()
//     }

//     #[doc = " Gets a temporal graph property."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `name` - The name of the property."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " Option<LockedView<TProp>> - The history of property values if it exists."]
//     fn temporal_prop(&self, name: &str) -> Option<LockedView<TProp>> {
//         todo!()
//     }

//     #[doc = " Gets a static property of a given vertex given the name and vertex reference."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `v` - A reference to the vertex for which the property is being queried."]
//     #[doc = " * `name` - The name of the property."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " Option<Prop> - The property value if it exists."]
//     fn static_vertex_prop(&self, v: VID, name: &str) -> Option<Prop> {
//         todo!()
//     }

//     #[doc = " Gets the keys of static properties of a given vertex"]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `v` - A reference to the vertex for which the property is being queried."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " Vec<String> - The keys of the static properties."]
//     fn static_vertex_prop_names<'a>(
//         &'a self,
//         v: VID,
//     ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
//         todo!()
//     }

//     #[doc = " Gets a temporal property of a given vertex given the name and vertex reference."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `v` - A reference to the vertex for which the property is being queried."]
//     #[doc = " * `name` - The name of the property."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " Option<LockedView<TProp>> - The history of property values if it exists."]
//     fn temporal_vertex_prop(&self, v: VID, name: &str) -> Option<LockedView<TProp>> {
//         todo!()
//     }

//     #[doc = " Returns a vector of all names of temporal properties within the given vertex"]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `v` - A reference to the vertex for which to retrieve the names."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " A vector of strings representing the names of the temporal properties"]
//     fn temporal_vertex_prop_names<'a>(
//         &'a self,
//         v: VID,
//     ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
//         todo!()
//     }

//     #[doc = " Returns a vector of all names of temporal properties that exist on at least one vertex"]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " A vector of strings representing the names of the temporal properties"]
//     fn all_vertex_prop_names(&self, is_static: bool) -> Vec<String> {
//         todo!()
//     }

//     #[doc = " Returns a vector of all names of temporal properties that exist on at least one vertex"]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " A vector of strings representing the names of the temporal properties"]
//     fn all_edge_prop_names(&self, is_static: bool) -> Vec<String> {
//         todo!()
//     }

//     #[doc = " Returns the static edge property with the given name for the"]
//     #[doc = " given edge reference."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `e` - An `EdgeRef` reference to the edge of interest."]
//     #[doc = " * `name` - A `String` containing the name of the temporal property."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " A property if it exists"]
//     fn static_edge_prop(&self, e: EdgeRef, name: &str, layer_ids: LayerIds) -> Option<Prop> {
//         todo!()
//     }

//     #[doc = " Returns a vector of keys for the static properties of the given edge reference."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `e` - An `EdgeRef` reference to the edge of interest."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " * A `Vec` of `String` containing the keys for the static properties of the given edge."]
//     fn static_edge_prop_names<'a>(
//         &'a self,
//         e: EdgeRef,
//         layer_ids: LayerIds,
//     ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
//         todo!()
//     }

//     #[doc = " Returns a vector of all temporal values of the edge property with the given name for the"]
//     #[doc = " given edge reference."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `e` - An `EdgeRef` reference to the edge of interest."]
//     #[doc = " * `name` - A `String` containing the name of the temporal property."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " A property if it exists"]
//     fn temporal_edge_prop(
//         &self,
//         e: EdgeRef,
//         name: &str,
//         layer_ids: LayerIds,
//     ) -> Option<LockedLayeredTProp> {
//         todo!()
//     }

//     #[doc = " Returns a vector of keys for the temporal properties of the given edge reference."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `e` - An `EdgeRef` reference to the edge of interest."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " * A `Vec` of `String` containing the keys for the temporal properties of the given edge."]
//     fn temporal_edge_prop_names<'a>(
//         &'a self,
//         e: EdgeRef,
//         layer_ids: LayerIds,
//     ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
//         todo!()
//     }

//     fn core_edges(&self) -> Box<dyn Iterator<Item = ArcEntry<EdgeStore>>> {
//         todo!()
//     }

//     fn core_edge(&self, eid: EID) -> ArcEntry<EdgeStore> {
//         todo!()
//     }

//     fn core_vertices(&self) -> Box<dyn Iterator<Item = ArcEntry<VertexStore>>> {
//         todo!()
//     }

//     fn core_vertex(&self, vid: VID) -> ArcEntry<VertexStore> {
//         todo!()
//     }
// }
