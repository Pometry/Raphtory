use std::ops::Range;

use raphtory::{
    core::entities::{
        edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
        LayerIds, VID,
    },
    db::api::view::{
        internal::{EdgeFilter, TimeSemantics},
        BoxedIter,
    },
    prelude::Prop,
};

use crate::TemporalColumnarGraph;

// impl TimeSemantics for TemporalColumnarGraph {
//     #[doc = " Returns the timestamp for the earliest activity"]
//     fn earliest_time_global(&self) -> Option<i64> {
//         todo!()
//     }

//     #[doc = " Returns the timestamp for the latest activity"]
//     fn latest_time_global(&self) -> Option<i64> {
//         todo!()
//     }

//     #[doc = " Returns the timestamp for the earliest activity in the window"]
//     fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
//         todo!()
//     }

//     #[doc = " Returns the timestamp for the latest activity in the window"]
//     fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
//         todo!()
//     }

//     #[doc = " check if vertex `v` should be included in window `w`"]
//     fn include_vertex_window(
//         &self,
//         v: VID,
//         w: Range<i64>,
//         layer_ids: &LayerIds,
//         edge_filter: Option<&EdgeFilter>,
//     ) -> bool {
//         todo!()
//     }

//     #[doc = " check if edge `e` should be included in window `w`"]
//     fn include_edge_window(&self, e: &EdgeStore, w: Range<i64>, layer_ids: &LayerIds) -> bool {
//         todo!()
//     }

//     #[doc = " Exploded edge iterator for edge `e`"]
//     fn edge_exploded(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
//         todo!()
//     }

//     #[doc = " Explode edge iterator for edge `e` for every layer"]
//     fn edge_layers(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
//         todo!()
//     }

//     #[doc = " Exploded edge iterator for edge`e` over window `w`"]
//     fn edge_window_exploded(
//         &self,
//         e: EdgeRef,
//         w: Range<i64>,
//         layer_ids: LayerIds,
//     ) -> BoxedIter<EdgeRef> {
//         todo!()
//     }

//     #[doc = " Exploded edge iterator for edge `e` over window `w` for every layer"]
//     fn edge_window_layers(
//         &self,
//         e: EdgeRef,
//         w: Range<i64>,
//         layer_ids: LayerIds,
//     ) -> BoxedIter<EdgeRef> {
//         todo!()
//     }

//     #[doc = " Get the time of the earliest activity of an edge"]
//     fn edge_earliest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
//         todo!()
//     }

//     #[doc = " Get the time of the earliest activity of an edge `e` in window `w`"]
//     fn edge_earliest_time_window(
//         &self,
//         e: EdgeRef,
//         w: Range<i64>,
//         layer_ids: LayerIds,
//     ) -> Option<i64> {
//         todo!()
//     }

//     #[doc = " Get the time of the latest activity of an edge"]
//     fn edge_latest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
//         todo!()
//     }

//     #[doc = " Get the time of the latest activity of an edge `e` in window `w`"]
//     fn edge_latest_time_window(
//         &self,
//         e: EdgeRef,
//         w: Range<i64>,
//         layer_ids: LayerIds,
//     ) -> Option<i64> {
//         todo!()
//     }

//     #[doc = " Get the edge deletions for use with materialize"]
//     fn edge_deletion_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
//         todo!()
//     }

//     #[doc = " Get the edge deletions for use with materialize restricted to window `w`"]
//     fn edge_deletion_history_window(
//         &self,
//         e: EdgeRef,
//         w: Range<i64>,
//         layer_ids: LayerIds,
//     ) -> Vec<i64> {
//         todo!()
//     }

//     #[doc = " Returns a vector of all temporal values of the graph property with the given name"]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `name` - The name of the property to retrieve."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " A vector of tuples representing the temporal values of the property"]
//     #[doc = " that fall within the specified time window, where the first element of each tuple is the timestamp"]
//     #[doc = " and the second element is the property value."]
//     fn temporal_prop_vec(&self, name: &str) -> Vec<(i64, Prop)> {
//         todo!()
//     }

//     #[doc = " Returns a vector of all temporal values of the graph property with the given name"]
//     #[doc = " that fall within the specified time window."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `name` - The name of the property to retrieve."]
//     #[doc = " * `t_start` - The start time of the window to consider."]
//     #[doc = " * `t_end` - The end time of the window to consider."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " A vector of tuples representing the temporal values of the property"]
//     #[doc = " that fall within the specified time window, where the first element of each tuple is the timestamp"]
//     #[doc = " and the second element is the property value."]
//     fn temporal_prop_vec_window(&self, name: &str, t_start: i64, t_end: i64) -> Vec<(i64, Prop)> {
//         todo!()
//     }

//     #[doc = " Returns a vector of all temporal values of the vertex property with the given name for the"]
//     #[doc = " given vertex"]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `v` - A reference to the vertex for which to retrieve the temporal property vector."]
//     #[doc = " * `name` - The name of the property to retrieve."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " A vector of tuples representing the temporal values of the property for the given vertex"]
//     #[doc = " that fall within the specified time window, where the first element of each tuple is the timestamp"]
//     #[doc = " and the second element is the property value."]
//     fn temporal_vertex_prop_vec(&self, v: VID, name: &str) -> Vec<(i64, Prop)> {
//         todo!()
//     }

//     #[doc = " Returns a vector of all temporal values of the vertex property with the given name for the given vertex"]
//     #[doc = " that fall within the specified time window."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `v` - A reference to the vertex for which to retrieve the temporal property vector."]
//     #[doc = " * `name` - The name of the property to retrieve."]
//     #[doc = " * `t_start` - The start time of the window to consider."]
//     #[doc = " * `t_end` - The end time of the window to consider."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " A vector of tuples representing the temporal values of the property for the given vertex"]
//     #[doc = " that fall within the specified time window, where the first element of each tuple is the timestamp"]
//     #[doc = " and the second element is the property value."]
//     fn temporal_vertex_prop_vec_window(
//         &self,
//         v: VID,
//         name: &str,
//         t_start: i64,
//         t_end: i64,
//     ) -> Vec<(i64, Prop)> {
//         todo!()
//     }

//     #[doc = " Returns a vector of tuples containing the values of the temporal property with the given name"]
//     #[doc = " for the given edge reference within the specified time window."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `e` - An `EdgeRef` reference to the edge of interest."]
//     #[doc = " * `name` - A `String` containing the name of the temporal property."]
//     #[doc = " * `t_start` - An `i64` containing the start time of the time window (inclusive)."]
//     #[doc = " * `t_end` - An `i64` containing the end time of the time window (exclusive)."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " * A `Vec` of tuples containing the values of the temporal property with the given name for the given edge"]
//     #[doc = " within the specified time window."]
//     #[doc = ""]
//     fn temporal_edge_prop_vec_window(
//         &self,
//         e: EdgeRef,
//         name: &str,
//         t_start: i64,
//         t_end: i64,
//         layer_ids: LayerIds,
//     ) -> Vec<(i64, Prop)> {
//         todo!()
//     }

//     #[doc = " Returns a vector of tuples containing the values of the temporal property with the given name"]
//     #[doc = " for the given edge reference."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `e` - An `EdgeRef` reference to the edge of interest."]
//     #[doc = " * `name` - A `String` containing the name of the temporal property."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " * A `Vec` of tuples containing the values of the temporal property with the given name for the given edge."]
//     fn temporal_edge_prop_vec(
//         &self,
//         e: EdgeRef,
//         name: &str,
//         layer_ids: LayerIds,
//     ) -> Vec<(i64, Prop)> {
//         todo!()
//     }
// }
