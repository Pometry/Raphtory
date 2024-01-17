use std::{ops::Range, sync::Arc};

use once_cell::sync::Lazy;

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, properties::tprop::LayeredTProp, LayerIds, VID},
        storage::timeindex::TimeIndexOps,
    },
    db::api::view::{
        internal::{CoreGraphOps, EdgeFilter, EdgeWindowFilter, TimeSemantics},
        BoxedIter,
    },
    prelude::Prop,
};

use rayon::prelude::*;

use super::Graph2;

static WINDOW_FILTER: Lazy<EdgeWindowFilter> =
    Lazy::new(|| Arc::new(move |e, layer_ids, w| e.active(layer_ids, w)));

impl TimeSemantics for Graph2 {
    #[doc = " Returns the timestamp for the earliest activity"]
    fn earliest_time_global(&self) -> Option<i64> {
        Some(self.earliest())
    }

    #[doc = " Returns the timestamp for the latest activity"]
    fn latest_time_global(&self) -> Option<i64> {
        Some(self.latest())
    }

    #[doc = " Returns the timestamp for the earliest activity in the window"]
    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.all_edges_par(0) // TODO: will probably need to check all layers
            .filter_map(|edge| {
                let ts = edge.distinct_timestamps();
                let ts = ts.range(start..end);
                ts.first_t()
            })
            .min()
    }

    #[doc = " Returns the timestamp for the latest activity in the window"]
    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.all_edges_par(0) // TODO: will probably need to check all layers
            .filter_map(|edge| {
                let ts = edge.distinct_timestamps();
                let ts = ts.range(start..end);
                ts.last_t()
            })
            .max()
    }

    #[doc = " check if node `v` should be included in window `w`"]
    fn include_node_window(
        &self,
        v: VID,
        w: Range<i64>,
        layer_ids: &LayerIds,
        _edge_filter: Option<&EdgeFilter>,
    ) -> bool {
        let layer_id = match layer_ids {
            LayerIds::One(layer_id) => *layer_id,
            _ => panic!("only one layer id supported"),
        };

        let node = self.internal_node(v, layer_id);
        let timestamps = node.into_timestamps();
        timestamps.active(w.clone())
    }

    #[doc = " check if edge `e` should be included in window `w`"]
    fn include_edge_window(&self) -> &EdgeWindowFilter {
        &WINDOW_FILTER
    }

    #[doc = " Exploded edge iterator for edge `e`"]
    fn edge_exploded(&self, _e: EdgeRef, _layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        todo!()
    }

    #[doc = " Explode edge iterator for edge `e` for every layer"]
    fn edge_layers(&self, _e: EdgeRef, _layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        todo!()
    }

    #[doc = " Exploded edge iterator for edge`e` over window `w`"]
    fn edge_window_exploded(
        &self,
        _e: EdgeRef,
        _w: Range<i64>,
        _layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        todo!()
    }

    #[doc = " Exploded edge iterator for edge `e` over window `w` for every layer"]
    fn edge_window_layers(
        &self,
        _e: EdgeRef,
        _w: Range<i64>,
        _layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        todo!()
    }

    #[doc = " Get the time of the earliest activity of an edge"]
    fn edge_earliest_time(&self, _e: EdgeRef, _layer_ids: LayerIds) -> Option<i64> {
        todo!()
    }

    #[doc = " Get the time of the earliest activity of an edge `e` in window `w`"]
    fn edge_earliest_time_window(
        &self,
        _e: EdgeRef,
        _w: Range<i64>,
        _layer_ids: LayerIds,
    ) -> Option<i64> {
        todo!()
    }

    #[doc = " Get the time of the latest activity of an edge"]
    fn edge_latest_time(&self, _e: EdgeRef, _layer_ids: LayerIds) -> Option<i64> {
        todo!()
    }

    #[doc = " Get the time of the latest activity of an edge `e` in window `w`"]
    fn edge_latest_time_window(
        &self,
        _e: EdgeRef,
        _w: Range<i64>,
        _layer_ids: LayerIds,
    ) -> Option<i64> {
        todo!()
    }

    #[doc = " Get the edge deletions for use with materialize"]
    fn edge_deletion_history(&self, _e: EdgeRef, _layer_ids: LayerIds) -> Vec<i64> {
        todo!()
    }

    #[doc = " Get the edge deletions for use with materialize restricted to window `w`"]
    fn edge_deletion_history_window(
        &self,
        _e: EdgeRef,
        _w: Range<i64>,
        _layer_ids: LayerIds,
    ) -> Vec<i64> {
        todo!()
    }

    #[doc = " Check if graph has temporal property with the given id"]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `prop_id` - The id of the property to retrieve."]
    fn has_temporal_prop(&self, _prop_id: usize) -> bool {
        todo!()
    }

    #[doc = " Returns a vector of all temporal values of the graph property with the given id"]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `prop_id` - The id of the property to retrieve."]
    #[doc = ""]
    #[doc = " Returns:"]
    #[doc = ""]
    #[doc = " A vector of tuples representing the temporal values of the property"]
    #[doc = " that fall within the specified time window, where the first element of each tuple is the timestamp"]
    #[doc = " and the second element is the property value."]
    fn temporal_prop_vec(&self, _prop_id: usize) -> Vec<(i64, Prop)> {
        todo!()
    }

    #[doc = " Check if graph has temporal property with the given id in the window"]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `prop_id` - The id of the property to retrieve."]
    #[doc = " * `w` - time window"]
    fn has_temporal_prop_window(&self, _prop_id: usize, _w: Range<i64>) -> bool {
        todo!()
    }

    #[doc = " Returns a vector of all temporal values of the graph property with the given name"]
    #[doc = " that fall within the specified time window."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `name` - The name of the property to retrieve."]
    #[doc = " * `start` - The start time of the window to consider."]
    #[doc = " * `end` - The end time of the window to consider."]
    #[doc = ""]
    #[doc = " Returns:"]
    #[doc = ""]
    #[doc = " A vector of tuples representing the temporal values of the property"]
    #[doc = " that fall within the specified time window, where the first element of each tuple is the timestamp"]
    #[doc = " and the second element is the property value."]
    fn temporal_prop_vec_window(
        &self,
        _prop_id: usize,
        _start: i64,
        _end: i64,
    ) -> Vec<(i64, Prop)> {
        todo!()
    }

    #[doc = " Check if node has temporal property with the given id"]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `v` - The id of the node"]
    #[doc = " * `prop_id` - The id of the property to retrieve."]
    fn has_temporal_node_prop(&self, _v: VID, _prop_id: usize) -> bool {
        todo!()
    }

    #[doc = " Returns a vector of all temporal values of the node property with the given name for the"]
    #[doc = " given node"]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `v` - A reference to the node for which to retrieve the temporal property vector."]
    #[doc = " * `name` - The name of the property to retrieve."]
    #[doc = ""]
    #[doc = " Returns:"]
    #[doc = ""]
    #[doc = " A vector of tuples representing the temporal values of the property for the given node"]
    #[doc = " that fall within the specified time window, where the first element of each tuple is the timestamp"]
    #[doc = " and the second element is the property value."]
    fn temporal_node_prop_vec(&self, _v: VID, _id: usize) -> Vec<(i64, Prop)> {
        todo!()
    }

    #[doc = " Check if node has temporal property with the given id in the window"]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `v` - the id of the node"]
    #[doc = " * `prop_id` - The id of the property to retrieve."]
    #[doc = " * `w` - time window"]
    fn has_temporal_node_prop_window(&self, _v: VID, _prop_id: usize, _w: Range<i64>) -> bool {
        todo!()
    }

    #[doc = " Returns a vector of all temporal values of the node property with the given name for the given node"]
    #[doc = " that fall within the specified time window."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `v` - A reference to the node for which to retrieve the temporal property vector."]
    #[doc = " * `name` - The name of the property to retrieve."]
    #[doc = " * `start` - The start time of the window to consider."]
    #[doc = " * `end` - The end time of the window to consider."]
    #[doc = ""]
    #[doc = " Returns:"]
    #[doc = ""]
    #[doc = " A vector of tuples representing the temporal values of the property for the given node"]
    #[doc = " that fall within the specified time window, where the first element of each tuple is the timestamp"]
    #[doc = " and the second element is the property value."]
    fn temporal_node_prop_vec_window(
        &self,
        _v: VID,
        _id: usize,
        _start: i64,
        _end: i64,
    ) -> Vec<(i64, Prop)> {
        todo!()
    }

    #[doc = " Check if edge has temporal property with the given id in the window"]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `e` - the id of the edge"]
    #[doc = " * `prop_id` - The id of the property to retrieve."]
    #[doc = " * `w` - time window"]
    fn has_temporal_edge_prop_window(
        &self,
        _e: EdgeRef,
        prop_id: usize,
        _w: Range<i64>,
        layer_ids: LayerIds,
    ) -> bool {
        let layer_id = match layer_ids {
            LayerIds::One(layer_id) => layer_id,
            _ => panic!("only one layer id supported"),
        };
        self.edges_data_type(layer_id).get(prop_id).is_some()
    }

    #[doc = " Returns a vector of tuples containing the values of the temporal property with the given name"]
    #[doc = " for the given edge reference within the specified time window."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `e` - An `EdgeRef` reference to the edge of interest."]
    #[doc = " * `name` - A `String` containing the name of the temporal property."]
    #[doc = " * `start` - An `i64` containing the start time of the time window (inclusive)."]
    #[doc = " * `end` - An `i64` containing the end time of the time window (exclusive)."]
    #[doc = ""]
    #[doc = " Returns:"]
    #[doc = ""]
    #[doc = " * A `Vec` of tuples containing the values of the temporal property with the given name for the given edge"]
    #[doc = " within the specified time window."]
    #[doc = ""]
    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.temporal_edge_prop(e, id, layer_ids)
            .into_iter()
            .map(|t_props| t_props.iter_window(start..end).collect::<Vec<_>>())
            .next()
            .unwrap_or_default()
    }

    #[doc = " Check if edge has temporal property with the given id"]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `e` - The id of the edge"]
    #[doc = " * `prop_id` - The id of the property to retrieve."]
    fn has_temporal_edge_prop(&self, _e: EdgeRef, prop_id: usize, layer_ids: LayerIds) -> bool {
        let layer_id = match layer_ids {
            LayerIds::One(layer_id) => layer_id,
            _ => panic!("only one layer id supported"),
        };
        self.edges_data_type(layer_id).get(prop_id).is_some()
    }

    #[doc = " Returns a vector of tuples containing the values of the temporal property with the given name"]
    #[doc = " for the given edge reference."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `e` - An `EdgeRef` reference to the edge of interest."]
    #[doc = " * `name` - A `String` containing the name of the temporal property."]
    #[doc = ""]
    #[doc = " Returns:"]
    #[doc = ""]
    #[doc = " * A `Vec` of tuples containing the values of the temporal property with the given name for the given edge."]
    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        id: usize,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.temporal_edge_prop(e, id, layer_ids)
            .into_iter()
            .map(|t_props| t_props.iter().collect::<Vec<_>>())
            .next()
            .unwrap_or_default()
    }
}
