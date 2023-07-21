use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID, LayerIds},
        Prop,
    },
    db::api::view::internal::{time_semantics::TimeSemantics, CoreGraphOps},
};
use std::collections::HashMap;

/// Additional methods for retrieving properties that are automatically implemented
pub trait GraphPropertiesOps {
    /// Returns a hash map containing all the temporal properties of the given edge reference,
    /// where each key is the name of a temporal property and each value is a vector of tuples containing
    /// the property value and the time it was recorded.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge.
    ///
    /// # Returns
    ///
    /// * A `HashMap` containing all the temporal properties of the given edge, where each key is the name of a
    /// temporal property and each value is a vector of tuples containing the property value and the time it was recorded.
    ///
    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>>;

    fn static_edge_props(&self, e: EdgeRef) -> HashMap<String, Prop>;

    fn static_vertex_props(&self, v: VID) -> HashMap<String, Prop>;

    fn static_props(&self) -> HashMap<String, Prop>;

    fn temporal_props(&self) -> HashMap<String, Vec<(i64, Prop)>>;

    /// Returns a map of all temporal values of the vertex properties for the given vertex.
    /// The keys of the map are the names of the properties, and the values are vectors of tuples
    ///
    /// # Arguments
    ///
    /// - `v` - A reference to the vertex for which to retrieve the temporal property vector.
    ///
    /// # Returns
    /// - A map of all temporal values of the vertex properties for the given vertex.
    fn temporal_vertex_props(&self, v: VID) -> HashMap<String, Vec<(i64, Prop)>>;

    /// Returns a map of all temporal values of the vertex properties for the given vertex
    /// that fall within the specified time window.
    ///
    /// # Arguments
    ///
    /// - `v` - A reference to the vertex for which to retrieve the temporal property vector.
    /// - `t_start` - The start time of the window to consider (inclusive).
    /// - `t_end` - The end time of the window to consider (exclusive).
    ///
    /// # Returns
    /// - A map of all temporal values of the vertex properties for the given vertex
    fn temporal_vertex_props_window(
        &self,
        v: VID,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>>;

    /// Returns a hash map containing all the temporal properties of the given edge reference within the specified
    /// time window, where each key is the name of a temporal property and each value is a vector of tuples containing
    /// the property value and the time it was recorded.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge.
    /// * `t_start` - An `i64` containing the start time of the time window (inclusive).
    /// * `t_end` - An `i64` containing the end time of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// * A `HashMap` containing all the temporal properties of the given edge within the specified time window,
    /// where each key is the name of a temporal property and each value is a vector of tuples containing the property
    /// value and the time it was recorded.
    ///
    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>>;
}

impl<G: TimeSemantics + CoreGraphOps> GraphPropertiesOps for G {
    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>> {
        let mut map = HashMap::default();
        for name in self.temporal_edge_prop_names(e) {
            map.insert(name.clone(), self.temporal_edge_prop_vec(e, &name, LayerIds::All));
        }
        map
    }

    fn static_edge_props(&self, e: EdgeRef) -> HashMap<String, Prop> {
        let mut map = HashMap::default();
        for name in self.static_edge_prop_names(e) {
            if let Some(p) = self.static_edge_prop(e, &name) {
                map.insert(name.clone(), p);
            };
        }
        map
    }

    fn static_vertex_props(&self, v: VID) -> HashMap<String, Prop> {
        let mut map = HashMap::default();
        for name in self.static_vertex_prop_names(v) {
            if let Some(p) = self.static_vertex_prop(v, &name) {
                map.insert(name.clone(), p);
            }
        }
        map
    }

    fn static_props(&self) -> HashMap<String, Prop> {
        let mut map = HashMap::default();
        for name in self.static_prop_names() {
            if let Some(p) = self.static_prop(&name) {
                map.insert(name.clone(), p);
            }
        }
        map
    }

    fn temporal_props(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        let mut map = HashMap::default();
        for name in self.temporal_prop_names() {
            map.insert(name.clone(), self.temporal_prop_vec(&name));
        }
        map
    }

    fn temporal_vertex_props(&self, v: VID) -> HashMap<String, Vec<(i64, Prop)>> {
        let mut map = HashMap::default();
        for name in self.temporal_vertex_prop_names(v) {
            map.insert(name.clone(), self.temporal_vertex_prop_vec(v, &name));
        }
        map
    }

    fn temporal_vertex_props_window(
        &self,
        v: VID,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        let mut map = HashMap::default();
        for name in self.temporal_vertex_prop_names(v) {
            map.insert(
                name.clone(),
                self.temporal_vertex_prop_vec_window(v, &name, t_start, t_end),
            );
        }
        map
    }

    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        let mut map = HashMap::default();
        for name in self.temporal_edge_prop_names(e) {
            map.insert(
                name.clone(),
                self.temporal_edge_prop_vec_window(e, &name, t_start, t_end, LayerIds::All),
            );
        }
        map
    }
}
