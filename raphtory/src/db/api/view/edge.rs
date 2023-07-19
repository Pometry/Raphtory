use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef, LayerIds},
        Prop,
    },
    db::api::view::{internal::*, *},
};
use std::collections::HashMap;

pub trait EdgeViewInternalOps<G: GraphViewOps, V: VertexViewOps<Graph = G>> {
    fn graph(&self) -> G;

    fn eref(&self) -> EdgeRef;

    fn new_vertex(&self, v: VertexRef) -> V;

    fn new_edge(&self, e: EdgeRef) -> Self;
}

pub trait EdgeViewOps: EdgeViewInternalOps<Self::Graph, Self::Vertex> {
    type Graph: GraphViewOps;
    type Vertex: VertexViewOps<Graph = Self::Graph>;
    type EList: EdgeListOps<Graph = Self::Graph, Vertex = Self::Vertex>;

    fn property(&self, name: &str, include_static: bool) -> Option<Prop> {
        let props = self.property_history(name);
        match props.last() {
            None => {
                if include_static {
                    self.graph().static_edge_prop(self.eref(), name)
                } else {
                    None
                }
            }
            Some((_, prop)) => Some(prop.clone()),
        }
    }

    fn property_history(&self, name: &str) -> Vec<(i64, Prop)> {
        match self.eref().time() {
            None => self.graph().temporal_edge_prop_vec(self.eref(), name),
            Some(t) => self.graph().temporal_edge_prop_vec_window(
                self.eref(),
                name,
                t,
                t.saturating_add(1),
            ),
        }
    }

    fn history(&self) -> Vec<i64> {
        self.graph()
            .edge_t(self.eref())
            .map(|e| e.time().expect("exploded"))
            .collect()
    }

    fn properties(&self, include_static: bool) -> HashMap<String, Prop> {
        self.property_names(include_static)
            .into_iter()
            .filter_map(|key| self.property(&key, include_static).map(|v| (key, v)))
            .collect()
    }

    fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        // match on the self.edge.time option property and run two function s
        // one for static and one for temporal
        match self.eref().time() {
            None => self.graph().temporal_edge_props(self.eref()),
            Some(t) => self
                .graph()
                .temporal_edge_props_window(self.eref(), t, t.saturating_add(1)),
        }
    }

    fn property_names(&self, include_static: bool) -> Vec<String> {
        let mut names: Vec<String> = self.graph().temporal_edge_prop_names(self.eref());
        if include_static {
            names.extend(self.graph().static_edge_prop_names(self.eref()))
        }
        names
    }

    fn has_property(&self, name: &str, include_static: bool) -> bool {
        (!self.property_history(name).is_empty())
            || (include_static
                && self
                    .graph()
                    .static_edge_prop_names(self.eref())
                    .contains(&name.to_owned()))
    }

    fn has_static_property(&self, name: &str) -> bool {
        self.graph()
            .static_edge_prop_names(self.eref())
            .contains(&name.to_owned())
    }

    /// Returns static property of an edge by name
    fn static_property(&self, name: &str) -> Option<Prop> {
        self.graph().static_edge_prop(self.eref(), name)
    }

    /// Returns all static properties of an edge
    fn static_properties(&self) -> HashMap<String, Prop> {
        self.graph().static_edge_props(self.eref())
    }

    /// Returns the source vertex of the edge.
    fn src(&self) -> Self::Vertex {
        let vertex = self.eref().src();
        self.new_vertex(vertex)
    }

    /// Returns the destination vertex of the edge.
    fn dst(&self) -> Self::Vertex {
        let vertex = self.eref().dst();
        self.new_vertex(vertex)
    }

    /// Check if edge is active at a given time point
    fn active(&self, t: i64) -> bool {
        match self.eref().time() {
            Some(tt) => tt <= t && t <= self.latest_time().unwrap_or(tt),
            None => self.graph().has_edge_ref_window(
                self.eref().src(),
                self.eref().dst(),
                t,
                t.saturating_add(1),
                LayerIds::All,
            ),
        }
    }

    /// Returns the id of the edge.
    fn id(
        &self,
    ) -> (
        <Self::Vertex as VertexViewOps>::ValueType<u64>,
        <Self::Vertex as VertexViewOps>::ValueType<u64>,
    ) {
        (self.src().id(), self.dst().id())
    }

    /// Explodes an edge and returns all instances it had been updated as seperate edges
    fn explode(&self) -> Self::EList;

    /// Gets the first time an edge was seen
    fn earliest_time(&self) -> Option<i64> {
        self.graph().edge_earliest_time(self.eref())
    }

    /// Gets the latest time an edge was updated
    fn latest_time(&self) -> Option<i64> {
        self.graph().edge_latest_time(self.eref())
    }

    /// Gets the time stamp of the edge if it is exploded
    fn time(&self) -> Option<i64> {
        self.eref().time()
    }

    /// Gets the name of the layer this edge belongs to
    fn layer_names(&self) -> Vec<String> {
        if let Some(layer_ids) = self.graph().get_layer_ids(self.eref().pid()) {
            match layer_ids {
                LayerIds::All => self.graph().get_unique_layers(),
                LayerIds::One(id) => vec![self.graph().get_layer_name_by_id(id)],
                LayerIds::Multiple(ids) => ids
                    .iter()
                    .map(|id| self.graph().get_layer_name_by_id(*id))
                    .collect(),
            }
        } else {
            vec![]
        }
    }
}

/// This trait defines the operations that can be
/// performed on a list of edges in a temporal graph view.
pub trait EdgeListOps:
    IntoIterator<Item = Self::ValueType<Self::Edge>, IntoIter = Self::IterType<Self::Edge>> + Sized
{
    type Graph: GraphViewOps;
    type Vertex: VertexViewOps<Graph = Self::Graph>;
    type Edge: EdgeViewOps<Graph = Self::Graph, Vertex = Self::Vertex>;
    type ValueType<T>;

    /// the type of list of vertices
    type VList: VertexListOps<Graph = Self::Graph, Vertex = Self::Vertex>;

    /// the type of iterator
    type IterType<T>: Iterator<Item = Self::ValueType<T>>;

    fn has_property(self, name: String, include_static: bool) -> Self::IterType<bool>;

    fn property(self, name: String, include_static: bool) -> Self::IterType<Option<Prop>>;
    fn properties(self, include_static: bool) -> Self::IterType<HashMap<String, Prop>>;
    fn property_names(self, include_static: bool) -> Self::IterType<Vec<String>>;

    fn has_static_property(self, name: String) -> Self::IterType<bool>;
    fn static_property(self, name: String) -> Self::IterType<Option<Prop>>;
    fn static_properties(self) -> Self::IterType<HashMap<String, Prop>>;

    /// gets a property of an edge with the given name
    /// includes the timestamp of the property
    fn property_history(self, name: String) -> Self::IterType<Vec<(i64, Prop)>>;
    fn property_histories(self) -> Self::IterType<HashMap<String, Vec<(i64, Prop)>>>;

    /// gets the source vertices of the edges in the list
    fn src(self) -> Self::VList;

    /// gets the destination vertices of the edges in the list
    fn dst(self) -> Self::VList;

    fn id(self) -> Self::IterType<(u64, u64)>;

    /// returns a list of exploded edges that include an edge at each point in time
    fn explode(self) -> Self::IterType<Self::Edge>;

    /// Get the timestamp for the earliest activity of the edge
    fn earliest_time(self) -> Self::IterType<Option<i64>>;

    /// Get the timestamp for the latest activity of the edge
    fn latest_time(self) -> Self::IterType<Option<i64>>;
}
