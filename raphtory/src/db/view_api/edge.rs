use crate::core::edge_ref::EdgeRef;
use crate::core::vertex_ref::VertexRef;
use crate::core::Prop;
use crate::db::view_api::internal::GraphViewInternalOps;
use crate::db::view_api::{GraphViewOps, VertexListOps, VertexViewOps};
use std::collections::HashMap;
use std::sync::Arc;

pub trait EdgeViewInternalOps<G: GraphViewOps, V: VertexViewOps<Graph = G>> {
    fn graph(&self) -> Arc<G>;

    fn eref(&self) -> EdgeRef;

    fn new_vertex(&self, v: VertexRef) -> V;

    fn new_edge(&self, e: EdgeRef) -> Self;
}

pub trait EdgeViewOps: EdgeViewInternalOps<Self::Graph, Self::Vertex> {
    type Graph: GraphViewOps;
    type Vertex: VertexViewOps<Graph = Self::Graph>;
    type EList: EdgeListOps<Graph = Self::Graph, Vertex = Self::Vertex>;

    fn property(&self, name: String, include_static: bool) -> Option<Prop> {
        let props = self.property_history(name.clone());
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

    fn property_history(&self, name: String) -> Vec<(i64, Prop)> {
        match self.eref().time() {
            None => self.graph().temporal_edge_props_vec(self.eref(), name),
            Some(t) => self.graph().temporal_edge_props_vec_window(
                self.eref(),
                name,
                t,
                t.saturating_add(1),
            ),
        }
    }

    fn history(&self) -> Vec<i64> {
        self.graph().edge_timestamps(self.eref(), None)
    }

    fn properties(&self, include_static: bool) -> HashMap<String, Prop> {
        let mut props: HashMap<String, Prop> = self
            .property_histories()
            .iter()
            .map(|(key, values)| (key.clone(), values.last().unwrap().1.clone()))
            .collect();

        if include_static {
            for prop_name in self.graph().static_edge_prop_names(self.eref()) {
                if let Some(prop) = self
                    .graph()
                    .static_edge_prop(self.eref(), prop_name.clone())
                {
                    props.insert(prop_name, prop);
                }
            }
        }
        props
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
    fn has_property(&self, name: String, include_static: bool) -> bool {
        (!self.property_history(name.clone()).is_empty())
            || (include_static
                && self
                    .graph()
                    .static_edge_prop_names(self.eref())
                    .contains(&name))
    }

    fn has_static_property(&self, name: String) -> bool {
        self.graph()
            .static_edge_prop_names(self.eref())
            .contains(&name)
    }

    fn static_property(&self, name: String) -> Option<Prop> {
        self.graph().static_edge_prop(self.eref(), name)
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

    fn active(&self, t: i64) -> bool {
        self.graph().has_edge_ref_window(
            self.eref().src(),
            self.eref().dst(),
            t,
            t.saturating_add(1),
            self.eref().layer(),
        )
    }

    /// Explodes an edge and returns all instances it had been updated as seperate edges
    fn explode(&self) -> Self::EList;

    /// Gets the first time an edge was seen
    fn earliest_time(&self) -> Option<i64> {
        self.graph()
            .edge_timestamps(self.eref(), None)
            .first()
            .copied()
    }

    /// Gets the latest time an edge was updated
    fn latest_time(&self) -> Option<i64> {
        self.graph()
            .edge_timestamps(self.eref(), None)
            .last()
            .copied()
    }

    /// Gets the time stamp of the edge if it is exploded
    fn time(&self) -> Option<i64> {
        self.eref().time()
    }

    /// Gets the name of the layer this edge belongs to
    fn layer_name(&self) -> String {
        if self.eref().layer() == 0 {
            "default layer".to_string()
        } else {
            self.graph().get_layer_name_by_id(self.eref().layer())
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

    /// gets a property of an edge with the given name
    /// includes the timestamp of the property
    fn property_history(self, name: String) -> Self::IterType<Vec<(i64, Prop)>>;
    fn property_histories(self) -> Self::IterType<HashMap<String, Vec<(i64, Prop)>>>;

    /// gets the source vertices of the edges in the list
    fn src(self) -> Self::VList;

    /// gets the destination vertices of the edges in the list
    fn dst(self) -> Self::VList;

    /// returns a list of exploded edges that include an edge at each point in time
    fn explode(self) -> Self::IterType<Self::Edge>;

    /// Get the timestamp for the earliest activity of the edge
    fn earliest_time(self) -> Self::IterType<Option<i64>>;

    /// Get the timestamp for the latest activity of the edge
    fn latest_time(self) -> Self::IterType<Option<i64>>;
}
