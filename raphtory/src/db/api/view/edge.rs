use chrono::NaiveDateTime;

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::time::{error::ParseTimeError, IntoTime},
        ArcStr,
    },
    db::{
        api::{
            properties::{
                internal::{ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps},
                Properties,
            },
            view::{internal::*, *},
        },
        graph::{
            edge::EdgeView,
            views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
        },
    },
};

pub trait EdgeViewInternalOps<G: GraphViewOps, V: VertexViewOps<Graph = G>> {
    fn graph(&self) -> G;

    fn eref(&self) -> EdgeRef;

    fn new_vertex(&self, v: VID) -> V;

    fn new_edge(&self, e: EdgeRef) -> Self;
}

pub trait EdgeViewOps:
    EdgeViewInternalOps<Self::Graph, Self::Vertex>
    + ConstPropertiesOps
    + TemporalPropertiesOps
    + TemporalPropertyViewOps
    + TimeOps
    + Sized
    + Clone
{
    type Graph: GraphViewOps;
    type Vertex: VertexViewOps<Graph = Self::Graph>;
    type EList: EdgeListOps<Graph = Self::Graph, Vertex = Self::Vertex>;

    /// list the activation timestamps for the edge
    fn history(&self) -> Vec<i64> {
        let layer_ids = self.graph().layer_ids().constrain_from_edge(self.eref());
        self.graph()
            .edge_exploded(self.eref(), layer_ids)
            .map(|e| *e.time().expect("exploded").t())
            .collect()
    }

    /// Return a view of the properties of the edge
    fn properties(&self) -> Properties<Self> {
        Properties::new(self.clone())
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
        let layer_ids = self.graph().layer_ids().constrain_from_edge(self.eref());
        match self.eref().time() {
            Some(tt) => *tt.t() <= t && t <= self.latest_time().unwrap_or(*tt.t()),
            None => self.graph().include_edge_window(
                &self.graph().core_edge(self.eref().pid()),
                t..t.saturating_add(1),
                &layer_ids,
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

    fn explode_layers(&self) -> Self::EList;

    /// Gets the first time an edge was seen
    fn earliest_time(&self) -> Option<i64> {
        let layer_ids = self.graph().layer_ids().constrain_from_edge(self.eref());
        self.graph().edge_earliest_time(self.eref(), layer_ids)
    }

    fn earliest_date_time(&self) -> Option<NaiveDateTime> {
        let layer_ids = self.graph().layer_ids().constrain_from_edge(self.eref());
        let earliest_time = self.graph().edge_earliest_time(self.eref(), layer_ids);
        NaiveDateTime::from_timestamp_millis(earliest_time?)
    }

    fn latest_date_time(&self) -> Option<NaiveDateTime> {
        let layer_ids = self.graph().layer_ids().constrain_from_edge(self.eref());
        let latest_time = self.graph().edge_latest_time(self.eref(), layer_ids);
        NaiveDateTime::from_timestamp_millis(latest_time?)
    }

    /// Gets the latest time an edge was updated
    fn latest_time(&self) -> Option<i64> {
        let layer_ids = self.graph().layer_ids().constrain_from_edge(self.eref());
        self.graph().edge_latest_time(self.eref(), layer_ids)
    }

    fn start_date_time(&self) -> Option<NaiveDateTime> {
        self.graph()
            .start()
            .map(|t| NaiveDateTime::from_timestamp_millis(t).unwrap())
    }

    fn end_date_time(&self) -> Option<NaiveDateTime> {
        self.graph()
            .end()
            .map(|t| NaiveDateTime::from_timestamp_millis(t).unwrap())
    }

    /// Gets the time stamp of the edge if it is exploded
    fn time(&self) -> Option<i64> {
        self.eref().time().map(|ti| *ti.t())
    }

    fn date_time(&self) -> Option<NaiveDateTime> {
        self.eref()
            .time()
            .map(|ti| NaiveDateTime::from_timestamp_millis(*ti.t()).unwrap())
    }

    /// Gets the layer name for the edge if it is restricted to a single layer
    fn layer_name(&self) -> Option<ArcStr> {
        self.eref()
            .layer()
            .map(|l_id| self.graph().get_layer_name(*l_id))
    }

    /// Gets the TimeIndexEntry if the edge is exploded
    fn time_and_index(&self) -> Option<TimeIndexEntry> {
        self.eref().time()
    }

    /// Gets the name of the layer this edge belongs to
    fn layer_names(&self) -> BoxedIter<ArcStr> {
        let layer_ids = self
            .graph()
            .edge_layer_ids(&self.graph().core_edge(self.eref().pid()))
            .constrain_from_edge(self.eref());
        self.graph().get_layer_names_from_ids(layer_ids)
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
    fn properties(self) -> Self::IterType<Properties<Self::Edge>>;

    /// gets the source vertices of the edges in the list
    fn src(self) -> Self::VList;

    /// gets the destination vertices of the edges in the list
    fn dst(self) -> Self::VList;

    fn id(self) -> Self::IterType<(u64, u64)>;

    /// returns a list of exploded edges that include an edge at each point in time
    fn explode(self) -> Self::IterType<Self::Edge>;

    /// Get the timestamp for the earliest activity of the edge
    fn earliest_time(self) -> Self::IterType<Option<i64>>;

    fn earliest_date_time(self) -> Self::IterType<Option<NaiveDateTime>>;

    /// Get the timestamp for the latest activity of the edge
    fn latest_time(self) -> Self::IterType<Option<i64>>;

    fn latest_date_time(self) -> Self::IterType<Option<NaiveDateTime>>;

    fn date_time(self) -> Self::IterType<Option<NaiveDateTime>>;

    /// Get the timestamps of the edges if they are  exploded
    fn time(self) -> Self::IterType<Option<i64>>;

    /// Get the layer name for each edge if it is restricted to a single layer
    fn layer_name(self) -> Self::IterType<Option<ArcStr>>;

    fn layer_names(self) -> Self::IterType<Vec<ArcStr>>;

    fn history(self) -> Self::IterType<Vec<i64>>;

    fn start(self) -> Self::IterType<Option<i64>>;

    fn start_date_time(self) -> Self::IterType<Option<NaiveDateTime>>;

    fn end(self) -> Self::IterType<Option<i64>>;

    fn end_date_time(self) -> Self::IterType<Option<NaiveDateTime>>;

    fn at<T: IntoTime>(self, t: T) -> Self::IterType<<Self::Edge as TimeOps>::WindowedViewType>;

    fn window<T: IntoTime>(
        self,
        t_start: T,
        t_end: T,
    ) -> Self::IterType<<Self::Edge as TimeOps>::WindowedViewType>;
}

#[cfg(test)]
mod test_edge_view {
    use crate::prelude::*;

    #[test]
    fn test_exploded_edge_properties() {
        let g = Graph::new();
        let actual_prop_values = vec![0, 1, 2, 3];
        for v in actual_prop_values.iter() {
            g.add_edge(0, 1, 2, [("test", *v)], None).unwrap();
        }

        let prop_values: Vec<_> = g
            .edge(1, 2)
            .unwrap()
            .explode()
            .flat_map(|e| e.properties().get("test").into_i32())
            .collect();
        assert_eq!(prop_values, actual_prop_values)
    }

    #[test]
    fn test_exploded_edge_multilayer() {
        let g = Graph::new();
        let expected_prop_values = vec![0, 1, 2, 3];
        for v in expected_prop_values.iter() {
            g.add_edge(0, 1, 2, [("test", *v)], Some((v % 2).to_string().as_str()))
                .unwrap();
        }

        let prop_values: Vec<_> = g
            .edge(1, 2)
            .unwrap()
            .explode()
            .flat_map(|e| e.properties().get("test").into_i32())
            .collect();
        let actual_layers: Vec<_> = g
            .edge(1, 2)
            .unwrap()
            .explode()
            .map(|e| e.layer_names().into_iter().next().unwrap())
            .collect();
        let expected_layers: Vec<_> = expected_prop_values
            .iter()
            .map(|v| (v % 2).to_string())
            .collect();
        assert_eq!(prop_values, expected_prop_values);
        assert_eq!(actual_layers, expected_layers);
    }

    #[test]
    fn test_sorting_by_secondary_index() {
        let g = Graph::new();
        g.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(0, 1, 2, [("second", true)], None).unwrap();
        g.add_edge(0, 2, 3, [("second", true)], None).unwrap();

        let mut exploded_edges: Vec<_> = g.edges().explode().collect();
        exploded_edges.sort_by_key(|a| a.time_and_index());

        let res: Vec<_> = exploded_edges
            .into_iter()
            .map(|e| {
                (
                    e.src().id(),
                    e.dst().id(),
                    e.properties().get("second").into_bool(),
                )
            })
            .collect();
        assert_eq!(
            res,
            vec![
                (2, 3, None),
                (1, 2, None),
                (1, 2, Some(true)),
                (2, 3, Some(true))
            ]
        )
    }
}
