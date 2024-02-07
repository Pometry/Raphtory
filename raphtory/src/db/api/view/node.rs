use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        storage::timeindex::AsTime,
        Direction,
    },
    db::api::{
        properties::{internal::PropertiesOps, Properties},
        view::{
            internal::{CoreGraphOps, EdgeFilterOps, GraphOps, InternalLayerOps, TimeSemantics},
            TimeOps,
        },
    },
    prelude::{EdgeViewOps, GraphViewOps, LayerOps},
};
use chrono::{DateTime, Utc};
use crate::core::ArcStr;

pub trait BaseNodeViewOps<'graph>: Clone + TimeOps<'graph> + LayerOps<'graph> {
    type BaseGraph: GraphViewOps<'graph>;
    type Graph: GraphViewOps<'graph>;
    type ValueType<T>: 'graph
    where
        T: 'graph;

    type PropType: PropertiesOps + Clone + 'graph;
    type PathType: NodeViewOps<'graph, BaseGraph = Self::BaseGraph, Graph = Self::BaseGraph>
        + 'graph;
    type Edges: EdgeViewOps<'graph, Graph = Self::Graph, BaseGraph = Self::BaseGraph> + 'graph;

    fn map<O: 'graph, F: Fn(&Self::Graph, VID) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O>;

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>>;

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges;

    fn hop<
        I: Iterator<Item = VID> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType;
}

/// Operations defined for a node
pub trait NodeViewOps<'graph>: Clone + TimeOps<'graph> + LayerOps<'graph> {
    type BaseGraph: GraphViewOps<'graph>;
    type Graph: GraphViewOps<'graph>;
    type ValueType<T>: 'graph
    where
        T: 'graph;
    type PathType: NodeViewOps<'graph, BaseGraph = Self::BaseGraph, Graph = Self::BaseGraph>
        + 'graph;
    type PropType: PropertiesOps + Clone + 'graph;
    type Edges: EdgeViewOps<'graph, Graph = Self::Graph, BaseGraph = Self::BaseGraph> + 'graph;

    /// Get the numeric id of the node
    fn id(&self) -> Self::ValueType<u64>;

    /// Get the name of this node if a user has set one otherwise it returns the ID.
    ///
    /// Returns:
    ///
    /// The name of the node if one exists, otherwise the ID as a string.
    fn name(&self) -> Self::ValueType<String>;

    /// Returns the type of node
    fn node_type(&self) -> Self::ValueType<Option<ArcStr>>;

    /// Get the timestamp for the earliest activity of the node
    fn earliest_time(&self) -> Self::ValueType<Option<i64>>;

    fn earliest_date_time(&self) -> Self::ValueType<Option<DateTime<Utc>>>;

    /// Get the timestamp for the latest activity of the node
    fn latest_time(&self) -> Self::ValueType<Option<i64>>;

    fn latest_date_time(&self) -> Self::ValueType<Option<DateTime<Utc>>>;

    /// Gets the history of the node (time that the node was added and times when changes were made to the node)
    fn history(&self) -> Self::ValueType<Vec<i64>>;

    /// Gets the history of the node (time that the node was added and times when changes were made to the node) as DateTime<Utc> objects if parseable
    fn history_date_time(&self) -> Self::ValueType<Option<Vec<DateTime<Utc>>>>;

    /// Get a view of the temporal properties of this node.
    ///
    /// Returns:
    ///
    /// A view with the names of the properties as keys and the property values as values.
    fn properties(&self) -> Self::ValueType<Properties<Self::PropType>>;

    /// Get the degree of this node (i.e., the number of edges that are incident to it).
    ///
    /// Returns:
    ///
    /// The degree of this node.
    fn degree(&self) -> Self::ValueType<usize>;

    /// Get the in-degree of this node (i.e., the number of edges that point into it).
    ///
    /// Returns:
    ///
    /// The in-degree of this node.
    fn in_degree(&self) -> Self::ValueType<usize>;

    /// Get the out-degree of this node (i.e., the number of edges that point out of it).
    ///
    /// Returns:
    ///
    /// The out-degree of this node.
    fn out_degree(&self) -> Self::ValueType<usize>;

    /// Get the edges that are incident to this node.
    ///
    /// Returns:
    ///
    /// An iterator over the edges that are incident to this node.
    fn edges(&self) -> Self::Edges;

    /// Get the edges that point into this node.
    ///
    /// Returns:
    ///
    /// An iterator over the edges that point into this node.
    fn in_edges(&self) -> Self::Edges;

    /// Get the edges that point out of this node.
    ///
    /// Returns:
    ///
    /// An iterator over the edges that point out of this node.
    fn out_edges(&self) -> Self::Edges;

    /// Get the neighbours of this node.
    ///
    /// Returns:
    ///
    /// An iterator over the neighbours of this node.
    fn neighbours(&self) -> Self::PathType;

    /// Get the neighbours of this node that point into this node.
    ///
    /// Returns:
    ///
    /// An iterator over the neighbours of this node that point into this node.
    fn in_neighbours(&self) -> Self::PathType;

    /// Get the neighbours of this node that point out of this node.
    ///
    /// Returns:
    ///
    /// An iterator over the neighbours of this node that point out of this node.
    fn out_neighbours(&self) -> Self::PathType;
}

impl<'graph, V: BaseNodeViewOps<'graph> + 'graph> NodeViewOps<'graph> for V {
    type BaseGraph = V::BaseGraph;
    type Graph = V::Graph;
    type ValueType<T: 'graph> = V::ValueType<T>;
    type PathType = V::PathType;
    type PropType = V::PropType;
    type Edges = V::Edges;

    #[inline]
    fn id(&self) -> Self::ValueType<u64> {
        self.map(|g, v| g.node_id(v))
    }
    #[inline]
    fn name(&self) -> Self::ValueType<String> {
        self.map(|g, v| g.node_name(v))
    }
    #[inline]
    fn node_type(&self) -> Self::ValueType<Option<ArcStr>> {
        self.map(|g, v| g.node_type(v))
    }
    #[inline]
    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.map(|g, v| g.node_earliest_time(v))
    }
    #[inline]
    fn earliest_date_time(&self) -> Self::ValueType<Option<DateTime<Utc>>> {
        self.map(|g, v| g.node_earliest_time(v)?.dt())
    }

    #[inline]
    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.map(|g, v| g.node_latest_time(v))
    }

    #[inline]
    fn latest_date_time(&self) -> Self::ValueType<Option<DateTime<Utc>>> {
        self.map(|g, v| g.node_latest_time(v)?.dt())
    }

    #[inline]
    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.map(|g, v| g.node_history(v))
    }
    #[inline]
    fn history_date_time(&self) -> Self::ValueType<Option<Vec<DateTime<Utc>>>> {
        self.map(|g, v| {
            g.node_history(v)
                .iter()
                .map(|t| t.dt())
                .collect::<Option<Vec<_>>>()
        })
    }

    #[inline]
    fn properties(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.as_props()
    }
    #[inline]
    fn degree(&self) -> Self::ValueType<usize> {
        self.map(|g, v| g.degree(v, Direction::BOTH, &g.layer_ids(), g.edge_filter()))
    }
    #[inline]
    fn in_degree(&self) -> Self::ValueType<usize> {
        self.map(|g, v| g.degree(v, Direction::IN, &g.layer_ids(), g.edge_filter()))
    }
    #[inline]
    fn out_degree(&self) -> Self::ValueType<usize> {
        self.map(|g, v| g.degree(v, Direction::OUT, &g.layer_ids(), g.edge_filter()))
    }
    #[inline]
    fn edges(&self) -> Self::Edges {
        self.map_edges(|g, v| g.node_edges(v, Direction::BOTH, g.layer_ids(), g.edge_filter()))
    }
    #[inline]
    fn in_edges(&self) -> Self::Edges {
        self.map_edges(|g, v| g.node_edges(v, Direction::IN, g.layer_ids(), g.edge_filter()))
    }
    #[inline]
    fn out_edges(&self) -> Self::Edges {
        self.map_edges(|g, v| g.node_edges(v, Direction::OUT, g.layer_ids(), g.edge_filter()))
    }
    #[inline]
    fn neighbours(&self) -> Self::PathType {
        self.hop(|g, v| g.neighbours(v, Direction::BOTH, g.layer_ids(), g.edge_filter()))
    }
    #[inline]
    fn in_neighbours(&self) -> Self::PathType {
        self.hop(|g, v| g.neighbours(v, Direction::IN, g.layer_ids(), g.edge_filter()))
    }
    #[inline]
    fn out_neighbours(&self) -> Self::PathType {
        self.hop(|g, v| g.neighbours(v, Direction::OUT, g.layer_ids(), g.edge_filter()))
    }
}
