use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        Direction,
    },
    db::{
        api::{
            properties::{internal::PropertiesOps, Properties},
            view::{
                edge::EdgeListOps,
                internal::{
                    CoreGraphOps, EdgeFilterOps, GraphOps, InternalLayerOps, TimeSemantics,
                },
                BoxedLIter, IntoDynBoxed, TimeOps,
            },
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{EdgeViewOps, GraphViewOps, LayerOps},
};
use chrono::NaiveDateTime;

pub trait BaseNodeViewOps<'graph>: Clone + TimeOps<'graph> + LayerOps<'graph> {
    type BaseGraph: GraphViewOps<'graph>;
    type Graph: GraphViewOps<'graph>;
    type ValueType<T>: 'graph
    where
        T: 'graph;

    type PropType: PropertiesOps + Clone + 'graph;
    type PathType: NodeViewOps<'graph, BaseGraph = Self::BaseGraph, Graph = Self::BaseGraph>
        + 'graph;
    type Edge: EdgeViewOps<'graph, Graph = Self::Graph, BaseGraph = Self::BaseGraph> + 'graph;
    type EList: EdgeListOps<'graph, Edge = Self::Edge> + 'graph;

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
    ) -> Self::EList;

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
    type Edge: EdgeViewOps<'graph, Graph = Self::Graph, BaseGraph = Self::BaseGraph> + 'graph;
    type EList: EdgeListOps<'graph, Edge = Self::Edge> + 'graph;

    /// Get the numeric id of the node
    fn id(&self) -> Self::ValueType<u64>;

    /// Get the name of this node if a user has set one otherwise it returns the ID.
    ///
    /// Returns:
    ///
    /// The name of the node if one exists, otherwise the ID as a string.
    fn name(&self) -> Self::ValueType<String>;

    /// Get the timestamp for the earliest activity of the node
    fn earliest_time(&self) -> Self::ValueType<Option<i64>>;

    fn earliest_date_time(&self) -> Self::ValueType<Option<NaiveDateTime>>;

    /// Get the timestamp for the latest activity of the node
    fn latest_time(&self) -> Self::ValueType<Option<i64>>;

    fn latest_date_time(&self) -> Self::ValueType<Option<NaiveDateTime>>;

    /// Gets the history of the node (time that the node was added and times when changes were made to the node)
    fn history(&self) -> Self::ValueType<Vec<i64>>;

    /// Gets the history of the node (time that the node was added and times when changes were made to the node) as NaiveDateTime objects if parseable
    fn history_date_time(&self) -> Self::ValueType<Option<Vec<NaiveDateTime>>>;

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
    fn edges(&self) -> Self::EList;

    /// Get the edges that point into this node.
    ///
    /// Returns:
    ///
    /// An iterator over the edges that point into this node.
    fn in_edges(&self) -> Self::EList;

    /// Get the edges that point out of this node.
    ///
    /// Returns:
    ///
    /// An iterator over the edges that point out of this node.
    fn out_edges(&self) -> Self::EList;

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
    type Edge = V::Edge;
    type EList = V::EList;

    #[inline]
    fn id(&self) -> Self::ValueType<u64> {
        self.map(|g, v| g.node_id(v))
    }
    #[inline]
    fn name(&self) -> Self::ValueType<String> {
        self.map(|g, v| g.node_name(v))
    }
    #[inline]
    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.map(|g, v| g.node_earliest_time(v))
    }
    #[inline]
    fn earliest_date_time(&self) -> Self::ValueType<Option<NaiveDateTime>> {
        self.map(|g, v| NaiveDateTime::from_timestamp_millis(g.node_earliest_time(v)?))
    }

    #[inline]
    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.map(|g, v| g.node_latest_time(v))
    }

    #[inline]
    fn latest_date_time(&self) -> Self::ValueType<Option<NaiveDateTime>> {
        self.map(|g, v| NaiveDateTime::from_timestamp_millis(g.node_latest_time(v)?))
    }

    #[inline]
    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.map(|g, v| g.node_history(v))
    }
    #[inline]
    fn history_date_time(&self) -> Self::ValueType<Option<Vec<NaiveDateTime>>> {
        self.map(|g, v| {
            g.node_history(v)
                .iter()
                .map(|t| NaiveDateTime::from_timestamp_millis(*t))
                .collect::<Option<Vec<NaiveDateTime>>>()
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
    fn edges(&self) -> Self::EList {
        self.map_edges(|g, v| g.node_edges(v, Direction::BOTH, g.layer_ids(), g.edge_filter()))
    }
    #[inline]
    fn in_edges(&self) -> Self::EList {
        self.map_edges(|g, v| g.node_edges(v, Direction::IN, g.layer_ids(), g.edge_filter()))
    }
    #[inline]
    fn out_edges(&self) -> Self::EList {
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

/// A trait for operations on a list of nodes.
pub trait NodeListOps<'graph>: IntoIterator<Item = Self::ValueType<Self::Node>> + Sized {
    type Node: NodeViewOps<'graph> + TimeOps<'graph> + 'graph;
    type Neighbour: NodeViewOps<
        'graph,
        BaseGraph = <Self::Node as NodeViewOps<'graph>>::BaseGraph,
        Graph = <Self::Node as NodeViewOps<'graph>>::BaseGraph,
    >;
    type Edge: EdgeViewOps<
            'graph,
            BaseGraph = <Self::Node as NodeViewOps<'graph>>::BaseGraph,
            Graph = <Self::Node as NodeViewOps<'graph>>::Graph,
        > + 'graph;

    /// The type of the iterator for the list of nodes
    type IterType<T>: Iterator<
        Item = Self::ValueType<<Self::Node as NodeViewOps<'graph>>::ValueType<T>>,
    >
    where
        T: 'graph;
    /// The type of the iterator for the list of edges
    type ValueType<T>: 'graph
    where
        T: 'graph;

    /// Return the timestamp of the earliest activity.
    fn earliest_time(self) -> Self::IterType<Option<i64>>;

    /// Return the timestamp of the latest activity.
    fn latest_time(self) -> Self::IterType<Option<i64>>;

    /// Create views for the nodes including all events between `start` (inclusive) and `end` (exclusive)
    fn window(
        self,
        start: i64,
        end: i64,
    ) -> Self::IterType<<Self::Node as TimeOps<'graph>>::WindowedViewType>;

    /// Create views for the nodes including all events until `end` (inclusive)
    fn at(self, end: i64) -> Self::IterType<<Self::Node as TimeOps<'graph>>::WindowedViewType>;

    /// Returns the ids of nodes in the list.
    ///
    /// Returns:
    /// The ids of nodes in the list.
    fn id(self) -> Self::IterType<u64>;
    fn name(self) -> Self::IterType<String>;

    /// Returns an iterator over properties of the nodes
    fn properties(
        self,
    ) -> Self::IterType<Properties<<Self::Node as NodeViewOps<'graph>>::PropType>>;

    fn history(self) -> Self::IterType<Vec<i64>>;

    /// Returns an iterator over the degree of the nodes.
    ///
    /// Returns:
    /// An iterator over the degree of the nodes.
    fn degree(self) -> Self::IterType<usize>;

    /// Returns an iterator over the in-degree of the nodes.
    /// The in-degree of a node is the number of edges that connect to it from other nodes.
    ///
    /// Returns:
    /// An iterator over the in-degree of the nodes.
    fn in_degree(self) -> Self::IterType<usize>;

    /// Returns an iterator over the out-degree of the nodes.
    /// The out-degree of a node is the number of edges that connects to it from the node.
    ///
    /// Returns:
    ///
    /// An iterator over the out-degree of the nodes.
    fn out_degree(self) -> Self::IterType<usize>;

    /// Returns an iterator over the edges of the nodes.
    fn edges(self) -> Self::IterType<Self::Edge>;

    /// Returns an iterator over the incoming edges of the nodes.
    ///
    /// Returns:
    ///
    /// An iterator over the incoming edges of the nodes.
    fn in_edges(self) -> Self::IterType<Self::Edge>;

    /// Returns an iterator over the outgoing edges of the nodes.
    ///
    /// Returns:
    ///
    /// An iterator over the outgoing edges of the nodes.
    fn out_edges(self) -> Self::IterType<Self::Edge>;

    /// Returns an iterator over the neighbours of the nodes.
    ///
    /// Returns:
    ///
    /// An iterator over the neighbours of the nodes as NodeViews.
    fn neighbours(self) -> Self::IterType<Self::Neighbour>;

    /// Returns an iterator over the incoming neighbours of the nodes.
    ///
    /// Returns:
    ///
    /// An iterator over the incoming neighbours of the nodes as NodeViews.
    fn in_neighbours(self) -> Self::IterType<Self::Neighbour>;

    /// Returns an iterator over the outgoing neighbours of the nodes.
    ///
    /// Returns:
    ///
    /// An iterator over the outgoing neighbours of the nodes as NodeViews.
    fn out_neighbours(self) -> Self::IterType<Self::Neighbour>;
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> NodeListOps<'graph>
    for BoxedLIter<'graph, BoxedLIter<'graph, NodeView<G, GH>>>
{
    type Node = NodeView<G, GH>;
    type Neighbour = NodeView<G, G>;
    type Edge = EdgeView<G, GH>;
    type IterType<T> = BoxedLIter<'graph, BoxedLIter<'graph, T>> where T: 'graph;
    type ValueType<T> = BoxedLIter<'graph, T> where T: 'graph;

    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        self.map(|it| it.earliest_time()).into_dyn_boxed()
    }

    fn latest_time(self) -> Self::IterType<Option<i64>> {
        self.map(|it| it.latest_time()).into_dyn_boxed()
    }

    fn window(
        self,
        start: i64,
        end: i64,
    ) -> Self::IterType<<Self::Node as TimeOps<'graph>>::WindowedViewType> {
        self.map(move |it| it.window(start, end)).into_dyn_boxed()
    }

    fn at(self, end: i64) -> Self::IterType<<Self::Node as TimeOps<'graph>>::WindowedViewType> {
        self.map(move |it| it.at(end)).into_dyn_boxed()
    }

    fn id(self) -> Self::IterType<u64> {
        self.map(|it| it.id()).into_dyn_boxed()
    }

    fn name(self) -> Self::IterType<String> {
        self.map(|it| it.name()).into_dyn_boxed()
    }

    fn properties(
        self,
    ) -> Self::IterType<Properties<<Self::Node as NodeViewOps<'graph>>::PropType>> {
        self.map(|it| it.properties()).into_dyn_boxed()
    }

    fn history(self) -> Self::IterType<Vec<i64>> {
        self.map(|it| it.history()).into_dyn_boxed()
    }

    fn degree(self) -> Self::IterType<usize> {
        self.map(|it| it.degree()).into_dyn_boxed()
    }

    fn in_degree(self) -> Self::IterType<usize> {
        self.map(|it| it.in_degree()).into_dyn_boxed()
    }

    fn out_degree(self) -> Self::IterType<usize> {
        self.map(|it| it.out_degree()).into_dyn_boxed()
    }

    fn edges(self) -> Self::IterType<Self::Edge> {
        self.map(|it| it.edges()).into_dyn_boxed()
    }

    fn in_edges(self) -> Self::IterType<Self::Edge> {
        self.map(|it| it.in_edges()).into_dyn_boxed()
    }

    fn out_edges(self) -> Self::IterType<Self::Edge> {
        self.map(|it| it.out_edges()).into_dyn_boxed()
    }

    fn neighbours(self) -> Self::IterType<Self::Neighbour> {
        self.map(|it| it.neighbours()).into_dyn_boxed()
    }

    fn in_neighbours(self) -> Self::IterType<Self::Neighbour> {
        self.map(|it| it.in_neighbours()).into_dyn_boxed()
    }

    fn out_neighbours(self) -> Self::IterType<Self::Neighbour> {
        self.map(|it| it.out_neighbours()).into_dyn_boxed()
    }
}
