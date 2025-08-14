use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        storage::timeindex::AsTime,
    },
    db::api::{
        properties::internal::InternalPropertiesOps,
        state::{ops, NodeOp},
        view::{internal::OneHopFilter, node_edges, reset_filter::ResetFilter, TimeOps},
    },
    prelude::{EdgeViewOps, GraphViewOps, LayerOps},
};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use raphtory_api::core::Direction;
use raphtory_storage::graph::graph::GraphStorage;

pub trait BaseNodeViewOps<'graph>: Clone + TimeOps<'graph> + LayerOps<'graph> {
    type BaseGraph: GraphViewOps<'graph>;
    type Graph: GraphViewOps<'graph>;
    type ValueType<Op>: 'graph
    where
        Op: NodeOp + 'graph,
        Op::Output: 'graph;

    type PropType: InternalPropertiesOps + Clone + 'graph;
    type PathType: NodeViewOps<'graph, BaseGraph = Self::BaseGraph, Graph = Self::BaseGraph>
        + 'graph;
    type Edges: EdgeViewOps<'graph, Graph = Self::Graph, BaseGraph = Self::BaseGraph> + 'graph;

    fn graph(&self) -> &Self::Graph;

    fn map<F: NodeOp + Clone + 'graph>(&self, op: F) -> Self::ValueType<F>;
    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: Fn(&GraphStorage, &Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Edges;

    fn hop<
        I: Iterator<Item = VID> + Send + Sync + 'graph,
        F: for<'a> Fn(&GraphStorage, &'a Self::Graph, VID) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::PathType;
}

/// Operations defined for a node
pub trait NodeViewOps<'graph>: Clone + TimeOps<'graph> + LayerOps<'graph> {
    type BaseGraph: GraphViewOps<'graph>;
    type Graph: GraphViewOps<'graph>;
    type ValueType<T: NodeOp>: 'graph
    where
        T: 'graph,
        T::Output: 'graph;
    type PathType: NodeViewOps<'graph, BaseGraph = Self::BaseGraph, Graph = Self::BaseGraph>
        + 'graph;
    type Edges: EdgeViewOps<'graph, Graph = Self::Graph, BaseGraph = Self::BaseGraph> + 'graph;

    /// Get the numeric id of the node
    fn id(&self) -> Self::ValueType<ops::Id>;

    /// Get the name of this node if a user has set one otherwise it returns the ID.
    ///
    /// Returns:
    ///
    /// The name of the node if one exists, otherwise the ID as a string.
    fn name(&self) -> Self::ValueType<ops::Name>;

    /// Returns the type of node
    fn node_type(&self) -> Self::ValueType<ops::Type>;
    fn node_type_id(&self) -> Self::ValueType<ops::TypeId>;
    /// Get the timestamp for the earliest activity of the node
    fn earliest_time(&self) -> Self::ValueType<ops::EarliestTime<Self::Graph>>;

    fn earliest_date_time(
        &self,
    ) -> Self::ValueType<ops::Map<ops::EarliestTime<Self::Graph>, Option<DateTime<Utc>>>>;

    /// Get the timestamp for the latest activity of the node
    fn latest_time(&self) -> Self::ValueType<ops::LatestTime<Self::Graph>>;

    fn latest_date_time(
        &self,
    ) -> Self::ValueType<ops::Map<ops::LatestTime<Self::Graph>, Option<DateTime<Utc>>>>;

    /// Gets the history of the node (time that the node was added and times when changes were made to the node)
    fn history(&self) -> Self::ValueType<ops::History<Self::Graph>>;

    fn edge_history_count(&self) -> Self::ValueType<ops::EdgeHistoryCount<Self::Graph>>;

    /// Gets the history of the node (time that the node was added and times when changes were made to the node) as `DateTime<Utc>` objects if parseable
    fn history_date_time(
        &self,
    ) -> Self::ValueType<ops::Map<ops::History<Self::Graph>, Option<Vec<DateTime<Utc>>>>>;

    //Returns true if the node has any updates within the current window, otherwise false
    fn is_active(&self) -> Self::ValueType<ops::Map<ops::History<Self::Graph>, bool>>;

    /// Get a view of the temporal properties of this node.
    ///
    /// Returns:
    ///
    /// A view with the names of the properties as keys and the property values as values.
    fn properties(&self) -> Self::ValueType<ops::GetProperties<'graph, Self::Graph>>;

    /// Get a view of the metadata of this node.
    fn metadata(&self) -> Self::ValueType<ops::GetMetadata<'graph, Self::Graph>>;

    /// Get the degree of this node (i.e., the number of edges that are incident to it).
    ///
    /// Returns:
    ///
    /// The degree of this node.
    fn degree(&self) -> Self::ValueType<ops::Degree<Self::Graph>>;

    /// Get the in-degree of this node (i.e., the number of edges that point into it).
    ///
    /// Returns:
    ///
    /// The in-degree of this node.
    fn in_degree(&self) -> Self::ValueType<ops::Degree<Self::Graph>>;

    /// Get the out-degree of this node (i.e., the number of edges that point out of it).
    ///
    /// Returns:
    ///
    /// The out-degree of this node.
    fn out_degree(&self) -> Self::ValueType<ops::Degree<Self::Graph>>;

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
    type ValueType<T: NodeOp + 'graph>
        = V::ValueType<T>
    where
        T::Output: 'graph;
    type PathType = V::PathType;
    type Edges = V::Edges;

    #[inline]
    fn id(&self) -> Self::ValueType<ops::Id> {
        self.map(ops::Id)
    }

    #[inline]
    fn name(&self) -> Self::ValueType<ops::Name> {
        self.map(ops::Name)
    }
    #[inline]
    fn node_type(&self) -> Self::ValueType<ops::Type> {
        self.map(ops::Type)
    }
    #[inline]
    fn node_type_id(&self) -> Self::ValueType<ops::TypeId> {
        self.map(ops::TypeId)
    }
    #[inline]
    fn earliest_time(&self) -> Self::ValueType<ops::EarliestTime<Self::Graph>> {
        let op = ops::EarliestTime {
            graph: self.graph().clone(),
        };
        self.map(op)
    }
    #[inline]
    fn earliest_date_time(
        &self,
    ) -> Self::ValueType<ops::Map<ops::EarliestTime<Self::Graph>, Option<DateTime<Utc>>>> {
        let op = ops::EarliestTime {
            graph: self.graph().clone(),
        }
        .map(|t| t.and_then(|t| t.dt()));
        self.map(op)
    }

    #[inline]
    fn latest_time(&self) -> Self::ValueType<ops::LatestTime<Self::Graph>> {
        let op = ops::LatestTime {
            graph: self.graph().clone(),
        };
        self.map(op)
    }

    #[inline]
    fn latest_date_time(
        &self,
    ) -> Self::ValueType<ops::Map<ops::LatestTime<Self::Graph>, Option<DateTime<Utc>>>> {
        let op = ops::LatestTime {
            graph: self.graph().clone(),
        }
        .map(|t| t.and_then(|t| t.dt()));
        self.map(op)
    }

    #[inline]
    fn history(&self) -> Self::ValueType<ops::History<Self::Graph>> {
        let op = ops::History {
            graph: self.graph().clone(),
        };
        self.map(op)
    }

    #[inline]
    fn edge_history_count(&self) -> Self::ValueType<ops::EdgeHistoryCount<Self::Graph>> {
        let op = ops::EdgeHistoryCount {
            graph: self.graph().clone(),
        };
        self.map(op)
    }
    #[inline]
    fn history_date_time(
        &self,
    ) -> Self::ValueType<ops::Map<ops::History<Self::Graph>, Option<Vec<DateTime<Utc>>>>> {
        let op = ops::History {
            graph: self.graph().clone(),
        }
        .map(|h| h.into_iter().map(|t| t.dt()).collect());
        self.map(op)
    }

    fn is_active(&self) -> Self::ValueType<ops::Map<ops::History<Self::Graph>, bool>> {
        let op = ops::History {
            graph: self.graph().clone(),
        }
        .map(|h| !h.is_empty());
        self.map(op)
    }

    #[inline]
    fn properties(&self) -> Self::ValueType<ops::GetProperties<'graph, Self::Graph>> {
        let op = ops::GetProperties::new(self.graph().clone());
        self.map(op)
    }

    #[inline]
    fn metadata(&self) -> Self::ValueType<ops::GetMetadata<'graph, Self::Graph>> {
        let op = ops::GetMetadata::new(self.graph().clone());
        self.map(op)
    }

    #[inline]
    fn degree(&self) -> Self::ValueType<ops::Degree<Self::Graph>> {
        let op = ops::Degree {
            graph: self.graph().clone(),
            dir: Direction::BOTH,
        };
        self.map(op)
    }
    #[inline]
    fn in_degree(&self) -> Self::ValueType<ops::Degree<Self::Graph>> {
        let op = ops::Degree {
            graph: self.graph().clone(),
            dir: Direction::IN,
        };
        self.map(op)
    }
    #[inline]
    fn out_degree(&self) -> Self::ValueType<ops::Degree<Self::Graph>> {
        let op = ops::Degree {
            graph: self.graph().clone(),
            dir: Direction::OUT,
        };
        self.map(op)
    }
    #[inline]
    fn edges(&self) -> Self::Edges {
        self.map_edges(|cg, g, v| {
            let cg = cg.clone();
            let g = g.clone();
            node_edges(cg, g, v, Direction::BOTH)
        })
    }
    #[inline]
    fn in_edges(&self) -> Self::Edges {
        self.map_edges(|cg, g, v| {
            let cg = cg.clone();
            let g = g.clone();
            node_edges(cg, g, v, Direction::IN)
        })
    }
    #[inline]
    fn out_edges(&self) -> Self::Edges {
        self.map_edges(|cg, g, v| {
            let cg = cg.clone();
            let g = g.clone();
            node_edges(cg, g, v, Direction::OUT)
        })
    }
    #[inline]
    fn neighbours(&self) -> Self::PathType {
        self.hop(|cg, g, v| {
            let cg = cg.clone();
            let g = g.clone();
            node_edges(cg, g, v, Direction::BOTH)
                .map(|e| e.remote())
                .dedup()
        })
    }
    #[inline]
    fn in_neighbours(&self) -> Self::PathType {
        self.hop(|cg, g, v| {
            let cg = cg.clone();
            let g = g.clone();
            node_edges(cg, g, v, Direction::IN)
                .map(|e| e.remote())
                .dedup()
        })
    }
    #[inline]
    fn out_neighbours(&self) -> Self::PathType {
        self.hop(|cg, g, v| {
            let cg = cg.clone();
            let g = g.clone();
            node_edges(cg, g, v, Direction::OUT)
                .map(|e| e.remote())
                .dedup()
        })
    }
}

impl<'graph, V: BaseNodeViewOps<'graph> + OneHopFilter<'graph>> ResetFilter<'graph> for V {}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    const EDGES: [(i64, u64, u64); 6] = [
        (1, 0, 1),
        (2, 0, 2),
        (-1, 1, 0),
        (0, 0, 0),
        (7, 2, 1),
        (1, 0, 0),
    ];

    fn create_graph() -> Graph {
        let g = Graph::new();

        g.add_node(
            0,
            0,
            [("type", Prop::from("wallet")), ("cost", Prop::from(99.5))],
            None,
        )
        .unwrap();
        g.add_node(
            -1,
            1,
            [("type", Prop::from("wallet")), ("cost", Prop::from(10.0))],
            None,
        )
        .unwrap();
        g.add_node(
            6,
            2,
            [("type", Prop::from("wallet")), ("cost", Prop::from(76.0))],
            None,
        )
        .unwrap();

        for edge in EDGES {
            let (t, src, dst) = edge;

            g.add_edge(
                t,
                src,
                dst,
                [
                    ("prop1", Prop::from(1)),
                    ("prop2", Prop::from(9.8)),
                    ("prop3", Prop::from("test")),
                ],
                None,
            )
            .unwrap();
        }

        g
    }

    #[test]
    fn test_degree_iterable() {
        let g = create_graph();

        assert_eq!(g.nodes().degree().min(), Some(2));
        assert_eq!(g.nodes().degree().max(), Some(3));

        assert_eq!(g.nodes().in_degree().min(), Some(1));
        assert_eq!(g.nodes().in_degree().max(), Some(2));

        assert_eq!(g.nodes().out_degree().min(), Some(1));
        assert_eq!(g.nodes().out_degree().max(), Some(3));

        assert_eq!(g.nodes().degree().sum::<usize>(), 7);

        let mut degrees = g.nodes().degree().collect::<Vec<_>>();
        degrees.sort();
        assert_eq!(degrees, [2, 2, 3]);
    }
}
