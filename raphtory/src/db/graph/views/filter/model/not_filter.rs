use crate::{
    db::{
        api::{
            state::ops::{filter::NotOp, node::NodeOp, NodeFilterOp},
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            edge_op::{EdgeExistsOp, EdgeFilterOp, EdgeFilterOpExt},
            entity_op_filtered_graph::EntityOpFilteredGraph,
            model::{
                edge_filter::CompositeEdgeFilter,
                exploded_edge_filter::CompositeExplodedEdgeFilter,
                node_filter::CompositeNodeFilter, ComposableFilter, FilterTree,
                TryAsCompositeFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
            not_filtered_graph::NotFilteredGraph,
            CreateFilter, LeafKinds,
        },
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotFilter<T>(pub T);

impl<T: Display> Display for NotFilter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NOT({})", self.0)
    }
}

impl<T> ComposableFilter for NotFilter<T> {}

impl<T: CreateFilter + Clone> CreateFilter for NotFilter<T> {
    /// Boxed: an exploded composite keeps the wrapper graphs, whose per-event
    /// semantics a per-edge boolean cannot express, while every other composite
    /// lowers to one graph carrying a node test and an edge test — and those
    /// are different types.
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = DynGraphArc<'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = NotOp<T::NodeFilter<'graph, F, T::FilteredGraph<'graph, F>>>
    where
        Self: 'graph;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError>
    where
        Self: 'graph,
    {
        if self.is_exploded_edge_filter() {
            let f = self.0.filter_graph_view(filtered.clone())?;
            let filter = self.0.create_filter(filtered, f)?;
            return Ok(Arc::new(NotFilteredGraph { graph, filter }));
        }
        // Install only the tests the expression speaks about — see `LeafKinds`.
        let kinds = self.leaf_kinds();
        let node_op = kinds
            .nodes
            .then(|| {
                self.clone()
                    .create_node_membership(graph.clone(), filtered.clone(), true)
            })
            .transpose()?;
        let edge_op = kinds
            .edges
            .then(|| self.create_edge_filter(graph.clone(), filtered))
            .transpose()?;
        Ok(Arc::new(EntityOpFilteredGraph::new(
            graph, node_op, edge_op,
        )))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError>
    where
        Self: 'graph,
    {
        let f = self.0.filter_graph_view(filtered.clone())?;
        Ok(self.0.create_node_filter(filtered, f)?.not())
    }

    fn leaf_kinds(&self) -> LeafKinds {
        self.0.leaf_kinds()
    }

    fn create_node_membership<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
        polarity: bool,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError>
    where
        Self: 'graph,
    {
        let f = self.0.filter_graph_view(filtered.clone())?;
        Ok(Arc::new(
            self.0.create_node_membership(filtered, f, !polarity)?.not(),
        ))
    }

    fn is_exploded_edge_filter(&self) -> bool {
        self.0.is_exploded_edge_filter()
    }

    fn is_edge_composite(&self) -> bool {
        // Exploded operands narrow which *events* of an edge survive, which a
        // per-edge boolean cannot express, so those keep their wrapper graphs.
        !self.is_exploded_edge_filter()
    }

    /// Negate the operand's *boolean*, which is the set complement. The wrapper
    /// form negates one hook of a graph whose other hooks still impose the
    /// operand's restrictions, so it is not a complement.
    fn create_edge_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Arc<dyn EdgeFilterOp + 'graph>, GraphError>
    where
        Self: 'graph,
    {
        if self.leaf_kinds().node_only() {
            // One node test, however it is spelled: the edges are those whose
            // endpoints both pass it. Composing the operands at edge level
            // instead would read `~N` as "not (both endpoints pass N)" — every
            // edge that merely touches a failing node — and `N1 | N2` as "both
            // pass N1, or both pass N2", dropping an edge from an N1 node to an
            // N2 node.
            let op = self.create_node_filter(graph.clone(), filtered)?;
            return Ok(Arc::new(EdgeExistsOp::new(NodeFilteredGraph::new(
                graph, op,
            ))));
        }
        let f = self.0.filter_graph_view(filtered.clone())?;
        Ok(Arc::new(self.0.create_edge_filter(filtered, f)?.negate()))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError>
    where
        Self: 'graph,
    {
        Ok(graph)
    }
}

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for NotFilter<T> {
    fn try_as_filter_tree(&self) -> Result<FilterTree, GraphError> {
        // Same-kind combinations keep their composite form; mixed-kind trees
        // export structurally — the case the composite exports cannot
        // represent.
        if let Ok(f) = self.try_as_composite_node_filter() {
            return Ok(FilterTree::Node(f));
        }
        if let Ok(f) = self.try_as_composite_edge_filter() {
            return Ok(FilterTree::Edge(f));
        }
        if let Ok(f) = self.try_as_composite_exploded_edge_filter() {
            return Ok(FilterTree::ExplodedEdge(f));
        }
        Ok(FilterTree::Not(Box::new(self.0.try_as_filter_tree()?)))
    }

    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Not(Box::new(
            self.0.try_as_composite_node_filter()?,
        )))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Not(Box::new(
            self.0.try_as_composite_edge_filter()?,
        )))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::Not(Box::new(
            self.0.try_as_composite_exploded_edge_filter()?,
        )))
    }
}
