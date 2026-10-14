use crate::{
    db::{
        api::{
            state::ops::{filter::NodeExistsOp, NodeFilterOp, NodeOp},
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter,
                exploded_edge_filter::CompositeExplodedEdgeFilter,
                node_filter::CompositeNodeFilter, ComposableFilter, FilterTree,
                TryAsCompositeFilter,
            },
            or_filtered_graph::OrFilteredGraph,
            resolved_view::ViewBounds,
            CreateFilter,
        },
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrFilter<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: Display, R: Display> Display for OrFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} OR {})", self.left, self.right)
    }
}

impl<L, R> ComposableFilter for OrFilter<L, R> {}

impl<L: CreateFilter, R: CreateFilter> CreateFilter for OrFilter<L, R> {
    // Erased because a resolved view becomes one of `WindowedGraph`,
    // `MultiWindowedGraph`, `LayeredGraph` or the graph itself, and no single
    // associated type names all four; a graph carrying the resolved
    // `TimeSemantics` would, see #2776.
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = DynGraphArc<'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    // The result is the union of the operands' views. Views alone need
    // nothing more; with a predicate present each operand is built over
    // `graph` with its own scope, and membership is either operand's.
    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        _filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError>
    where
        Self: 'graph,
    {
        let bounds = self.view_bounds(graph.clone())?;
        let views_only = bounds.is_view_only();
        let union = bounds.apply(graph.clone())?;
        if views_only {
            return Ok(union);
        }
        let left_scope = self.left.filter_graph_view(graph.clone())?;
        let right_scope = self.right.filter_graph_view(graph.clone())?;
        let left = self.left.create_filter(graph.clone(), left_scope)?;
        let right = self.right.create_filter(graph, right_scope)?;
        Ok(Arc::new(OrFilteredGraph::new(union, left, right)))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError>
    where
        Self: 'graph,
    {
        let bounds = self.view_bounds(graph.clone())?;
        if bounds.is_view_only() {
            return Ok(Arc::new(NodeExistsOp::new(bounds.apply(graph)?)));
        }
        let left_scope = self.left.filter_graph_view(graph.clone())?;
        let right_scope = self.right.filter_graph_view(graph.clone())?;
        let left = self.left.create_node_filter(graph.clone(), left_scope)?;
        let right = self.right.create_node_filter(graph, right_scope)?;
        Ok(Arc::new(left.or(right)))
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

    fn view_bounds<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<ViewBounds, GraphError> {
        let left = self.left.view_bounds(graph.clone())?;
        let right = self.right.view_bounds(graph.clone())?;
        ViewBounds::or(&left, &right, &graph)
    }
}

impl<L: TryAsCompositeFilter, R: TryAsCompositeFilter> TryAsCompositeFilter for OrFilter<L, R> {
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
        Ok(FilterTree::Or(vec![
            self.left.try_as_filter_tree()?,
            self.right.try_as_filter_tree()?,
        ]))
    }

    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Or(
            Box::new(self.left.try_as_composite_node_filter()?),
            Box::new(self.right.try_as_composite_node_filter()?),
        ))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Or(
            Box::new(self.left.try_as_composite_edge_filter()?),
            Box::new(self.right.try_as_composite_edge_filter()?),
        ))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::Or(
            Box::new(self.left.try_as_composite_exploded_edge_filter()?),
            Box::new(self.right.try_as_composite_exploded_edge_filter()?),
        ))
    }
}
