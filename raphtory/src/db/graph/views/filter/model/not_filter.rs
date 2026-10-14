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
            not_filtered_graph::NotFilteredGraph,
            resolved_view::ViewBounds,
            CreateFilter,
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

impl<T: CreateFilter> CreateFilter for NotFilter<T> {
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

    // The result is what the negation resolves to: the complement of a views_only
    // view, which is the whole answer, or the hull of a mixed expression,
    // within which the inner expression is built over `graph` and negated.
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
        let complement = bounds.apply(graph.clone())?;
        if views_only {
            return Ok(complement);
        }
        let inner_scope = self.0.filter_graph_view(graph.clone())?;
        let inner = self.0.create_filter(graph, inner_scope)?;
        Ok(Arc::new(NotFilteredGraph::new(complement, inner)))
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
        let inner_scope = self.0.filter_graph_view(graph.clone())?;
        Ok(Arc::new(
            self.0.create_node_filter(graph, inner_scope)?.not(),
        ))
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
        self.0.view_bounds(graph.clone())?.not(graph)
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
