use crate::{
    db::{
        api::{
            state::ops::{filter::NodeExistsOp, NodeFilterOp, NodeOp},
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            and_filtered_graph::AndFilteredGraph,
            model::{
                edge_filter::CompositeEdgeFilter,
                exploded_edge_filter::CompositeExplodedEdgeFilter,
                node_filter::CompositeNodeFilter, ComposableFilter, FilterTree,
                TryAsCompositeFilter,
            },
            resolved_view::ViewBounds,
            CreateFilter,
        },
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AndFilter<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: Display, R: Display> Display for AndFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} AND {})", self.left, self.right)
    }
}

impl<L, R> ComposableFilter for AndFilter<L, R> {}

impl<L: CreateFilter, R: CreateFilter> CreateFilter for AndFilter<L, R> {
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

    // The result is the composed view (left applied, then right), and every
    // operand is evaluated on it: `filter(V & P)` is `g.view(V).filter(P)`.
    // A conjunction of views alone is that view, with nothing wrapped around
    // it.
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
        let composed = bounds.apply(graph)?;
        if views_only {
            return Ok(composed);
        }
        let left_scope = self.left.filter_graph_view(composed.clone())?;
        let right_scope = self.right.filter_graph_view(composed.clone())?;
        let left = self.left.create_filter(composed.clone(), left_scope)?;
        let right = self.right.create_filter(composed.clone(), right_scope)?;
        Ok(Arc::new(AndFilteredGraph::new(composed, left, right)))
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
        let views_only = bounds.is_view_only();
        let composed = bounds.apply(graph)?;
        if views_only {
            return Ok(Arc::new(NodeExistsOp::new(composed)));
        }
        let left_scope = self.left.filter_graph_view(composed.clone())?;
        let right_scope = self.right.filter_graph_view(composed.clone())?;
        let left = self.left.create_node_filter(composed.clone(), left_scope)?;
        let right = self.right.create_node_filter(composed, right_scope)?;
        Ok(Arc::new(left.and(right)))
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

    /// Left, then right: `latest()` and `snapshot_at` resolve against the
    /// graph they are applied to, so the order is part of the meaning.
    fn view_bounds<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<ViewBounds, GraphError> {
        let left = self.left.view_bounds(graph.clone())?;
        let after_left = left.clone().apply(graph.clone())?;
        let right = self.right.view_bounds(after_left)?;
        Ok(ViewBounds::and(&left, &right, &graph))
    }
}

impl<L: TryAsCompositeFilter, R: TryAsCompositeFilter> TryAsCompositeFilter for AndFilter<L, R> {
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
        Ok(FilterTree::And(vec![
            self.left.try_as_filter_tree()?,
            self.right.try_as_filter_tree()?,
        ]))
    }

    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::And(
            Box::new(self.left.try_as_composite_node_filter()?),
            Box::new(self.right.try_as_composite_node_filter()?),
        ))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::And(
            Box::new(self.left.try_as_composite_edge_filter()?),
            Box::new(self.right.try_as_composite_edge_filter()?),
        ))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::And(
            Box::new(self.left.try_as_composite_exploded_edge_filter()?),
            Box::new(self.right.try_as_composite_exploded_edge_filter()?),
        ))
    }
}
