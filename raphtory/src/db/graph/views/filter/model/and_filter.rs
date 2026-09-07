use crate::{
    db::{
        api::{
            state::ops::{filter::AndOp, node::NodeOp, NodeFilterOp},
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            and_filtered_graph::AndFilteredGraph,
            edge_op::{EdgeExistsOp, EdgeFilterOp, EdgeFilterOpExt},
            entity_op_filtered_graph::EntityOpFilteredGraph,
            model::{
                edge_filter::CompositeEdgeFilter,
                exploded_edge_filter::CompositeExplodedEdgeFilter,
                node_filter::CompositeNodeFilter, ComposableFilter, FilterTree,
                TryAsCompositeFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter, LeafKinds,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
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

impl<L: CreateFilter + Clone, R: CreateFilter + Clone> CreateFilter for AndFilter<L, R> {
    /// Boxed: an exploded composite keeps the wrapper graphs, whose per-event
    /// semantics a per-edge boolean cannot express, while every other composite
    /// lowers to one graph carrying a node test and an edge test — and those
    /// are different types.
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = DynGraphArc<'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = AndOp<
        L::NodeFilter<'graph, G, L::FilteredGraph<'graph, F>>,
        R::NodeFilter<'graph, G, R::FilteredGraph<'graph, F>>,
    >
    where
        Self: 'graph;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError>
    where
        Self: 'graph,
    {
        if self.is_exploded_edge_filter() {
            let l = self.left.filter_graph_view(filtered.clone())?;
            let r = self.right.filter_graph_view(filtered)?;
            let left = self.left.create_filter(graph.clone(), l)?;
            let right = self.right.create_filter(graph.clone(), r)?;
            return Ok(Arc::new(AndFilteredGraph::new(graph, left, right)));
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
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError>
    where
        Self: 'graph,
    {
        let l = self.left.filter_graph_view(filtered.clone())?;
        let r = self.right.filter_graph_view(filtered)?;
        let left = self.left.create_node_filter(graph.clone(), l)?;
        let right = self.right.create_node_filter(graph, r)?;
        Ok(left.and(right))
    }

    fn leaf_kinds(&self) -> LeafKinds {
        self.left.leaf_kinds().union(self.right.leaf_kinds())
    }

    fn create_node_membership<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
        polarity: bool,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError>
    where
        Self: 'graph,
    {
        let l = self.left.filter_graph_view(filtered.clone())?;
        let r = self.right.filter_graph_view(filtered)?;
        let left = self
            .left
            .create_node_membership(graph.clone(), l, polarity)?;
        let right = self.right.create_node_membership(graph, r, polarity)?;
        Ok(Arc::new(left.and(right)))
    }

    fn is_exploded_edge_filter(&self) -> bool {
        self.left.is_exploded_edge_filter() || self.right.is_exploded_edge_filter()
    }

    fn is_edge_composite(&self) -> bool {
        // Exploded operands narrow which *events* of an edge survive, which a
        // per-edge boolean cannot express, so those keep their wrapper graphs.
        !self.is_exploded_edge_filter()
    }

    /// Combine the operands' *booleans*, not their wrapper graphs: nesting the
    /// graphs drops a view operand's restriction, because a wrapper takes its
    /// time semantics from the graph it wraps.
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
        let l = self.left.filter_graph_view(filtered.clone())?;
        let r = self.right.filter_graph_view(filtered)?;
        let left = self.left.create_edge_filter(graph.clone(), l)?;
        let right = self.right.create_edge_filter(graph, r)?;
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
