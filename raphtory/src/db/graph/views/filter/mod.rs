use crate::{
    db::{
        api::{
            state::ops::{filter::NodeExistsOp, node::NodeOp, NodeFilterOp},
            view::internal::GraphView,
        },
        graph::views::filter::node_filtered_graph::NodeFilteredGraph,
    },
    errors::GraphError,
    prelude::GraphViewOps,
};

pub mod and_filtered_graph;
pub mod edge_filtered_graph;
pub mod edge_node_filtered_graph;
pub mod edge_op;
pub mod edge_property_filtered_graph;
pub mod entity_op_filtered_graph;
pub mod exploded_edge_filtered_graph;
pub mod exploded_edge_node_filtered_graph;
pub mod exploded_edge_property_filter;
pub mod model;
pub mod node_filtered_graph;
pub mod not_filtered_graph;
pub mod or_filtered_graph;

use crate::db::graph::views::filter::edge_op::EdgeFilterOp;
use std::sync::Arc;

pub struct Exists;

impl CreateFilter for Exists {
    crate::leaf_filter_lowering!(LeafKinds::VIEW);

    type EntityFiltered<'graph, G, F>
        = F
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;
    type NodeFilter<'graph, G, F>
        = NodeExistsOp<F>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        Ok(filtered)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Ok(NodeExistsOp::new(filtered))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

/// Lowers a filter that holds no other filters — a *leaf*.
///
/// Emits the three methods every leaf answers the same way: `leaf_kinds` is the
/// given constant; `create_node_membership` is the leaf's node op when it has
/// one and otherwise the constant the caller asked for; `create_edge_filter`
/// builds the wrapper graph the leaf already produces and asks it whether an
/// edge is in its view.
///
/// Wrong for anything that wraps another filter — a composite, a view applied
/// to an inner expression, or a type that delegates to a boxed filter — because
/// it rebuilds the very wrapper composition the per-edge boolean exists to
/// avoid, and it cannot see the kinds of the leaves inside. Those must delegate
/// to their inner filter instead.
#[macro_export]
macro_rules! leaf_filter_lowering {
    ($kinds:expr) => {
        fn leaf_kinds(&self) -> $crate::db::graph::views::filter::LeafKinds {
            $kinds
        }

        fn create_node_membership<
            'graph,
            G: $crate::db::api::state::ops::GraphView + 'graph,
            F: $crate::db::api::state::ops::GraphView + 'graph,
        >(
            self,
            graph: G,
            filtered: F,
            polarity: bool,
        ) -> Result<
            std::sync::Arc<dyn $crate::db::api::state::ops::node::NodeOp<Output = bool> + 'graph>,
            $crate::errors::GraphError,
        >
        where
            Self: 'graph,
        {
            if $kinds.nodes {
                Ok(std::sync::Arc::new(
                    self.create_node_filter(graph, filtered)?,
                ))
            } else {
                Ok(std::sync::Arc::new($crate::db::api::state::ops::Const(
                    polarity,
                )))
            }
        }

        fn create_edge_filter<
            'graph,
            G: $crate::db::api::state::ops::GraphView + 'graph,
            F: $crate::db::api::state::ops::GraphView + 'graph,
        >(
            self,
            graph: G,
            filtered: F,
        ) -> Result<
            std::sync::Arc<dyn $crate::db::graph::views::filter::edge_op::EdgeFilterOp + 'graph>,
            $crate::errors::GraphError,
        >
        where
            Self: 'graph,
        {
            Ok(std::sync::Arc::new(
                $crate::db::graph::views::filter::edge_op::EdgeExistsOp::new(
                    self.create_filter(graph, filtered)?,
                ),
            ))
        }
    };
}

/// Which entities a filter expression speaks about, read off the kinds of its
/// leaves.
///
/// A node predicate speaks about nodes; an edge predicate (or an exploded-edge
/// one) about edges; a view — a window, a layer, a snapshot — about both, since
/// it restricts which nodes *and* which edges are present. A composite speaks
/// about the union of what its operands speak about.
///
/// This decides how `graph.filter(expr)` lowers. An expression that speaks only
/// about nodes installs a node test and nothing else, so it behaves exactly like
/// a single node filter: edges follow from their endpoints, and walking from a
/// node does not test the node being stood on. One that speaks only about edges
/// installs an edge test and leaves nodes alone. Anything else installs both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeafKinds {
    pub nodes: bool,
    pub edges: bool,
}

impl LeafKinds {
    pub const NONE: Self = Self {
        nodes: false,
        edges: false,
    };
    pub const NODES: Self = Self {
        nodes: true,
        edges: false,
    };
    pub const EDGES: Self = Self {
        nodes: false,
        edges: true,
    };
    /// A view restricts nodes and edges alike.
    pub const VIEW: Self = Self {
        nodes: true,
        edges: true,
    };

    pub const fn union(self, other: Self) -> Self {
        Self {
            nodes: self.nodes || other.nodes,
            edges: self.edges || other.edges,
        }
    }

    /// Speaks about nodes and nothing else — one node test, however it is
    /// spelled.
    pub const fn node_only(self) -> bool {
        self.nodes && !self.edges
    }
}

pub trait CreateFilter: Sized {
    type EntityFiltered<'graph, G, F>: GraphView + 'graph
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type NodeFilter<'graph, G, F>: NodeFilterOp + 'graph
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type FilteredGraph<'graph, G>: GraphView + 'graph
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError>;

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError>;

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError>;

    /// Whether this filter tests individual edge *events* rather than edges.
    ///
    /// Exploded-edge filters do, and composites of them must keep their wrapper
    /// graphs: a per-edge boolean can only say whether an edge is in, not which
    /// of its events survive.
    fn is_exploded_edge_filter(&self) -> bool {
        false
    }

    /// Whether this filter is a boolean composite of other filters.
    ///
    /// Composites must be lowered to a per-edge boolean, because composing their
    /// operands' wrapper graphs loses a view operand's restriction. Everything
    /// else keeps its wrapper graph, which carries exact per-layer and
    /// per-event semantics that a single edge-level boolean cannot express —
    /// an exploded-edge property filter, for instance, narrows which *events*
    /// of an edge survive, not just which edges.
    fn is_edge_composite(&self) -> bool {
        false
    }

    /// Which entities this filter speaks about — see [`LeafKinds`].
    ///
    /// Required, like [`create_edge_filter`](Self::create_edge_filter): a
    /// filter that holds other filters must combine theirs, and a default could
    /// only guess.
    fn leaf_kinds(&self) -> LeafKinds;

    /// Which nodes belong to the graph this filter describes.
    ///
    /// Differs from [`create_node_filter`](Self::create_node_filter) in what it
    /// does with a leaf that says nothing about nodes: an edge predicate. That
    /// method treats one as an error, which is right when the caller wants a
    /// node predicate. This one treats it as *unknown*: a node is excluded only
    /// when the expression is definitely false about it. `and(unknown, N)` is
    /// `N`, `or(unknown, N)` keeps every node, `not(unknown)` is still unknown.
    ///
    /// `polarity` is how an unknown leaf resolves at this point in the tree:
    /// `true` at the root and under `and`/`or`, flipped by each `not`. A leaf
    /// answers `Const(polarity)`; `not` asks its operand with `!polarity` and
    /// negates. That is three-valued logic done with two-valued ops.
    fn create_node_membership<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
        polarity: bool,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError>
    where
        Self: 'graph;

    /// Lower this filter to a per-edge boolean.
    ///
    /// Required rather than defaulted, deliberately. A filter that holds other
    /// filters has to *delegate* this — asking each inner filter for its own
    /// boolean — while a filter that stands alone builds its wrapper graph and
    /// asks whether an edge is in its view ([`leaf_filter_lowering!`]
    /// writes that one out). A default could only be one of the two, and every
    /// type that wanted the other would compile silently and be wrong: the
    /// composition it was supposed to fix would quietly come back. Leaving it
    /// required turns each of those into a compile error instead.
    ///
    /// Boxed rather than an associated type so an implementation is one line
    /// instead of a type definition.
    fn create_edge_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Arc<dyn EdgeFilterOp + 'graph>, GraphError>
    where
        Self: 'graph;
}

impl<T: NodeFilterOp> CreateFilter for T {
    crate::leaf_filter_lowering!(LeafKinds::NODES);

    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = NodeFilteredGraph<G, T>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = Self
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
        _filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError>
    where
        Self: 'graph,
    {
        Ok(NodeFilteredGraph::new(graph, self))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError>
    where
        Self: 'graph,
    {
        Ok(self)
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
