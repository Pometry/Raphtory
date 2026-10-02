use crate::{
    db::{
        api::{
            state::ops::{filter::NodeExistsOp, NodeFilterOp},
            view::internal::{DynGraphArc, FilterOps, GraphView},
        },
        graph::views::filter::{
            node_filtered_graph::NodeFilteredGraph,
            resolved_view::{read_view, ViewBounds},
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::{
    entities::{LayerId, ELID},
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::{edges::edge_ref::EdgeEntryRef, nodes::node_ref::NodeStorageRef};

pub mod and_filtered_graph;
pub mod edge_filtered_graph;
pub mod edge_node_filtered_graph;
pub mod edge_property_filtered_graph;
pub mod exploded_edge_filtered_graph;
pub mod exploded_edge_node_filtered_graph;
pub mod exploded_edge_property_filter;
pub mod model;
pub mod node_filtered_graph;
pub mod not_filtered_graph;
pub mod or_filtered_graph;
pub mod resolved_view;

/// Whether `view` restricts entities of every kind, rather than only the kind
/// its predicates test. A window or a layer set does: both live in the view's
/// time semantics and layer ids, not in its per-kind `internal_*` hooks, so a
/// combinator asking only those hooks would treat such an operand as inert.
pub(crate) fn restricts_every_kind<G: GraphView>(view: &G) -> bool {
    view.window_filtered() || !view.layer_ids().is_all()
}

/// Whether an operand of a combinator admits an entity.
///
/// The per-kind `internal_*` hooks are the operand's own predicate tests, and
/// asking them alone is what a combinator wants: a union of a node filter and
/// an edge filter has to let a node become visible through a visible edge,
/// which each operand's *composed* filter — a whole-graph question — would
/// deny. But an operand whose restriction is a view has nothing in those
/// hooks, so the composed filter is consulted as well, and only then.
pub(crate) fn admits_node<G: GraphView>(view: &G, node: NodeStorageRef) -> bool {
    view.internal_filter_node(node, view.layer_ids())
        && (!restricts_every_kind(view) || view.filter_node(node))
}

pub(crate) fn admits_edge<G: GraphView>(view: &G, edge: EdgeEntryRef) -> bool {
    view.internal_filter_edge(edge, view.layer_ids())
        && (!restricts_every_kind(view) || view.filter_edge(edge))
}

pub(crate) fn admits_edge_layer<G: GraphView>(
    view: &G,
    edge: EdgeEntryRef,
    layer: LayerId,
) -> bool {
    view.internal_filter_edge_layer(edge, layer)
        && (!restricts_every_kind(view) || view.filter_edge_layer(edge, layer))
}

pub(crate) fn admits_exploded_edge<G: GraphView>(view: &G, eid: ELID, t: EventTime) -> bool {
    view.internal_filter_exploded_edge(eid, t, view.layer_ids())
        && (!restricts_every_kind(view) || view.filter_exploded_edge(eid, t))
}

pub struct Exists;

impl CreateFilter for Exists {
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

    fn view_bounds<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<ViewBounds, GraphError> {
        Ok(ViewBounds::ViewOnly(read_view(&graph)))
    }
}

/// How a filter expression is lowered onto a graph.
///
/// An expression restricts a graph in two distinct ways. A *predicate* tests
/// entities, reading their properties and history from a graph: for
/// `Node.property("p") == 1` that is the graph it is applied to, and for
/// `Node.window(0, 3).property("p") == 1` it is that graph windowed — the
/// window is the predicate's private scope and restricts nothing else.
/// `filter_graph_view(graph)` is that scope, built innermost-first so it
/// matches the chain that built the expression, and `create_filter(graph,
/// filtered)` receives it as `filtered` alongside the graph the result is
/// built over. Callers pass `filtered = filter_graph_view(graph)`.
///
/// A *view* (`Graph.window`, `Graph.layer`, …) restricts the result itself:
/// what it contains and the time semantics it carries. `view_bounds` says
/// what an expression restricts the result to, and `result_view` applies it;
/// the combinators use it to give `&`, `|` and `~` of views one graph with
/// the right time semantics instead of a stack of wrappers.
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

    /// What this expression restricts `graph` to, and what its negation does,
    /// so `or` and `not` can be resolved to one view. A predicate restricts
    /// nothing, which is the default; views and combinators override it.
    fn view_bounds<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<ViewBounds, GraphError> {
        let _ = graph;
        Ok(ViewBounds::predicate())
    }

    /// `graph` restricted to what this expression restricts the result to:
    /// one `WindowedGraph`, `MultiWindowedGraph` or `LayeredGraph` — or
    /// `graph` itself for a predicate.
    fn result_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        self.view_bounds(graph.clone())?.apply(graph)
    }
}

impl<T: NodeFilterOp> CreateFilter for T {
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
