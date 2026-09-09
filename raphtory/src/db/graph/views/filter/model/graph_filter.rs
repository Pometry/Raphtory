use crate::{
    db::{
        api::{
            state::ops::filter::NodeExistsOp,
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter, ComposableFilter, CompositeExplodedEdgeFilter,
                CompositeNodeFilter, FilterTree, GraphViewOp, TryAsCompositeFilter, Wrap,
            },
            resolved_view::{read_view, ViewBounds},
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::{LayerOps, TimeOps},
};
use raphtory_api::core::{entities::Layer, storage::timeindex::AsTime, utils::time::IntoTime};
use std::{fmt, sync::Arc};

/// Entry point for graph-level view filters: `GraphFilter.window(0, 5)`.
///
/// Empty on its own — each method returns the [`ViewFilter`] that carries the
/// chain, the same way `NodeFilter` and `EdgeFilter` open their own families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphFilter;

impl GraphFilter {
    /// Events in `[start, end)`.
    pub fn window<S: IntoTime, E: IntoTime>(self, start: S, end: E) -> ViewFilter {
        ViewFilter::from(self).window(start, end)
    }

    /// Events at `time`.
    pub fn at<T: IntoTime>(self, time: T) -> ViewFilter {
        ViewFilter::from(self).at(time)
    }

    /// Events strictly after `time`.
    pub fn after<T: IntoTime>(self, time: T) -> ViewFilter {
        ViewFilter::from(self).after(time)
    }

    /// Events strictly before `time`.
    pub fn before<T: IntoTime>(self, time: T) -> ViewFilter {
        ViewFilter::from(self).before(time)
    }

    /// Only the latest events.
    pub fn latest(self) -> ViewFilter {
        ViewFilter::from(self).latest()
    }

    /// Everything not deleted at `time`.
    pub fn snapshot_at<T: IntoTime>(self, time: T) -> ViewFilter {
        ViewFilter::from(self).snapshot_at(time)
    }

    /// Everything not deleted at the latest time.
    pub fn snapshot_latest(self) -> ViewFilter {
        ViewFilter::from(self).snapshot_latest()
    }

    /// Only the given layers.
    pub fn layer<L: Into<Layer>>(self, layers: L) -> ViewFilter {
        ViewFilter::from(self).layer(layers)
    }
}

/// A graph-level view as a filter: the restrictions to apply to the graph, in
/// order. `GraphFilter.window(0, 5).layer("x")` is the chain
/// `[Window, Layers]`, and applying it is exactly `g.window(0, 5).layer("x")`.
///
/// This is the one filter that restricts the *result*. The same window on a
/// predicate (`Node.window(0, 5).property("p")`) is a [`Windowed`] wrapper
/// instead, and stays the private scope of that predicate.
///
/// [`Windowed`]: crate::db::graph::views::filter::model::windowed_filter::Windowed
#[derive(Debug, Clone, PartialEq)]
pub struct ViewFilter {
    ops: Vec<GraphViewOp>,
}

impl From<GraphFilter> for ViewFilter {
    fn from(_: GraphFilter) -> Self {
        Self { ops: Vec::new() }
    }
}

impl ViewFilter {
    fn then(mut self, op: GraphViewOp) -> Self {
        self.ops.push(op);
        self
    }

    /// Events in `[start, end)`.
    pub fn window<S: IntoTime, E: IntoTime>(self, start: S, end: E) -> Self {
        self.then(GraphViewOp::Window {
            start: start.into_time(),
            end: end.into_time(),
        })
    }

    /// Events at `time`.
    pub fn at<T: IntoTime>(self, time: T) -> Self {
        self.then(GraphViewOp::At(time.into_time()))
    }

    /// Events strictly after `time`.
    ///
    /// Kept as its own op rather than a window ending at the largest
    /// timestamp: an open end is unbounded, where a window ending at
    /// `i64::MAX` leaves a sliver of time beyond it that a negation would
    /// pick up — and on a persistent graph that sliver admits every edge
    /// still alive at the end of time.
    pub fn after<T: IntoTime>(self, time: T) -> Self {
        self.then(GraphViewOp::After(time.into_time()))
    }

    /// Events strictly before `time`.
    pub fn before<T: IntoTime>(self, time: T) -> Self {
        self.then(GraphViewOp::Before(time.into_time()))
    }

    /// Only the latest events.
    pub fn latest(self) -> Self {
        self.then(GraphViewOp::Latest)
    }

    /// Everything not deleted at `time`.
    pub fn snapshot_at<T: IntoTime>(self, time: T) -> Self {
        self.then(GraphViewOp::SnapshotAt(time.into_time()))
    }

    /// Everything not deleted at the latest time.
    pub fn snapshot_latest(self) -> Self {
        self.then(GraphViewOp::SnapshotLatest)
    }

    /// Only the given layers.
    pub fn layer<L: Into<Layer>>(self, layers: L) -> Self {
        self.then(GraphViewOp::Layers(layers.into()))
    }

    /// Apply the chain to `graph`, first op first.
    pub fn apply<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        let mut graph: DynGraphArc<'graph> = Arc::new(graph);
        for op in &self.ops {
            graph = match op {
                GraphViewOp::Window { start, end } => Arc::new(graph.window(start.t(), end.t())),
                GraphViewOp::At(time) => Arc::new(graph.at(time.t())),
                GraphViewOp::Before(time) => Arc::new(graph.before(time.t())),
                GraphViewOp::After(time) => Arc::new(graph.after(time.t())),
                GraphViewOp::Latest => Arc::new(graph.latest()),
                GraphViewOp::SnapshotAt(time) => Arc::new(graph.snapshot_at(time.t())),
                GraphViewOp::SnapshotLatest => Arc::new(graph.snapshot_latest()),
                GraphViewOp::Layers(layers) => Arc::new(graph.layers(layers.clone())?),
            };
        }
        Ok(graph)
    }
}

impl fmt::Display for ViewFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for op in self.ops.iter().rev() {
            match op {
                GraphViewOp::Window { start, end } => {
                    write!(f, "WINDOW[{}..{}](", start.t(), end.t())?
                }
                GraphViewOp::At(time) => write!(f, "AT[{}](", time.t())?,
                GraphViewOp::Before(time) => write!(f, "BEFORE[{}](", time.t())?,
                GraphViewOp::After(time) => write!(f, "AFTER[{}](", time.t())?,
                GraphViewOp::Latest => write!(f, "LATEST(")?,
                GraphViewOp::SnapshotAt(time) => write!(f, "SNAPSHOT_AT[{time}](")?,
                GraphViewOp::SnapshotLatest => write!(f, "SNAPSHOT_LATEST(")?,
                GraphViewOp::Layers(layers) => write!(f, "LAYER[{layers:?}](")?,
            }
        }
        write!(f, "GRAPH")?;
        for _ in &self.ops {
            write!(f, ")")?;
        }
        Ok(())
    }
}

impl Wrap for ViewFilter {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl ComposableFilter for ViewFilter {}

impl CreateFilter for ViewFilter {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = F;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NodeExistsOp<F>;

    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

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
        self.apply(graph)
    }

    fn view_bounds<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<ViewBounds, GraphError> {
        let applied = self.apply(graph.clone())?;
        Ok(ViewBounds::ViewOnly(read_view(&applied)))
    }
}

impl TryAsCompositeFilter for ViewFilter {
    fn try_as_filter_tree(&self) -> Result<FilterTree, GraphError> {
        // The wire form lists the chain outermost-first: last applied first.
        Ok(FilterTree::View(self.ops.iter().rev().cloned().collect()))
    }

    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}
