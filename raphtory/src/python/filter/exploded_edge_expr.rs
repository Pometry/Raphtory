use crate::{
    db::graph::views::filter::model::exploded_edge_filter::ExplodedEdgeFilter,
    python::{
        filter::{
            edge_expr::DynEdgeFilterFactory,
            filter_expr::PyFilterExpr,
            node_expr::{PyExpr, PyPropertyExpr},
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods};
use raphtory_api::core::storage::timeindex::EventTime;
use std::sync::Arc;

/// Entry point for constructing **exploded edge** filter expressions.
///
/// An **exploded edge** represents an edge view where temporal events are treated
/// as individually addressable edge instances (i.e. "event-level" edges), rather
/// than a single aggregated edge across time.
///
/// This filter provides:
/// - property and metadata filters,
/// - view restrictions (time windows, snapshots, layers),
/// - and structural predicates over exploded edge state (active/valid/deleted/self-loop).
///
/// Examples:
///     ExplodedEdge.property("weight") > 0.5
///     ExplodedEdge.window(0, 10).is_active()
///     ExplodedEdge.layer("fire_nation").is_valid()
#[pyclass(frozen, name = "ExplodedEdge", module = "raphtory.filter")]
pub struct PyExplodedEdgeFilter(Arc<dyn DynEdgeFilterFactory>);

impl PyExplodedEdgeFilter {
    pub(crate) fn root() -> Self {
        PyExplodedEdgeFilter(Arc::new(ExplodedEdgeFilter))
    }
}

impl From<Arc<dyn DynEdgeFilterFactory>> for PyExplodedEdgeFilter {
    fn from(value: Arc<dyn DynEdgeFilterFactory>) -> Self {
        PyExplodedEdgeFilter(value)
    }
}

#[pymethods]
impl PyExplodedEdgeFilter {
    #[new]
    fn new() -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter(Arc::new(ExplodedEdgeFilter))
    }

    /// Filters an exploded edge property by name.
    ///
    /// The property may be static or temporal depending on the query context.
    ///
    /// Arguments:
    ///     name (str): Property key.
    fn property(&self, name: String) -> PyPropertyExpr {
        self.0.dyn_property(name).into()
    }

    /// Filters an exploded edge metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of an exploded edge.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    fn metadata(&self, name: String) -> PyExpr {
        self.0.dyn_metadata(name).into()
    }

    /// Restricts exploded edge evaluation to the given time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    fn window(&self, start: EventTime, end: EventTime) -> PyExplodedEdgeFilter {
        self.0.dyn_window(start, end).into()
    }

    /// Restricts exploded edge evaluation to a single point in time.
    fn at(&self, time: EventTime) -> PyExplodedEdgeFilter {
        self.0.dyn_at(time).into()
    }

    /// Restricts exploded edge evaluation to times strictly after the given time.
    fn after(&self, time: EventTime) -> PyExplodedEdgeFilter {
        self.0.dyn_after(time).into()
    }

    /// Restricts exploded edge evaluation to times strictly before the given time.
    fn before(&self, time: EventTime) -> PyExplodedEdgeFilter {
        self.0.dyn_before(time).into()
    }

    /// Evaluates exploded edge predicates against the latest available state.
    fn latest(&self) -> PyExplodedEdgeFilter {
        self.0.dyn_latest().into()
    }

    /// Evaluates exploded edge predicates against a snapshot of the graph at a given time.
    fn snapshot_at(&self, time: EventTime) -> PyExplodedEdgeFilter {
        self.0.dyn_snapshot_at(time).into()
    }

    /// Evaluates exploded edge predicates against the most recent snapshot of the graph.
    fn snapshot_latest(&self) -> PyExplodedEdgeFilter {
        self.0.dyn_snapshot_latest().into()
    }

    /// Restricts evaluation to exploded edges belonging to the given layer.
    fn layer(&self, layer: String) -> PyExplodedEdgeFilter {
        self.0.dyn_layer(vec![layer]).into()
    }

    /// Restricts evaluation to exploded edges belonging to any of the given layers.
    fn layers(&self, layers: FromIterable<String>) -> PyExplodedEdgeFilter {
        self.0.dyn_layer(layers.to_vec()).into()
    }

    /// Matches exploded edges that have at least one event in the current view.
    fn is_active(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_active())
    }

    /// Matches exploded edges that are structurally valid in the current view.
    fn is_valid(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_valid())
    }

    /// Matches exploded edges that have been deleted.
    fn is_deleted(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_deleted())
    }

    /// Matches exploded edges that are self-loops (source == destination).
    fn is_self_loop(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_self_loop())
    }
}
