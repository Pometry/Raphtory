use crate::{
    db::graph::views::filter::model::tree::{
        Entity, Expr, FilterExpr, Scope, Structural, Target, ViewOp,
    },
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            node_expr::{PyExpr, PyPropertyExpr},
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods};
use raphtory_api::core::storage::timeindex::EventTime;

/// An exploded-edge filter scoped to a view.
///
/// An exploded edge is one temporal event of an edge, addressed individually
/// rather than as the edge aggregated across time. Obtained from the view
/// methods on [`ExplodedEdge`]; its property and structural predicates evaluate
/// within that view, and its own view methods narrow it further.
#[pyclass(frozen, name = "ExplodedEdgeFilter", module = "raphtory.filter")]
pub struct PyExplodedEdgeFilter(pub(crate) Scope);

impl PyExplodedEdgeFilter {
    pub(crate) fn root() -> Self {
        PyExplodedEdgeFilter(Scope::new(Entity::ExplodedEdge))
    }

    fn with_view(&self, view: ViewOp) -> Self {
        PyExplodedEdgeFilter(self.0.clone().with_view(view))
    }

    fn read(&self, target: Target) -> Expr {
        Expr::Read {
            scope: self.0.clone(),
            target,
        }
    }
}

#[pymethods]
impl PyExplodedEdgeFilter {
    #[new]
    fn new() -> PyExplodedEdgeFilter {
        Self::root()
    }

    /// Filters an exploded edge property by name.
    ///
    /// The property may be static or temporal depending on the query context.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    fn property(&self, name: String) -> PyPropertyExpr {
        PyPropertyExpr(self.read(Target::Property(name)))
    }

    /// Filters an exploded edge metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of an exploded edge.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    fn metadata(&self, name: String) -> PyExpr {
        PyExpr(self.read(Target::Metadata(name)))
    }

    /// Restricts exploded edge evaluation to the given time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (int): Start time.
    ///     end (int): End time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn window(&self, start: EventTime, end: EventTime) -> PyExplodedEdgeFilter {
        self.with_view(ViewOp::Window { start, end })
    }

    /// Restricts exploded edge evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn at(&self, time: EventTime) -> PyExplodedEdgeFilter {
        self.with_view(ViewOp::At(time))
    }

    /// Restricts exploded edge evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn after(&self, time: EventTime) -> PyExplodedEdgeFilter {
        self.with_view(ViewOp::After(time))
    }

    /// Restricts exploded edge evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn before(&self, time: EventTime) -> PyExplodedEdgeFilter {
        self.with_view(ViewOp::Before(time))
    }

    /// Evaluates exploded edge predicates against the latest available state.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn latest(&self) -> PyExplodedEdgeFilter {
        self.with_view(ViewOp::Latest)
    }

    /// Evaluates exploded edge predicates against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn snapshot_at(&self, time: EventTime) -> PyExplodedEdgeFilter {
        self.with_view(ViewOp::SnapshotAt(time))
    }

    /// Evaluates exploded edge predicates against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn snapshot_latest(&self) -> PyExplodedEdgeFilter {
        self.with_view(ViewOp::SnapshotLatest)
    }

    /// Restricts evaluation to exploded edges belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn layer(&self, layer: String) -> PyExplodedEdgeFilter {
        self.with_view(ViewOp::Layers(vec![layer]))
    }

    /// Restricts evaluation to exploded edges belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn layers(&self, layers: FromIterable<String>) -> PyExplodedEdgeFilter {
        self.with_view(ViewOp::Layers(layers.into()))
    }

    /// Matches exploded edges that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_active(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Structural {
            scope: self.0.clone(),
            pred: Structural::IsActive,
        })
    }

    /// Matches exploded edges that are structurally valid in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_valid(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Structural {
            scope: self.0.clone(),
            pred: Structural::IsValid,
        })
    }

    /// Matches exploded edges that have been deleted.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_deleted(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Structural {
            scope: self.0.clone(),
            pred: Structural::IsDeleted,
        })
    }

    /// Matches exploded edges that are self-loops (source == destination).
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_self_loop(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Structural {
            scope: self.0.clone(),
            pred: Structural::IsSelfLoop,
        })
    }
}

/// Entry point for constructing exploded-edge filter expressions.
///
/// Every method is static; the view methods return an
/// [`ExplodedEdgeFilter`] scoped to that view for further chaining.
#[pyclass(frozen, name = "ExplodedEdge", module = "raphtory.filter")]
pub struct PyExplodedEdge;

#[pymethods]
impl PyExplodedEdge {
    /// Filters an exploded edge property by name.
    ///
    /// The property may be static or temporal depending on the query context.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    #[staticmethod]
    fn property(name: String) -> PyPropertyExpr {
        PyExplodedEdgeFilter::root().property(name)
    }

    /// Filters an exploded edge metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of an exploded edge.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn metadata(name: String) -> PyExpr {
        PyExplodedEdgeFilter::root().metadata(name)
    }

    /// Restricts exploded edge evaluation to the given time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (int): Start time.
    ///     end (int): End time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn window(start: EventTime, end: EventTime) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().window(start, end)
    }

    /// Restricts exploded edge evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn at(time: EventTime) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().at(time)
    }

    /// Restricts exploded edge evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn after(time: EventTime) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().after(time)
    }

    /// Restricts exploded edge evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn before(time: EventTime) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().before(time)
    }

    /// Evaluates exploded edge predicates against the latest available state.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn latest() -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().latest()
    }

    /// Evaluates exploded edge predicates against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().snapshot_at(time)
    }

    /// Evaluates exploded edge predicates against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn snapshot_latest() -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().snapshot_latest()
    }

    /// Restricts evaluation to exploded edges belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn layer(layer: String) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().layer(layer)
    }

    /// Restricts evaluation to exploded edges belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().layers(layers)
    }

    /// Matches exploded edges that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    #[staticmethod]
    fn is_active() -> PyFilterExpr {
        PyExplodedEdgeFilter::root().is_active()
    }

    /// Matches exploded edges that are structurally valid in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    #[staticmethod]
    fn is_valid() -> PyFilterExpr {
        PyExplodedEdgeFilter::root().is_valid()
    }

    /// Matches exploded edges that have been deleted.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    #[staticmethod]
    fn is_deleted() -> PyFilterExpr {
        PyExplodedEdgeFilter::root().is_deleted()
    }

    /// Matches exploded edges that are self-loops (source == destination).
    ///
    /// Returns:
    ///     filter.FilterExpr:
    #[staticmethod]
    fn is_self_loop() -> PyFilterExpr {
        PyExplodedEdgeFilter::root().is_self_loop()
    }
}
