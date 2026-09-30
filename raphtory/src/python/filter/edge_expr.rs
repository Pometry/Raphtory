use crate::{
    db::graph::views::filter::model::{
        edge_filter::Endpoint,
        expr::{EdgeLeaf, Expr, Field, Leaf, NodeExpr, NodeLeaf, ViewOp},
    },
    python::{
        filter::{
            node_expr::{PyExpr, PyPropertyExpr, Typed},
            repr,
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods, PyResult, Python};
use raphtory_api::core::storage::timeindex::EventTime;

/// Entry point for filtering an edge endpoint (source or destination).
///
/// An `EdgeEndpoint` is obtained from `Edge.src()` or `Edge.dst()` and allows
/// you to filter on endpoint fields (id, name, type) as well as endpoint
/// properties and metadata.
///
/// Examples:
///     Edge.src().id() == 1
///     Edge.dst().name().starts_with("user:")
///     Edge.src().property("country") == "UK"
#[pyclass(frozen, name = "EdgeEndpoint", module = "raphtory.filter")]
pub struct PyEdgeEndpoint {
    views: Vec<ViewOp>,
    endpoint: Endpoint,
}

impl PyEdgeEndpoint {
    /// A node expression evaluated on the node at this end of the edge. The
    /// edge's views scope that node term.
    fn through(&self, inner: NodeExpr) -> EdgeLeaf {
        match self.endpoint {
            Endpoint::Src => EdgeLeaf::Src(Box::new(inner)),
            Endpoint::Dst => EdgeLeaf::Dst(Box::new(inner)),
        }
    }

    fn term(&self, leaf: NodeLeaf) -> PyExpr {
        PyExpr(Typed::Edge(Expr::Term(self.through(Expr::Term(leaf)))))
    }

    fn field(&self, field: Field) -> PyExpr {
        self.term(NodeLeaf::Field {
            views: self.views.clone(),
            field,
        })
    }

    fn property_term(&self, name: String) -> PyPropertyExpr {
        PyPropertyExpr::new(|temporal| {
            Typed::Edge(Expr::Term(self.through(Expr::Term(NodeLeaf::property(
                self.views.clone(),
                name.clone(),
                temporal,
            )))))
        })
    }
}

#[pymethods]
impl PyEdgeEndpoint {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let end = match self.endpoint {
            Endpoint::Src => "src",
            Endpoint::Dst => "dst",
        };
        Ok(format!(
            "{}.{end}()",
            repr::factory(py, "Edge", &self.views)?
        ))
    }

    /// Selects the endpoint node ID field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn id(&self) -> PyExpr {
        self.field(Field::Id)
    }

    /// Selects the endpoint node name field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn name(&self) -> PyExpr {
        self.field(Field::Name)
    }

    /// Selects the endpoint node type field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn node_type(&self) -> PyExpr {
        self.field(Field::NodeType)
    }

    /// Filters an endpoint node property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    fn property(&self, name: String) -> PyPropertyExpr {
        self.property_term(name)
    }

    /// Filters an endpoint node metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    fn metadata(&self, name: String) -> PyExpr {
        self.term(NodeLeaf::metadata(self.views.clone(), name))
    }
}

impl PyEdgeFilter {
    pub(crate) fn root() -> Self {
        PyEdgeFilter(Vec::new())
    }

    fn with_view(&self, view: ViewOp) -> Self {
        let mut views = self.0.clone();
        views.push(view);
        PyEdgeFilter(views)
    }

    fn term(&self, leaf: EdgeLeaf) -> PyExpr {
        PyExpr(Typed::Edge(Expr::Term(leaf)))
    }

    fn property_term(&self, name: String) -> PyPropertyExpr {
        PyPropertyExpr::new(|temporal| {
            Typed::Edge(Expr::Term(EdgeLeaf::property(
                self.0.clone(),
                name.clone(),
                temporal,
            )))
        })
    }
}

/// An edge filter scoped to a view.
///
/// Obtained from the view methods on `Edge` (`Edge.window(...)`,
/// `Edge.layer(...)`, ...); its endpoint, property and structural predicates
/// evaluate within that view, and its own view methods narrow it further.
#[pyclass(frozen, name = "EdgeFilter", module = "raphtory.filter")]
pub struct PyEdgeFilter(pub(crate) Vec<ViewOp>);

#[pymethods]
impl PyEdgeFilter {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        repr::factory(py, "Edge", &self.0)
    }

    /// Selects the edge **source endpoint** for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpoint:
    fn src(&self) -> PyEdgeEndpoint {
        PyEdgeEndpoint {
            views: self.0.clone(),
            endpoint: Endpoint::Src,
        }
    }

    /// Selects the edge **destination endpoint** for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpoint:
    fn dst(&self) -> PyEdgeEndpoint {
        PyEdgeEndpoint {
            views: self.0.clone(),
            endpoint: Endpoint::Dst,
        }
    }

    /// Filters an edge property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    fn property(&self, name: String) -> PyPropertyExpr {
        self.property_term(name)
    }

    /// Filters an edge metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    fn metadata(&self, name: String) -> PyExpr {
        self.term(EdgeLeaf::metadata(self.0.clone(), name))
    }

    /// Restricts edge evaluation to the given time window.
    ///
    /// Arguments:
    ///     start (TimeInput): Start time.
    ///     end (TimeInput): End time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn window(&self, start: EventTime, end: EventTime) -> PyEdgeFilter {
        self.with_view(ViewOp::Window { start, end })
    }

    /// Restricts edge evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (TimeInput): Event time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn at(&self, time: EventTime) -> PyEdgeFilter {
        self.with_view(ViewOp::At(time))
    }

    /// Restricts edge evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Lower time bound.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn after(&self, time: EventTime) -> PyEdgeFilter {
        self.with_view(ViewOp::After(time))
    }

    /// Restricts edge evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Upper time bound.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn before(&self, time: EventTime) -> PyEdgeFilter {
        self.with_view(ViewOp::Before(time))
    }

    /// Evaluates edge predicates against the latest available edge state.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn latest(&self) -> PyEdgeFilter {
        self.with_view(ViewOp::Latest)
    }

    /// Evaluates edge predicates against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Snapshot time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn snapshot_at(&self, time: EventTime) -> PyEdgeFilter {
        self.with_view(ViewOp::SnapshotAt(time))
    }

    /// Evaluates edge predicates against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn snapshot_latest(&self) -> PyEdgeFilter {
        self.with_view(ViewOp::SnapshotLatest)
    }

    /// Restricts evaluation to edges belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn layer(&self, layer: String) -> PyEdgeFilter {
        self.with_view(ViewOp::Layers(vec![layer]))
    }

    /// Restricts evaluation to edges belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn layers(&self, layers: FromIterable<String>) -> PyEdgeFilter {
        self.with_view(ViewOp::Layers(layers.into()))
    }

    /// Matches edges that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_active(&self) -> PyExpr {
        self.term(EdgeLeaf::IsActive {
            views: self.0.clone(),
        })
    }

    /// Matches edges that are structurally valid in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_valid(&self) -> PyExpr {
        self.term(EdgeLeaf::IsValid {
            views: self.0.clone(),
        })
    }

    /// Matches edges that have been deleted.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_deleted(&self) -> PyExpr {
        self.term(EdgeLeaf::IsDeleted {
            views: self.0.clone(),
        })
    }

    /// Matches edges that are self-loops (source == destination).
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_self_loop(&self) -> PyExpr {
        self.term(EdgeLeaf::IsSelfLoop {
            views: self.0.clone(),
        })
    }
}

/// Entry point for constructing edge filter expressions.
///
/// Every method is static: `Edge.src().name() == "alice"` selects edges
/// directly, and the view methods return an `EdgeFilter` scoped to that
/// view for further chaining.
#[pyclass(frozen, name = "Edge", module = "raphtory.filter")]
pub struct PyEdge;

#[pymethods]
impl PyEdge {
    /// Selects the edge **source endpoint** for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpoint:
    #[staticmethod]
    fn src() -> PyEdgeEndpoint {
        PyEdgeFilter::root().src()
    }

    /// Selects the edge **destination endpoint** for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpoint:
    #[staticmethod]
    fn dst() -> PyEdgeEndpoint {
        PyEdgeFilter::root().dst()
    }

    /// Filters an edge property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    #[staticmethod]
    fn property(name: String) -> PyPropertyExpr {
        PyEdgeFilter::root().property(name)
    }

    /// Filters an edge metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn metadata(name: String) -> PyExpr {
        PyEdgeFilter::root().metadata(name)
    }

    /// Restricts edge evaluation to the given time window.
    ///
    /// Arguments:
    ///     start (TimeInput): Start time.
    ///     end (TimeInput): End time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn window(start: EventTime, end: EventTime) -> PyEdgeFilter {
        PyEdgeFilter::root().window(start, end)
    }

    /// Restricts edge evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (TimeInput): Event time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn at(time: EventTime) -> PyEdgeFilter {
        PyEdgeFilter::root().at(time)
    }

    /// Restricts edge evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Lower time bound.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn after(time: EventTime) -> PyEdgeFilter {
        PyEdgeFilter::root().after(time)
    }

    /// Restricts edge evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Upper time bound.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn before(time: EventTime) -> PyEdgeFilter {
        PyEdgeFilter::root().before(time)
    }

    /// Evaluates edge predicates against the latest available edge state.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn latest() -> PyEdgeFilter {
        PyEdgeFilter::root().latest()
    }

    /// Evaluates edge predicates against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Snapshot time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyEdgeFilter {
        PyEdgeFilter::root().snapshot_at(time)
    }

    /// Evaluates edge predicates against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn snapshot_latest() -> PyEdgeFilter {
        PyEdgeFilter::root().snapshot_latest()
    }

    /// Restricts evaluation to edges belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn layer(layer: String) -> PyEdgeFilter {
        PyEdgeFilter::root().layer(layer)
    }

    /// Restricts evaluation to edges belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyEdgeFilter {
        PyEdgeFilter::root().layers(layers)
    }

    /// Matches edges that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn is_active() -> PyExpr {
        PyEdgeFilter::root().is_active()
    }

    /// Matches edges that are structurally valid in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn is_valid() -> PyExpr {
        PyEdgeFilter::root().is_valid()
    }

    /// Matches edges that have been deleted.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn is_deleted() -> PyExpr {
        PyEdgeFilter::root().is_deleted()
    }

    /// Matches edges that are self-loops (source == destination).
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn is_self_loop() -> PyExpr {
        PyEdgeFilter::root().is_self_loop()
    }
}
