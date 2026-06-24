use crate::{
    db::graph::views::filter::model::{
        degree_filter::DegreeFilterFactory,
        node_expr::{CreateOp},
        node_state_filter::NodeStateBoolColOp,
        NodeViewFilterOps, PropertyFilterFactory, ViewWrapOps,
    },
    prelude::{EntityExprFilterOps, NodeFilter, NodeFilterFactory},
    python::{graph::node_state::PyOutputNodeState, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods, PyResult};
use raphtory_api::core::storage::timeindex::EventTime;
use std::sync::Arc;
use crate::db::graph::views::filter::model::node_expr::DynCreateOp;

#[pyclass(frozen, name = "Expr", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExpr(Arc<dyn DynCreateOp>);

impl<E: CreateOp> From<E> for PyExpr {
    fn from(value: E) -> Self {
        PyExpr(Arc::new(value))
    }
}

#[pymethods]
impl PyExpr {
    fn __eq__(&self, other: &Self) -> Self {
        self.0.eq(&other.0).into()
    }
}

/// Constructs node filter expressions.
///
/// Each method returns either:
/// - a field-specific filter builder, or
/// - a view-restricted filter context, or
/// - a boolean predicate over node state.
#[pyclass(frozen, name = "Node", module = "raphtory.filter")]
pub struct PyNodeFilter(Arc<dyn NodeFilterFactory>);

#[pymethods]
impl PyNodeFilter {
    #[new]
    fn new() -> PyNodeFilter {
        PyNodeFilter(Arc::new(NodeFilter))
    }

    /// Selects the node ID field for filtering.
    ///
    /// Returns:
    ///     filter.NodeIdFilterBuilder:
    fn id(&self) -> PyExpr {
        self.0.id().into()
    }

    /// Selects the node name field for filtering.
    ///
    /// Returns:
    ///     filter.NodeNameFilterBuilder:
    fn name(&self) -> PyExpr {
        self.0.name().into()
    }

    /// Selects the node type field for filtering.
    ///
    /// Returns:
    ///     filter.NodeTypeFilterBuilder:
    fn node_type(&self) -> PyExpr {
        self.0.node_type().into()
    }

    /// Selects incoming node degree for filtering.
    ///
    /// Returns:
    ///     filter.FilterOps
    fn in_degree(&self) -> PyExpr {
        self.0.in_degree().into()
    }

    /// Selects total node degree for filtering.
    ///
    /// Returns:
    ///     filter.FilterOps
    fn degree(&self) -> PyExpr {
        self.0.degree().into()
    }

    /// Selects outgoing node degree for filtering.
    ///
    /// Returns:
    ///     filter.FilterOps
    fn out_degree(&self) -> PyExpr {
        self.0.out_degree().into()
    }

    /// Filters a node property by name.
    ///
    /// The property may be static or temporal depending on the query context.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyFilterOps:
    fn property(&self, name: String) -> PyExpr {
        self.0.property(name).into()
    }

    /// Filters a node metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of a node.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.FilterOps:
    fn metadata(&self, name: String) -> PyExpr {
        self.0.metadata(name).into()
    }

    /// Restricts node evaluation to the given time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (int): Start time.
    ///     end (int): End time.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn window(&self, start: EventTime, end: EventTime) -> PyNodeFilter {
        self.0.window(start, end).into()
    }

    /// Restricts node evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn at(&self, time: EventTime) -> PyNodeFilter {
        self.0.at(time).into()
    }

    /// Restricts node evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn after(&self, time: EventTime) -> PyNodeFilter {
        self.0.after(time).into()
    }

    /// Restricts node evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn before(&self, time: EventTime) -> PyNodeFilter {
        self.0.before(time).into()
    }

    /// Evaluates filters against the latest available state of each node.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn latest(&self) -> PyNodeFilter {
        self.0.latest().into()
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn snapshot_at(&self, time: EventTime) -> PyNodeFilter {
        self.0.snapshot_at(time).into()
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn snapshot_latest(&self) -> PyNodeFilter {
        self.0.snapshot_latest().into()
    }

    /// Restricts evaluation to nodes belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn layer(&self, layer: String) -> PyNodeFilter {
        self.0.layer(layer).into()
    }

    /// Restricts evaluation to nodes belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn layers(&self, layers: FromIterable<String>) -> PyNodeFilter {
        self.0.layer(layers).into()
    }

    /// Matches nodes that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_active(&self) -> PyNodeFilter {
        self.0.is_active().into()
    }

    /// Build a node filter from a boolean column of an existing node-state result.
    ///
    /// Arguments:
    ///     state (OutputNodeState): A pre-computed node state (e.g. from an algorithm).
    ///     col (str): Name of the boolean column on `state` whose values determine inclusion.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn by_state_column(&self, state: &PyOutputNodeState, col: String) -> PyResult<PyExpr> {
        let op = NodeStateBoolColOp::new(&state.inner, &col)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(PyExpr(Arc::new(op)))
    }
}
