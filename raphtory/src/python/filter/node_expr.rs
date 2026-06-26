use crate::{
    db::graph::views::filter::model::{
        degree_filter::DegreeFilterFactory,
        node_expr::{CreateOp, DynCreateOp, EntityExpr},
        node_state_filter::NodeStateBoolColOp,
        CreateView, EntityMarker, InternalViewWrapOps, NodeViewFilterOps, PropertyExpr,
        PropertyFilterFactory, ViewWrapOps,
    },
    prelude::{EntityAggOps, EntityExprFilterOps, NodeFilter, NodeFilterFactory},
    python::{graph::node_state::PyOutputNodeState, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, PyResult, Python};
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
use std::sync::Arc;

trait DynTemporal: DynCreateOp {
    fn temporal(&self) -> Arc<dyn DynCreateOp>;
}

impl<
        E: EntityExpr<Marker: Into<EntityMarker>>
            + CreateView
            + NodeFilterFactory
            + Send
            + Sync
            + 'static,
    > DynTemporal for PropertyExpr<E>
{
    fn temporal(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.temporal())
    }
}
// filter.Node.neighbours.is_active.all
#[pyclass(frozen, subclass, name = "Expr", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExpr(Arc<dyn DynCreateOp>);

#[pyclass(frozen, extends = PyExpr, name = "PropertyExpr", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyPropertyExpr(Arc<dyn DynTemporal>);

impl<'py> IntoPyObject<'py> for PyPropertyExpr {
    type Target = PyPropertyExpr;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let parent = PyExpr(self.0.clone());
        let child = self;
        Bound::new(py, (child, parent))
    }
}

impl<E: CreateOp<Marker: Into<EntityMarker>>> From<E> for PyExpr {
    fn from(value: E) -> Self {
        PyExpr(Arc::new(value))
    }
}

impl From<Arc<dyn DynTemporal>> for PyPropertyExpr {
    fn from(value: Arc<dyn DynTemporal>) -> Self {
        PyPropertyExpr(value)
    }
}

impl From<Arc<dyn DynNodeFilterFactory>> for PyNodeFilter {
    fn from(value: Arc<dyn DynNodeFilterFactory>) -> Self {
        PyNodeFilter(value)
    }
}

#[pymethods]
impl PyExpr {
    fn __eq__(&self, other: &Self) -> Self {
        self.0.clone().eq(other.0.clone()).into()
    }
    fn __ne__(&self, other: &Self) -> Self {
        self.0.clone().ne(other.0.clone()).into()
    }
    fn __lt__(&self, other: &Self) -> Self {
        self.0.clone().lt(other.0.clone()).into()
    }
    fn __le__(&self, other: &Self) -> Self {
        self.0.clone().le(other.0.clone()).into()
    }
    fn __gt__(&self, other: &Self) -> Self {
        self.0.clone().gt(other.0.clone()).into()
    }
    fn __ge__(&self, other: &Self) -> Self {
        self.0.clone().ge(other.0.clone()).into()
    }

    fn starts_with(&self, other: &Self) -> Self {
        self.0.clone().starts_with(other.0.clone()).into()
    }
    fn ends_with(&self, other: &Self) -> Self {
        self.0.clone().ends_with(other.0.clone()).into()
    }
    fn contains(&self, other: &Self) -> Self {
        self.0.clone().contains(other.0.clone()).into()
    }
    fn not_contains(&self, other: &Self) -> Self {
        self.0.clone().not_contains(other.0.clone()).into()
    }
    fn fuzzy_search(&self, other: &Self, levenshtein_distance: usize, prefix_match: bool) -> Self {
        self.0
            .clone()
            .fuzzy_search(other.0.clone(), levenshtein_distance, prefix_match)
            .into()
    }

    fn is_in(&self, values: FromIterable<Prop>) -> Self {
        self.0.clone().is_in(values).into()
    }
    fn is_not_in(&self, values: FromIterable<Prop>) -> Self {
        self.0.clone().is_not_in(values).into()
    }

    fn is_some(&self) -> Self {
        self.0.clone().is_some().into()
    }
    fn is_none(&self) -> Self {
        self.0.clone().is_none().into()
    }

    fn __invert__(&self) -> Self {
        self.0.clone().not().into()
    }

    fn any(&self) -> Self {
        self.0.clone().any().into()
    }
    fn all(&self) -> Self {
        self.0.clone().all().into()
    }

    fn sum(&self) -> Self {
        self.0.clone().sum().into()
    }
    fn avg(&self) -> Self {
        self.0.clone().avg().into()
    }
    fn min(&self) -> Self {
        self.0.clone().min().into()
    }
    fn max(&self) -> Self {
        self.0.clone().max().into()
    }
    fn first(&self) -> Self {
        self.0.clone().first().into()
    }
    fn last(&self) -> Self {
        self.0.clone().last().into()
    }
    fn len(&self) -> Self {
        self.0.clone().len().into()
    }

    // ── Temporal ────────────────────────────────────────────────────────
    // `.temporal()` only exists on `PropertyExpr<E>`. To expose it on PyExpr
    // you need either a separate `PyPropertyExpr` subtype, or a `dyn_temporal`
    // method on `DynCreateOp` that downcasts/dispatches.
    //
    // fn temporal(&self) -> Self { … }
}

pub trait DynNodeFilterFactory: Send + Sync + 'static {
    fn dyn_id(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_name(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_node_type(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_degree(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_in_degree(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_out_degree(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_is_active(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_property(&self, name: String) -> Arc<dyn DynTemporal>;
    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp>;

    fn dyn_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynNodeFilterFactory>;
    fn dyn_at(&self, time: EventTime) -> Arc<dyn DynNodeFilterFactory>;
    fn dyn_after(&self, time: EventTime) -> Arc<dyn DynNodeFilterFactory>;
    fn dyn_before(&self, time: EventTime) -> Arc<dyn DynNodeFilterFactory>;
    fn dyn_latest(&self) -> Arc<dyn DynNodeFilterFactory>;
    fn dyn_snapshot_at(&self, time: EventTime) -> Arc<dyn DynNodeFilterFactory>;
    fn dyn_snapshot_latest(&self) -> Arc<dyn DynNodeFilterFactory>;
    fn dyn_layer(&self, layers: Vec<String>) -> Arc<dyn DynNodeFilterFactory>;
}

impl InternalViewWrapOps for Arc<dyn DynNodeFilterFactory> {
    type Window = Arc<dyn DynNodeFilterFactory>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        self.dyn_window(start, end)
    }
}

impl<T> DynNodeFilterFactory for T
where
    T: NodeFilterFactory + ViewWrapOps + CreateView + EntityExpr + Clone + Send + Sync + 'static,
    <T as EntityExpr>::Marker: Into<EntityMarker>,
{
    fn dyn_id(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.id())
    }
    fn dyn_name(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.name())
    }
    fn dyn_node_type(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.node_type())
    }

    fn dyn_degree(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.degree())
    }
    fn dyn_in_degree(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.in_degree())
    }
    fn dyn_out_degree(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.out_degree())
    }

    fn dyn_is_active(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.is_active())
    }

    fn dyn_property(&self, name: String) -> Arc<dyn DynTemporal> {
        Arc::new(self.property(name))
    }
    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp> {
        Arc::new(PropertyFilterFactory::metadata(self, name))
    }

    // Go dynamic before calling window — the Arc<dyn DynNodeFilterFactory> impl
    // has Window = Self, which terminates the recursive bound resolution.
    fn dyn_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynNodeFilterFactory> {
        let dyn_self: Arc<dyn DynNodeFilterFactory> = Arc::new(self.clone());
        dyn_self.window(start, end)
    }
    fn dyn_at(&self, time: EventTime) -> Arc<dyn DynNodeFilterFactory> {
        let dyn_self: Arc<dyn DynNodeFilterFactory> = Arc::new(self.clone());
        dyn_self.at(time)
    }
    fn dyn_after(&self, time: EventTime) -> Arc<dyn DynNodeFilterFactory> {
        let dyn_self: Arc<dyn DynNodeFilterFactory> = Arc::new(self.clone());
        dyn_self.after(time)
    }
    fn dyn_before(&self, time: EventTime) -> Arc<dyn DynNodeFilterFactory> {
        let dyn_self: Arc<dyn DynNodeFilterFactory> = Arc::new(self.clone());
        dyn_self.before(time)
    }
    fn dyn_latest(&self) -> Arc<dyn DynNodeFilterFactory> {
        Arc::new(self.clone().latest())
    }
    fn dyn_snapshot_at(&self, time: EventTime) -> Arc<dyn DynNodeFilterFactory> {
        Arc::new(self.clone().snapshot_at(time))
    }
    fn dyn_snapshot_latest(&self) -> Arc<dyn DynNodeFilterFactory> {
        Arc::new(self.clone().snapshot_latest())
    }
    fn dyn_layer(&self, layers: Vec<String>) -> Arc<dyn DynNodeFilterFactory> {
        Arc::new(self.clone().layer(layers))
    }
}

/// Constructs node filter expressions.
///
/// Each method returns either:
/// - a field-specific filter builder, or
/// - a view-restricted filter context, or
/// - a boolean predicate over node state.
#[pyclass(frozen, name = "Node", module = "raphtory.filter")]
pub struct PyNodeFilter(Arc<dyn DynNodeFilterFactory>);

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
        self.0.dyn_id().into()
    }

    /// Selects the node name field for filtering.
    ///
    /// Returns:
    ///     filter.NodeNameFilterBuilder:
    fn name(&self) -> PyExpr {
        self.0.dyn_name().into()
    }

    /// Selects the node type field for filtering.
    ///
    /// Returns:
    ///     filter.NodeTypeFilterBuilder:
    fn node_type(&self) -> PyExpr {
        self.0.dyn_node_type().into()
    }

    /// Selects incoming node degree for filtering.
    ///
    /// Returns:
    ///     filter.FilterOps
    fn in_degree(&self) -> PyExpr {
        self.0.dyn_in_degree().into()
    }

    /// Selects total node degree for filtering.
    ///
    /// Returns:
    ///     filter.FilterOps
    fn degree(&self) -> PyExpr {
        self.0.dyn_degree().into()
    }

    /// Selects outgoing node degree for filtering.
    ///
    /// Returns:
    ///     filter.FilterOps
    fn out_degree(&self) -> PyExpr {
        self.0.dyn_out_degree().into()
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
    fn property(&self, name: String) -> PyPropertyExpr {
        self.0.dyn_property(name).into()
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
        self.0.dyn_metadata(name).into()
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
        self.0.dyn_window(start, end).into()
    }

    /// Restricts node evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn at(&self, time: EventTime) -> PyNodeFilter {
        self.0.dyn_at(time).into()
    }

    /// Restricts node evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn after(&self, time: EventTime) -> PyNodeFilter {
        self.0.dyn_after(time).into()
    }

    /// Restricts node evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn before(&self, time: EventTime) -> PyNodeFilter {
        self.0.dyn_before(time).into()
    }

    /// Evaluates filters against the latest available state of each node.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn latest(&self) -> PyNodeFilter {
        self.0.dyn_latest().into()
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn snapshot_at(&self, time: EventTime) -> PyNodeFilter {
        self.0.dyn_snapshot_at(time).into()
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn snapshot_latest(&self) -> PyNodeFilter {
        self.0.dyn_snapshot_latest().into()
    }

    /// Restricts evaluation to nodes belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn layer(&self, layer: String) -> PyNodeFilter {
        self.0.dyn_layer(vec![layer]).into()
    }

    /// Restricts evaluation to nodes belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder:
    fn layers(&self, layers: FromIterable<String>) -> PyNodeFilter {
        self.0.dyn_layer(layers.to_vec()).into()
    }

    /// Matches nodes that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_active(&self) -> PyExpr {
        self.0.dyn_is_active().into()
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

// TODO:
//  We need a wrapper for Arc<dyn DynCreateFilter>
//  Py Expr needs to extend this wrapper (PyFilter)
//  by_state_column can return pyfilter
