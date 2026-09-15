use crate::{
    db::graph::views::filter::model::{
        dyn_factory::DynNodeFilterFactory,
        filter::{FieldFilterValue, NODE_ID_FIELD, NODE_NAME_FIELD, NODE_TYPE_FIELD},
        is_active_node_filter::IsActiveNode,
        node_expr::{ops::prop_as_gid, CreateOp, DynCreateOp, DynEntityExpr, DynTemporal},
        node_filter::CompositeNodeFilter,
        node_state_filter::NodeStateBoolColOp,
        property_filter::{Op, PropertyFilterValue, PropertyRef},
        DynPropertyExprFactory, EntityMarker, FilterOperator, FilterTree, ViewWrapOps,
    },
    prelude::{EntityAggOps, EntityExprFilterOps, NodeFilter},
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            wire::{wrap_node_views, WireEntity, WireLhs, WireTarget, WireValue, WireView},
        },
        graph::node_state::PyOutputNodeState,
        types::iterable::FromIterable,
    },
};
use pyo3::{
    exceptions::PyTypeError, pyclass, pymethods, Bound, FromPyObject, IntoPyObject, PyErr,
    PyResult, Python,
};
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropType},
    storage::timeindex::{AsTime, EventTime},
    Direction,
};
use std::sync::Arc;

// filter.Node.neighbours.is_active.all
#[pyclass(frozen, subclass, name = "Expr", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExpr(Arc<dyn DynCreateOp>, pub(crate) Option<WireLhs>);

#[pyclass(frozen, extends = PyExpr, name = "PropertyExpr", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyPropertyExpr(Arc<dyn DynTemporal>, pub(crate) Option<WireLhs>);

impl<'py> IntoPyObject<'py> for PyPropertyExpr {
    type Target = PyPropertyExpr;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let parent = PyExpr(self.0.clone(), self.1.clone());
        let child = self;
        Bound::new(py, (child, parent))
    }
}

impl<E: CreateOp<Marker: Into<EntityMarker>>> From<E> for PyExpr {
    fn from(value: E) -> Self {
        PyExpr(Arc::new(value), None)
    }
}

impl From<Arc<dyn DynTemporal>> for PyPropertyExpr {
    fn from(value: Arc<dyn DynTemporal>) -> Self {
        PyPropertyExpr(value, None)
    }
}

/// Accepts either another expression or a plain python value (extracted as a
/// `Prop` constant) on the rhs of comparison and string operators.
#[derive(FromPyObject)]
enum ExprOrValue {
    Expr(PyExpr),
    Value(Prop),
}

/// Values are checked against the expression's statically known type at the
/// comparison itself, so a mistyped literal fails where it is written instead
/// of at some later `filter()` call. Unknown types defer to filter time.
/// String operators require a string-castable operand whatever the lhs type.
fn check_str_value(v: &Prop) -> PyResult<()> {
    if v.dtype() != PropType::Str && v.clone().try_cast(PropType::Str).is_err() {
        return Err(PyTypeError::new_err(format!(
            "value {v:?} of type {} is not a valid string operand",
            v.dtype()
        )));
    }
    Ok(())
}

fn check_value(lhs: &Arc<dyn DynCreateOp>, v: &Prop) -> PyResult<()> {
    let pt = lhs.dyn_prop_type();
    if pt != PropType::Empty && v.dtype() != pt && v.clone().try_cast(pt.clone()).is_err() {
        return Err(PyTypeError::new_err(format!(
            "value {v:?} of type {} is not comparable with an expression of type {pt}",
            v.dtype()
        )));
    }
    Ok(())
}

impl PyExpr {
    pub(crate) fn new(op: Arc<dyn DynCreateOp>, wire: Option<WireLhs>) -> Self {
        PyExpr(op, wire)
    }

    /// A value in the shape the recorded lhs target expects on the wire.
    fn wire_single(&self, v: &Prop) -> Option<WireValue> {
        let lhs = self.1.as_ref()?;
        Some(match &lhs.target {
            WireTarget::Field(NODE_ID_FIELD) => {
                WireValue::Field(FieldFilterValue::ID(prop_as_gid(v)?))
            }
            WireTarget::Field(_) => match v {
                Prop::Str(s) => WireValue::Field(FieldFilterValue::Single(s.to_string())),
                _ => return None,
            },
            WireTarget::Prop(_) | WireTarget::Degree(_) => {
                WireValue::Prop(PropertyFilterValue::Single(v.clone()))
            }
        })
    }

    fn wire_set(&self, values: &[Prop]) -> Option<WireValue> {
        let lhs = self.1.as_ref()?;
        Some(match &lhs.target {
            WireTarget::Field(NODE_ID_FIELD) => WireValue::Field(FieldFilterValue::IDSet(
                Arc::new(values.iter().map(prop_as_gid).collect::<Option<_>>()?),
            )),
            WireTarget::Field(_) => WireValue::Field(FieldFilterValue::Set(Arc::new(
                values
                    .iter()
                    .map(|v| match v {
                        Prop::Str(s) => Some(s.to_string()),
                        _ => None,
                    })
                    .collect::<Option<_>>()?,
            ))),
            WireTarget::Prop(_) | WireTarget::Degree(_) => WireValue::Prop(
                PropertyFilterValue::Set(Arc::new(values.iter().cloned().collect())),
            ),
        })
    }

    fn finish(&self, operator: FilterOperator, value: Option<WireValue>) -> Option<FilterTree> {
        self.1.clone()?.finish(operator, value?)
    }

    fn with_op(&self, expr: Arc<dyn DynCreateOp>, op: Op) -> Self {
        PyExpr(expr, self.1.clone().map(|w| w.with_op(op)))
    }
}

#[pymethods]
impl PyExpr {
    fn __eq__(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        match other {
            ExprOrValue::Expr(e) => Ok(PyFilterExpr(Arc::new(self.0.clone().eq(e.0)), None)),
            ExprOrValue::Value(v) => {
                check_value(&self.0, &v)?;
                let wire = self.finish(FilterOperator::Eq, self.wire_single(&v));
                Ok(PyFilterExpr(Arc::new(self.0.clone().eq(v)), wire))
            }
        }
    }
    fn __ne__(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        match other {
            ExprOrValue::Expr(e) => Ok(PyFilterExpr(Arc::new(self.0.clone().ne(e.0)), None)),
            ExprOrValue::Value(v) => {
                check_value(&self.0, &v)?;
                let wire = self.finish(FilterOperator::Ne, self.wire_single(&v));
                Ok(PyFilterExpr(Arc::new(self.0.clone().ne(v)), wire))
            }
        }
    }
    fn __lt__(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        match other {
            ExprOrValue::Expr(e) => Ok(PyFilterExpr(Arc::new(self.0.clone().lt(e.0)), None)),
            ExprOrValue::Value(v) => {
                check_value(&self.0, &v)?;
                let wire = self.finish(FilterOperator::Lt, self.wire_single(&v));
                Ok(PyFilterExpr(Arc::new(self.0.clone().lt(v)), wire))
            }
        }
    }
    fn __le__(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        match other {
            ExprOrValue::Expr(e) => Ok(PyFilterExpr(Arc::new(self.0.clone().le(e.0)), None)),
            ExprOrValue::Value(v) => {
                check_value(&self.0, &v)?;
                let wire = self.finish(FilterOperator::Le, self.wire_single(&v));
                Ok(PyFilterExpr(Arc::new(self.0.clone().le(v)), wire))
            }
        }
    }
    fn __gt__(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        match other {
            ExprOrValue::Expr(e) => Ok(PyFilterExpr(Arc::new(self.0.clone().gt(e.0)), None)),
            ExprOrValue::Value(v) => {
                check_value(&self.0, &v)?;
                let wire = self.finish(FilterOperator::Gt, self.wire_single(&v));
                Ok(PyFilterExpr(Arc::new(self.0.clone().gt(v)), wire))
            }
        }
    }
    fn __ge__(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        match other {
            ExprOrValue::Expr(e) => Ok(PyFilterExpr(Arc::new(self.0.clone().ge(e.0)), None)),
            ExprOrValue::Value(v) => {
                check_value(&self.0, &v)?;
                let wire = self.finish(FilterOperator::Ge, self.wire_single(&v));
                Ok(PyFilterExpr(Arc::new(self.0.clone().ge(v)), wire))
            }
        }
    }

    /// Checks whether the value's string representation starts with the given value.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): Prefix to check for.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn starts_with(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        match other {
            ExprOrValue::Expr(e) => Ok(PyFilterExpr(
                Arc::new(self.0.clone().starts_with(e.0)),
                None,
            )),
            ExprOrValue::Value(v) => {
                check_str_value(&v)?;
                check_value(&self.0, &v)?;
                let wire = self.finish(FilterOperator::StartsWith, self.wire_single(&v));
                Ok(PyFilterExpr(Arc::new(self.0.clone().starts_with(v)), wire))
            }
        }
    }
    /// Checks whether the value's string representation ends with the given value.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): Suffix to check for.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn ends_with(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        match other {
            ExprOrValue::Expr(e) => Ok(PyFilterExpr(Arc::new(self.0.clone().ends_with(e.0)), None)),
            ExprOrValue::Value(v) => {
                check_str_value(&v)?;
                check_value(&self.0, &v)?;
                let wire = self.finish(FilterOperator::EndsWith, self.wire_single(&v));
                Ok(PyFilterExpr(Arc::new(self.0.clone().ends_with(v)), wire))
            }
        }
    }
    /// Checks whether the value's string representation contains the given value.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): Substring that must appear within the value.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn contains(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        match other {
            ExprOrValue::Expr(e) => Ok(PyFilterExpr(Arc::new(self.0.clone().contains(e.0)), None)),
            ExprOrValue::Value(v) => {
                check_str_value(&v)?;
                check_value(&self.0, &v)?;
                let wire = self.finish(FilterOperator::Contains, self.wire_single(&v));
                Ok(PyFilterExpr(Arc::new(self.0.clone().contains(v)), wire))
            }
        }
    }
    /// Checks whether the value's string representation **does not** contain the given value.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): Substring that must not appear within the value.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn not_contains(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        match other {
            ExprOrValue::Expr(e) => Ok(PyFilterExpr(
                Arc::new(self.0.clone().not_contains(e.0)),
                None,
            )),
            ExprOrValue::Value(v) => {
                check_str_value(&v)?;
                check_value(&self.0, &v)?;
                let wire = self.finish(FilterOperator::NotContains, self.wire_single(&v));
                Ok(PyFilterExpr(Arc::new(self.0.clone().not_contains(v)), wire))
            }
        }
    }
    /// Performs fuzzy matching against the value's string representation, within a Levenshtein distance and with optional prefix matching.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): String to approximately match against.
    ///     levenshtein_distance (int): Maximum allowed Levenshtein distance.
    ///     prefix_match (bool): Whether to require a matching prefix.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn fuzzy_search(
        &self,
        other: ExprOrValue,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyResult<PyFilterExpr> {
        Ok(match other {
            ExprOrValue::Expr(e) => PyFilterExpr(
                Arc::new(
                    self.0
                        .clone()
                        .fuzzy_search(e.0, levenshtein_distance, prefix_match),
                ),
                None,
            ),
            ExprOrValue::Value(v) => {
                check_str_value(&v)?;
                let wire = self.finish(
                    FilterOperator::FuzzySearch {
                        levenshtein_distance,
                        prefix_match,
                    },
                    self.wire_single(&v),
                );
                PyFilterExpr(
                    Arc::new(
                        self.0
                            .clone()
                            .fuzzy_search(v, levenshtein_distance, prefix_match),
                    ),
                    wire,
                )
            }
        })
    }

    /// Checks whether the value is contained within the given values.
    ///
    /// Arguments:
    ///     values (list[Prop]): Values to match against.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        let values: Vec<Prop> = values.into();
        let wire = self.finish(FilterOperator::IsIn, self.wire_set(&values));
        PyFilterExpr(Arc::new(self.0.clone().is_in(values)), wire)
    }
    /// Checks whether the value is **not** contained within the given values.
    ///
    /// Arguments:
    ///     values (list[Prop]): Values to exclude.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_not_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        let values: Vec<Prop> = values.into();
        let wire = self.finish(FilterOperator::IsNotIn, self.wire_set(&values));
        PyFilterExpr(Arc::new(self.0.clone().is_not_in(values)), wire)
    }

    /// Checks whether the value is present (not `None`).
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_some(&self) -> PyFilterExpr {
        let wire = self.finish(
            FilterOperator::IsSome,
            Some(WireValue::Prop(PropertyFilterValue::None)),
        );
        PyFilterExpr(Arc::new(self.0.clone().is_some()), wire)
    }
    /// Checks whether the value is `None` / missing.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_none(&self) -> PyFilterExpr {
        let wire = self.finish(
            FilterOperator::IsNone,
            Some(WireValue::Prop(PropertyFilterValue::None)),
        );
        PyFilterExpr(Arc::new(self.0.clone().is_none()), wire)
    }

    /// Requires that **any** element matches when the value is list-like (a temporal history or a list property).
    ///
    /// Returns:
    ///     filter.Expr:
    fn any(&self) -> Self {
        self.with_op(Arc::new(self.0.clone().any()), Op::Any)
    }
    /// Requires that **all** elements match when the value is list-like (a temporal history or a list property).
    ///
    /// Returns:
    ///     filter.Expr:
    fn all(&self) -> Self {
        self.with_op(Arc::new(self.0.clone().all()), Op::All)
    }

    /// Sums the elements when the value is numeric and list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn sum(&self) -> Self {
        self.with_op(Arc::new(self.0.clone().sum()), Op::Sum)
    }
    /// Averages the elements when the value is numeric and list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn avg(&self) -> Self {
        self.with_op(Arc::new(self.0.clone().avg()), Op::Avg)
    }
    /// Selects the minimum element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn min(&self) -> Self {
        self.with_op(Arc::new(self.0.clone().min()), Op::Min)
    }
    /// Selects the maximum element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn max(&self) -> Self {
        self.with_op(Arc::new(self.0.clone().max()), Op::Max)
    }
    /// Selects the first element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn first(&self) -> Self {
        self.with_op(Arc::new(self.0.clone().first()), Op::First)
    }
    /// Selects the last element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn last(&self) -> Self {
        self.with_op(Arc::new(self.0.clone().last()), Op::Last)
    }
    /// Selects the number of elements when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn len(&self) -> Self {
        self.with_op(Arc::new(self.0.clone().len()), Op::Len)
    }
}

impl PyPropertyExpr {
    pub(crate) fn new(expr: Arc<dyn DynTemporal>, wire: Option<WireLhs>) -> Self {
        PyPropertyExpr(expr, wire)
    }
}

#[pymethods]
impl PyPropertyExpr {
    /// Switches from the property's latest value to its full temporal history,
    /// unlocking the aggregate chain (`sum`, `avg`, `min`, `max`, `any`, ...).
    ///
    /// Returns:
    ///     filter.Expr:
    fn temporal(&self) -> PyExpr {
        PyExpr(
            self.0.temporal(),
            self.1.clone().and_then(WireLhs::temporal),
        )
    }
}

/// A node filter scoped to a view.
///
/// Obtained from the view methods on [`Node`] (`Node.window(...)`,
/// `Node.latest()`, ...); its field and property methods evaluate within that
/// view, and its own view methods narrow it further.
#[pyclass(frozen, name = "NodeFilter", module = "raphtory.filter")]
pub struct PyNodeFilter(Arc<dyn DynNodeFilterFactory>, Vec<WireView>);

impl PyNodeFilter {
    pub(crate) fn root() -> Self {
        PyNodeFilter(Arc::new(NodeFilter), Vec::new())
    }

    fn wrap<T: DynNodeFilterFactory>(&self, filter: T, view: WireView) -> Self {
        let mut views = self.1.clone();
        views.push(view);
        Self(Arc::new(filter), views)
    }

    fn lhs(&self, target: WireTarget) -> WireLhs {
        WireLhs {
            entity: WireEntity::Node,
            endpoint: None,
            target,
            ops: Vec::new(),
            views: self.1.clone(),
        }
    }
}

#[pymethods]
impl PyNodeFilter {
    #[new]
    fn new() -> PyNodeFilter {
        Self::root()
    }

    /// Selects the node ID field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn id(&self) -> PyExpr {
        PyExpr(
            self.0.dyn_id(),
            Some(self.lhs(WireTarget::Field(NODE_ID_FIELD))),
        )
    }

    /// Selects the node name field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn name(&self) -> PyExpr {
        PyExpr(
            self.0.dyn_name(),
            Some(self.lhs(WireTarget::Field(NODE_NAME_FIELD))),
        )
    }

    /// Selects the node type field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn node_type(&self) -> PyExpr {
        PyExpr(
            self.0.dyn_node_type(),
            Some(self.lhs(WireTarget::Field(NODE_TYPE_FIELD))),
        )
    }

    /// Selects incoming node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn in_degree(&self) -> PyExpr {
        PyExpr(
            self.0.dyn_in_degree(),
            Some(self.lhs(WireTarget::Degree(Direction::IN))),
        )
    }

    /// Selects total node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn degree(&self) -> PyExpr {
        PyExpr(
            self.0.dyn_degree(),
            Some(self.lhs(WireTarget::Degree(Direction::BOTH))),
        )
    }

    /// Selects outgoing node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn out_degree(&self) -> PyExpr {
        PyExpr(
            self.0.dyn_out_degree(),
            Some(self.lhs(WireTarget::Degree(Direction::OUT))),
        )
    }

    /// Filters a node property by name.
    ///
    /// The property may be static or temporal depending on the query context.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    fn property(&self, name: String) -> PyPropertyExpr {
        let lhs = self.lhs(WireTarget::Prop(PropertyRef::Property(name.clone())));
        PyPropertyExpr(self.0.dyn_property(name), Some(lhs))
    }

    /// Filters a node metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of a node.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    fn metadata(&self, name: String) -> PyExpr {
        let lhs = self.lhs(WireTarget::Prop(PropertyRef::Metadata(name.clone())));
        PyExpr(self.0.dyn_metadata(name), Some(lhs))
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
    ///     filter.NodeFilter:
    fn window(&self, start: EventTime, end: EventTime) -> PyNodeFilter {
        self.wrap(
            self.0.clone().window(start, end),
            WireView::Window(start, end),
        )
    }

    /// Restricts node evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn at(&self, time: EventTime) -> PyNodeFilter {
        self.wrap(
            self.0.clone().at(time),
            WireView::Window(time, EventTime::end(time.t().saturating_add(1))),
        )
    }

    /// Restricts node evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn after(&self, time: EventTime) -> PyNodeFilter {
        self.wrap(
            self.0.clone().after(time),
            WireView::Window(
                EventTime::start(time.t().saturating_add(1)),
                EventTime::end(i64::MAX),
            ),
        )
    }

    /// Restricts node evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn before(&self, time: EventTime) -> PyNodeFilter {
        self.wrap(
            self.0.clone().before(time),
            WireView::Window(EventTime::start(i64::MIN), EventTime::end(time.t())),
        )
    }

    /// Evaluates filters against the latest available state of each node.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn latest(&self) -> PyNodeFilter {
        self.wrap(self.0.clone().latest(), WireView::Latest)
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn snapshot_at(&self, time: EventTime) -> PyNodeFilter {
        self.wrap(self.0.clone().snapshot_at(time), WireView::SnapshotAt(time))
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn snapshot_latest(&self) -> PyNodeFilter {
        self.wrap(self.0.clone().snapshot_latest(), WireView::SnapshotLatest)
    }

    /// Restricts evaluation to nodes belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn layer(&self, layer: String) -> PyNodeFilter {
        self.wrap(
            self.0.clone().layer(vec![layer.clone()]),
            WireView::Layers(vec![layer]),
        )
    }

    /// Restricts evaluation to nodes belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn layers(&self, layers: FromIterable<String>) -> PyNodeFilter {
        let layers: Vec<String> = layers.into();
        self.wrap(
            self.0.clone().layer(layers.clone()),
            WireView::Layers(layers),
        )
    }

    /// Matches nodes that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_active(&self) -> PyFilterExpr {
        let tree = FilterTree::Node(wrap_node_views(
            CompositeNodeFilter::IsActiveNode(IsActiveNode),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_active(), Some(tree))
    }

    /// Build a node filter from a boolean column of an existing node-state result.
    ///
    /// Arguments:
    ///     state (OutputNodeState): A pre-computed node state (e.g. from an algorithm).
    ///     col (str): Name of the boolean column on `state` whose values determine inclusion.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn by_state_column(&self, state: &PyOutputNodeState, col: String) -> PyResult<PyFilterExpr> {
        let op = NodeStateBoolColOp::new(&state.inner, &col)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(PyFilterExpr(Arc::new(op), None))
    }
}

/// Entry point for constructing node filter expressions.
///
/// Every method is static: `Node.property("age") > 30` selects nodes
/// directly, and the view methods (`window`, `latest`, `layer`, ...) return a
/// [`NodeFilter`] scoped to that view for further chaining.
#[pyclass(frozen, name = "Node", module = "raphtory.filter")]
pub struct PyNode;

#[pymethods]
impl PyNode {
    /// Selects the node ID field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn id() -> PyExpr {
        PyNodeFilter::root().id()
    }

    /// Selects the node name field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn name() -> PyExpr {
        PyNodeFilter::root().name()
    }

    /// Selects the node type field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn node_type() -> PyExpr {
        PyNodeFilter::root().node_type()
    }

    /// Selects incoming node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn in_degree() -> PyExpr {
        PyNodeFilter::root().in_degree()
    }

    /// Selects total node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn degree() -> PyExpr {
        PyNodeFilter::root().degree()
    }

    /// Selects outgoing node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn out_degree() -> PyExpr {
        PyNodeFilter::root().out_degree()
    }

    /// Filters a node property by name.
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
        PyNodeFilter::root().property(name)
    }

    /// Filters a node metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of a node.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn metadata(name: String) -> PyExpr {
        PyNodeFilter::root().metadata(name)
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
    ///     filter.NodeFilter:
    #[staticmethod]
    fn window(start: EventTime, end: EventTime) -> PyNodeFilter {
        PyNodeFilter::root().window(start, end)
    }

    /// Restricts node evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn at(time: EventTime) -> PyNodeFilter {
        PyNodeFilter::root().at(time)
    }

    /// Restricts node evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn after(time: EventTime) -> PyNodeFilter {
        PyNodeFilter::root().after(time)
    }

    /// Restricts node evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn before(time: EventTime) -> PyNodeFilter {
        PyNodeFilter::root().before(time)
    }

    /// Evaluates filters against the latest available state of each node.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn latest() -> PyNodeFilter {
        PyNodeFilter::root().latest()
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyNodeFilter {
        PyNodeFilter::root().snapshot_at(time)
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn snapshot_latest() -> PyNodeFilter {
        PyNodeFilter::root().snapshot_latest()
    }

    /// Restricts evaluation to nodes belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn layer(layer: String) -> PyNodeFilter {
        PyNodeFilter::root().layer(layer)
    }

    /// Restricts evaluation to nodes belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyNodeFilter {
        PyNodeFilter::root().layers(layers)
    }

    /// Matches nodes that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    #[staticmethod]
    fn is_active() -> PyFilterExpr {
        PyNodeFilter::root().is_active()
    }

    /// Build a node filter from a boolean column of an existing node-state result.
    ///
    /// Arguments:
    ///     state (OutputNodeState): A pre-computed node state (e.g. from an algorithm).
    ///     col (str): Name of the boolean column on `state` whose values determine inclusion.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    #[staticmethod]
    fn by_state_column(state: &PyOutputNodeState, col: String) -> PyResult<PyFilterExpr> {
        PyNodeFilter::root().by_state_column(state, col)
    }
}
