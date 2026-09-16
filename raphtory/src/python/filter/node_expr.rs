use crate::{
    db::graph::views::filter::model::{
        node_state_filter::NodeStateBoolColOp,
        tree::{
            Agg, CmpOp, Entity, Expr, Field, FilterExpr, OpaqueFilter, Qual, Scope, StrOp,
            Structural, Target, ViewOp,
        },
    },
    python::{
        filter::filter_expr::PyFilterExpr, graph::node_state::PyOutputNodeState,
        types::iterable::FromIterable,
    },
};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    pyclass, pymethods, Bound, FromPyObject, IntoPyObject, PyErr, PyResult, Python,
};
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropType},
    storage::timeindex::EventTime,
    Direction,
};
use std::sync::Arc;

/// A value expression: a field, degree, property, metadata entry or an
/// aggregate over one. Comparing it to a value or to another expression gives
/// a [`FilterExpr`].
#[pyclass(
    frozen,
    subclass,
    name = "Expr",
    module = "raphtory.filter",
    from_py_object
)]
#[derive(Clone)]
pub struct PyExpr(pub(crate) Expr);

/// A property read, which can switch to the property's history with `temporal()`.
#[pyclass(
    frozen,
    extends = PyExpr,
    name = "PropertyExpr",
    module = "raphtory.filter",
    from_py_object
)]
#[derive(Clone)]
pub struct PyPropertyExpr(pub(crate) Expr);

impl<'py> IntoPyObject<'py> for PyPropertyExpr {
    type Target = PyPropertyExpr;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let parent = PyExpr(self.0.clone());
        Bound::new(py, (self, parent))
    }
}

/// Accepts either another expression or a plain python value (a constant) on
/// the right-hand side of comparison and string operators.
#[derive(FromPyObject)]
enum ExprOrValue {
    Expr(PyExpr),
    Value(Prop),
}

/// Values are checked against the expression's statically known type at the
/// comparison itself, so a mistyped literal fails where it is written instead
/// of at some later `filter()` call. Unknown types defer to filter time.
fn static_type(lhs: &Expr) -> PyResult<PropType> {
    Ok(lhs.compile()?.dyn_prop_type())
}

fn check_value(lhs: &Expr, v: &Prop) -> PyResult<()> {
    let pt = static_type(lhs)?;
    if pt != PropType::Empty && v.dtype() != pt && v.clone().try_cast(pt.clone()).is_err() {
        return Err(PyTypeError::new_err(format!(
            "value {v:?} of type {} is not comparable with an expression of type {pt}",
            v.dtype()
        )));
    }
    Ok(())
}

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

/// The right-hand side of a comparison, with a constant checked against the lhs.
fn compared(lhs: &Expr, other: ExprOrValue) -> PyResult<Expr> {
    Ok(match other {
        ExprOrValue::Expr(e) => e.0,
        ExprOrValue::Value(v) => {
            check_value(lhs, &v)?;
            Expr::Const(v)
        }
    })
}

/// The right-hand side of a string operator.
fn string_operand(lhs: &Expr, other: ExprOrValue, typed: bool) -> PyResult<Expr> {
    Ok(match other {
        ExprOrValue::Expr(e) => e.0,
        ExprOrValue::Value(v) => {
            check_str_value(&v)?;
            if typed {
                check_value(lhs, &v)?;
            }
            Expr::Const(v)
        }
    })
}

#[pymethods]
impl PyExpr {
    fn __eq__(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        Ok(PyFilterExpr(FilterExpr::Cmp {
            op: CmpOp::Eq,
            lhs: self.0.clone(),
            rhs: compared(&self.0, other)?,
        }))
    }
    fn __ne__(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        Ok(PyFilterExpr(FilterExpr::Cmp {
            op: CmpOp::Ne,
            lhs: self.0.clone(),
            rhs: compared(&self.0, other)?,
        }))
    }
    fn __lt__(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        Ok(PyFilterExpr(FilterExpr::Cmp {
            op: CmpOp::Lt,
            lhs: self.0.clone(),
            rhs: compared(&self.0, other)?,
        }))
    }
    fn __le__(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        Ok(PyFilterExpr(FilterExpr::Cmp {
            op: CmpOp::Le,
            lhs: self.0.clone(),
            rhs: compared(&self.0, other)?,
        }))
    }
    fn __gt__(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        Ok(PyFilterExpr(FilterExpr::Cmp {
            op: CmpOp::Gt,
            lhs: self.0.clone(),
            rhs: compared(&self.0, other)?,
        }))
    }
    fn __ge__(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        Ok(PyFilterExpr(FilterExpr::Cmp {
            op: CmpOp::Ge,
            lhs: self.0.clone(),
            rhs: compared(&self.0, other)?,
        }))
    }

    /// Checks whether the value's string representation starts with the given value.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): Prefix to check for.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn starts_with(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        Ok(PyFilterExpr(FilterExpr::Str {
            op: StrOp::StartsWith,
            lhs: self.0.clone(),
            rhs: string_operand(&self.0, other, true)?,
        }))
    }
    /// Checks whether the value's string representation ends with the given value.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): Suffix to check for.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn ends_with(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        Ok(PyFilterExpr(FilterExpr::Str {
            op: StrOp::EndsWith,
            lhs: self.0.clone(),
            rhs: string_operand(&self.0, other, true)?,
        }))
    }
    /// Checks whether the value's string representation contains the given value.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): Substring that must appear within the value.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn contains(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        Ok(PyFilterExpr(FilterExpr::Str {
            op: StrOp::Contains,
            lhs: self.0.clone(),
            rhs: string_operand(&self.0, other, true)?,
        }))
    }
    /// Checks whether the value's string representation **does not** contain the given value.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): Substring that must not appear within the value.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn not_contains(&self, other: ExprOrValue) -> PyResult<PyFilterExpr> {
        Ok(PyFilterExpr(FilterExpr::Str {
            op: StrOp::NotContains,
            lhs: self.0.clone(),
            rhs: string_operand(&self.0, other, true)?,
        }))
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
        Ok(PyFilterExpr(FilterExpr::Str {
            op: StrOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            lhs: self.0.clone(),
            rhs: string_operand(&self.0, other, false)?,
        }))
    }

    /// Checks whether the value is contained within the given values.
    ///
    /// Arguments:
    ///     values (list[Prop]): Values to match against.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::In {
            expr: self.0.clone(),
            values: values.into(),
            negated: false,
        })
    }
    /// Checks whether the value is **not** contained within the given values.
    ///
    /// Arguments:
    ///     values (list[Prop]): Values to exclude.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_not_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::In {
            expr: self.0.clone(),
            values: values.into(),
            negated: true,
        })
    }

    /// Checks whether the value is present (not `None`).
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_some(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::IsSome(self.0.clone()))
    }
    /// Checks whether the value is `None` / missing.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_none(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::IsNone(self.0.clone()))
    }

    /// Requires that **any** element matches when the value is list-like (a temporal history or a list property).
    ///
    /// Returns:
    ///     filter.Expr:
    fn any(&self) -> Self {
        PyExpr(Expr::Qual(Qual::Any, Box::new(self.0.clone())))
    }
    /// Requires that **all** elements match when the value is list-like (a temporal history or a list property).
    ///
    /// Returns:
    ///     filter.Expr:
    fn all(&self) -> Self {
        PyExpr(Expr::Qual(Qual::All, Box::new(self.0.clone())))
    }

    /// Sums the elements when the value is numeric and list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn sum(&self) -> Self {
        PyExpr(Expr::Agg(Agg::Sum, Box::new(self.0.clone())))
    }
    /// Averages the elements when the value is numeric and list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn avg(&self) -> Self {
        PyExpr(Expr::Agg(Agg::Avg, Box::new(self.0.clone())))
    }
    /// Selects the minimum element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn min(&self) -> Self {
        PyExpr(Expr::Agg(Agg::Min, Box::new(self.0.clone())))
    }
    /// Selects the maximum element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn max(&self) -> Self {
        PyExpr(Expr::Agg(Agg::Max, Box::new(self.0.clone())))
    }
    /// Selects the first element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn first(&self) -> Self {
        PyExpr(Expr::Agg(Agg::First, Box::new(self.0.clone())))
    }
    /// Selects the last element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn last(&self) -> Self {
        PyExpr(Expr::Agg(Agg::Last, Box::new(self.0.clone())))
    }
    /// Selects the number of elements when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn len(&self) -> Self {
        PyExpr(Expr::Agg(Agg::Len, Box::new(self.0.clone())))
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
        PyExpr(Expr::Temporal(Box::new(self.0.clone())))
    }
}

/// A node filter scoped to a view.
///
/// Obtained from the view methods on [`Node`] (`Node.window(...)`,
/// `Node.latest()`, ...); its field and property methods evaluate within that
/// view, and its own view methods narrow it further.
#[pyclass(frozen, name = "NodeFilter", module = "raphtory.filter")]
pub struct PyNodeFilter(pub(crate) Scope);

impl PyNodeFilter {
    pub(crate) fn root() -> Self {
        PyNodeFilter(Scope::new(Entity::Node))
    }

    fn with_view(&self, view: ViewOp) -> Self {
        PyNodeFilter(self.0.clone().with_view(view))
    }

    fn read(&self, target: Target) -> Expr {
        Expr::Read {
            scope: self.0.clone(),
            target,
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
        PyExpr(self.read(Target::Field(Field::Id)))
    }

    /// Selects the node name field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn name(&self) -> PyExpr {
        PyExpr(self.read(Target::Field(Field::Name)))
    }

    /// Selects the node type field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn node_type(&self) -> PyExpr {
        PyExpr(self.read(Target::Field(Field::NodeType)))
    }

    /// Selects incoming node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn in_degree(&self) -> PyExpr {
        PyExpr(self.read(Target::Degree(Direction::IN)))
    }

    /// Selects total node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn degree(&self) -> PyExpr {
        PyExpr(self.read(Target::Degree(Direction::BOTH)))
    }

    /// Selects outgoing node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn out_degree(&self) -> PyExpr {
        PyExpr(self.read(Target::Degree(Direction::OUT)))
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
        PyPropertyExpr(self.read(Target::Property(name)))
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
        PyExpr(self.read(Target::Metadata(name)))
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
        self.with_view(ViewOp::Window { start, end })
    }

    /// Restricts node evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn at(&self, time: EventTime) -> PyNodeFilter {
        self.with_view(ViewOp::At(time))
    }

    /// Restricts node evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn after(&self, time: EventTime) -> PyNodeFilter {
        self.with_view(ViewOp::After(time))
    }

    /// Restricts node evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn before(&self, time: EventTime) -> PyNodeFilter {
        self.with_view(ViewOp::Before(time))
    }

    /// Evaluates filters against the latest available state of each node.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn latest(&self) -> PyNodeFilter {
        self.with_view(ViewOp::Latest)
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn snapshot_at(&self, time: EventTime) -> PyNodeFilter {
        self.with_view(ViewOp::SnapshotAt(time))
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn snapshot_latest(&self) -> PyNodeFilter {
        self.with_view(ViewOp::SnapshotLatest)
    }

    /// Restricts evaluation to nodes belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn layer(&self, layer: String) -> PyNodeFilter {
        self.with_view(ViewOp::Layers(vec![layer]))
    }

    /// Restricts evaluation to nodes belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn layers(&self, layers: FromIterable<String>) -> PyNodeFilter {
        self.with_view(ViewOp::Layers(layers.into()))
    }

    /// Matches nodes that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_active(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Structural {
            scope: self.0.clone(),
            pred: Structural::IsActive,
        })
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
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyFilterExpr(FilterExpr::Opaque(OpaqueFilter(Arc::new(op)))))
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
