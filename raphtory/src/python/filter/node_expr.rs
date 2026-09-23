use crate::{
    db::graph::views::filter::model::{
        expr::{
            Agg, CmpOp, EdgeExpr, ExplodedEdgeExpr, Expr, Field, FilterExpr, Leaf, NodeExpr,
            NodeLeaf, OpaqueFilter, StrOp, ViewOp,
        },
        node_expr::DynCreateOp,
        node_state_filter::NodeStateBoolColOp,
        validate_const_comparable,
    },
    errors::GraphError,
    python::{
        filter::filter_expr::{no_view, ExprOrFilter, PyFilterExpr},
        graph::node_state::PyOutputNodeState,
        types::iterable::FromIterable,
    },
};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    IntoPyObjectExt,
};
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropType},
    storage::timeindex::EventTime,
    Direction,
};
use std::sync::Arc;

/// An expression over one kind of entity. Which kind is fixed by where the
/// chain started (`filter.Node`, `filter.Edge`, `filter.ExplodedEdge`), so
/// the tree it holds is the entity's own.
#[derive(Clone)]
pub(crate) enum Typed {
    Node(NodeExpr),
    Edge(EdgeExpr),
    ExplodedEdge(ExplodedEdgeExpr),
}

/// The same construction on the expression, whatever its entity.
macro_rules! map_typed {
    ($typed:expr, |$e:ident| $body:expr) => {
        match $typed {
            Typed::Node($e) => Typed::Node($body),
            Typed::Edge($e) => Typed::Edge($body),
            Typed::ExplodedEdge($e) => Typed::ExplodedEdge($body),
        }
    };
}

/// A construction over two expressions of the same entity; a mix is refused.
macro_rules! zip_typed {
    ($a:expr, $b:expr, |$l:ident, $r:ident| $body:expr) => {
        match ($a, $b) {
            (Typed::Node($l), Typed::Node($r)) => Ok(Typed::Node($body)),
            (Typed::Edge($l), Typed::Edge($r)) => Ok(Typed::Edge($body)),
            (Typed::ExplodedEdge($l), Typed::ExplodedEdge($r)) => Ok(Typed::ExplodedEdge($body)),
            (a, b) => Err(mixed(&a, &b)),
        }
    };
}

fn mixed(a: &Typed, b: &Typed) -> PyErr {
    PyTypeError::new_err(format!(
        "cannot combine {} expression with {} expression",
        a.entity(),
        b.entity()
    ))
}

impl Typed {
    fn entity(&self) -> &'static str {
        match self {
            Typed::Node(_) => "a node",
            Typed::Edge(_) => "an edge",
            Typed::ExplodedEdge(_) => "an exploded edge",
        }
    }

    /// The compiled value, for its statically known type and nullability.
    fn compile_value(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        match self {
            Typed::Node(e) => e.compile_value(),
            Typed::Edge(e) => e.compile_value(),
            Typed::ExplodedEdge(e) => e.compile_value(),
        }
    }

    /// A constant standing on the other side of this expression.
    fn constant(&self, value: Prop) -> Typed {
        match self {
            Typed::Node(_) => Typed::Node(Expr::Const(value)),
            Typed::Edge(_) => Typed::Edge(Expr::Const(value)),
            Typed::ExplodedEdge(_) => Typed::ExplodedEdge(Expr::Const(value)),
        }
    }

    /// The filter this yes/no expression is, on its entity.
    pub(crate) fn into_filter(self) -> FilterExpr {
        match self {
            Typed::Node(e) => NodeLeaf::filter(e),
            Typed::Edge(e) => <Expr<_> as Into<FilterExpr>>::into(e),
            Typed::ExplodedEdge(e) => FilterExpr::ExplodedEdge(e),
        }
    }
}

impl From<EdgeExpr> for FilterExpr {
    fn from(e: EdgeExpr) -> Self {
        FilterExpr::Edge(e)
    }
}

impl std::fmt::Display for Typed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Typed::Node(e) => e.fmt(f),
            Typed::Edge(e) => e.fmt(f),
            Typed::ExplodedEdge(e) => e.fmt(f),
        }
    }
}

/// A value expression: a field, degree, property, metadata entry, an aggregate
/// over one, or a yes/no built from them. Comparing it to a value or to another
/// expression gives a yes/no [`Expr`], which is a filter on its entity.
#[pyclass(
    frozen,
    subclass,
    name = "Expr",
    module = "raphtory.filter",
    from_py_object
)]
#[derive(Clone)]
pub struct PyExpr(pub(crate) Typed);

/// A property read, which can switch to the property's history with `temporal()`.
#[pyclass(
    frozen,
    extends = PyExpr,
    name = "PropertyExpr",
    module = "raphtory.filter",
    from_py_object
)]
#[derive(Clone)]
pub struct PyPropertyExpr {
    latest: Typed,
    history: Typed,
}

impl PyPropertyExpr {
    /// Both readings of the property, built by `read(temporal)`.
    pub(crate) fn new(read: impl Fn(bool) -> Typed) -> Self {
        PyPropertyExpr {
            latest: read(false),
            history: read(true),
        }
    }
}

impl<'py> IntoPyObject<'py> for PyPropertyExpr {
    type Target = PyPropertyExpr;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let parent = PyExpr(self.latest.clone());
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
/// of at some later `filter()` call. Unknown types defer to filter time. The
/// check is the engine's own; python only asks it early.
fn static_type(lhs: &Typed) -> PyResult<PropType> {
    Ok(lhs.compile_value()?.dyn_prop_type())
}

fn check_value(lhs: &Typed, v: &Prop) -> PyResult<()> {
    validate_const_comparable(&static_type(lhs)?, Some(v))
        .map_err(|e| PyTypeError::new_err(e.to_string()))
}

/// Presence tests only mean something on an expression that can be missing.
fn check_nullable(lhs: &Typed, op: &str) -> PyResult<()> {
    if !lhs.compile_value()?.dyn_nullable() {
        return Err(PyTypeError::new_err(format!(
            "{op}() is not valid on an expression that always has a value"
        )));
    }
    Ok(())
}

/// String operators require a string operand whatever the lhs type.
fn check_str_value(v: &Prop) -> PyResult<()> {
    validate_const_comparable(&PropType::Str, Some(v))
        .map_err(|e| PyTypeError::new_err(e.to_string()))
}

impl PyExpr {
    fn compare(&self, op: CmpOp, other: ExprOrValue) -> PyResult<PyExpr> {
        let rhs = match other {
            ExprOrValue::Expr(e) => e.0,
            ExprOrValue::Value(v) => {
                check_value(&self.0, &v)?;
                self.0.constant(v)
            }
        };
        Ok(PyExpr(zip_typed!(self.0.clone(), rhs, |l, r| Expr::Cmp(
            op,
            Box::new(l),
            Box::new(r)
        ))?))
    }

    fn string_op(&self, op: StrOp, other: ExprOrValue) -> PyResult<PyExpr> {
        let rhs = match other {
            ExprOrValue::Expr(e) => e.0,
            ExprOrValue::Value(v) => {
                check_str_value(&v)?;
                self.0.constant(v)
            }
        };
        Ok(PyExpr(zip_typed!(self.0.clone(), rhs, |l, r| Expr::Str(
            op.clone(),
            Box::new(l),
            Box::new(r)
        ))?))
    }

    fn membership(&self, values: FromIterable<Prop>, negated: bool) -> PyExpr {
        let values: Vec<Prop> = values.into();
        PyExpr(map_typed!(self.0.clone(), |e| Expr::In {
            expr: Box::new(e),
            values: values.clone(),
            negated,
        }))
    }

    fn presence(&self, none: bool, name: &str) -> PyResult<PyExpr> {
        check_nullable(&self.0, name)?;
        Ok(PyExpr(map_typed!(self.0.clone(), |e| if none {
            Expr::IsNone(Box::new(e))
        } else {
            Expr::IsSome(Box::new(e))
        })))
    }

    fn agg(&self, agg: Agg) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| Expr::Agg(agg, Box::new(e))))
    }

    /// `&` / `|` with another expression of the same entity stays an
    /// expression; with a filter, or across entities, it is a filter.
    fn combine<'py>(
        &self,
        py: Python<'py>,
        other: ExprOrFilter,
        all: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        if let ExprOrFilter::Expr(other) = &other {
            let joined = zip_typed!(self.0.clone(), other.0.clone(), |l, r| if all {
                Expr::And(vec![l, r])
            } else {
                Expr::Or(vec![l, r])
            });
            if let Ok(joined) = joined {
                return PyExpr(joined).into_bound_py_any(py);
            }
        }
        let other = other.into_filter();
        let mine = self.0.clone().into_filter();
        let filter = if all {
            FilterExpr::And(vec![mine, other])
        } else {
            no_view(&other)?;
            FilterExpr::Or(vec![mine, other])
        };
        PyFilterExpr(filter).into_bound_py_any(py)
    }
}

#[pymethods]
impl PyExpr {
    fn __eq__(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(CmpOp::Eq, other)
    }

    fn __ne__(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(CmpOp::Ne, other)
    }

    fn __lt__(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(CmpOp::Lt, other)
    }

    fn __le__(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(CmpOp::Le, other)
    }

    fn __gt__(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(CmpOp::Gt, other)
    }

    fn __ge__(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(CmpOp::Ge, other)
    }

    /// `self == other`, as a method, so a qualifier can follow without brackets:
    /// `filter.Node.property("p").temporal().eq(3).any()`.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): The value or expression to compare with.
    ///
    /// Returns:
    ///     filter.Expr:
    fn eq(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(CmpOp::Eq, other)
    }

    /// `self != other`, as a method.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): The value or expression to compare with.
    ///
    /// Returns:
    ///     filter.Expr:
    fn ne(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(CmpOp::Ne, other)
    }

    /// `self < other`, as a method.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): The value or expression to compare with.
    ///
    /// Returns:
    ///     filter.Expr:
    fn lt(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(CmpOp::Lt, other)
    }

    /// `self <= other`, as a method.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): The value or expression to compare with.
    ///
    /// Returns:
    ///     filter.Expr:
    fn le(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(CmpOp::Le, other)
    }

    /// `self > other`, as a method.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): The value or expression to compare with.
    ///
    /// Returns:
    ///     filter.Expr:
    fn gt(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(CmpOp::Gt, other)
    }

    /// `self >= other`, as a method.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): The value or expression to compare with.
    ///
    /// Returns:
    ///     filter.Expr:
    fn ge(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(CmpOp::Ge, other)
    }

    /// Checks whether the string value starts with the given prefix.
    ///
    /// Arguments:
    ///     other (str | filter.Expr): The prefix, or an expression giving it.
    ///
    /// Returns:
    ///     filter.Expr:
    fn starts_with(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.string_op(StrOp::StartsWith, other)
    }

    /// Checks whether the string value ends with the given suffix.
    ///
    /// Arguments:
    ///     other (str | filter.Expr): The suffix, or an expression giving it.
    ///
    /// Returns:
    ///     filter.Expr:
    fn ends_with(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.string_op(StrOp::EndsWith, other)
    }

    /// Checks whether the string value contains the given substring.
    ///
    /// Arguments:
    ///     other (str | filter.Expr): The substring, or an expression giving it.
    ///
    /// Returns:
    ///     filter.Expr:
    fn contains(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.string_op(StrOp::Contains, other)
    }

    /// Checks whether the string value does **not** contain the given substring.
    ///
    /// Arguments:
    ///     other (str | filter.Expr): The substring, or an expression giving it.
    ///
    /// Returns:
    ///     filter.Expr:
    fn not_contains(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.string_op(StrOp::NotContains, other)
    }

    /// Checks whether the string value is within a Levenshtein distance of the given text.
    ///
    /// Arguments:
    ///     other (str | filter.Expr): The text to match, or an expression giving it.
    ///     levenshtein_distance (int): Maximum edit distance for a match.
    ///     prefix_match (bool): Whether a prefix match within the distance also passes.
    ///
    /// Returns:
    ///     filter.Expr:
    fn fuzzy_search(
        &self,
        other: ExprOrValue,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyResult<PyExpr> {
        self.string_op(
            StrOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            other,
        )
    }

    /// Checks whether the value is contained within the given values.
    ///
    /// Arguments:
    ///     values (list[Prop]): Values to match against.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_in(&self, values: FromIterable<Prop>) -> PyExpr {
        self.membership(values, false)
    }

    /// Checks whether the value is **not** contained within the given values.
    ///
    /// Arguments:
    ///     values (list[Prop]): Values to exclude.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_not_in(&self, values: FromIterable<Prop>) -> PyExpr {
        self.membership(values, true)
    }

    /// Checks whether the value is present.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_some(&self) -> PyResult<PyExpr> {
        self.presence(false, "is_some")
    }

    /// Checks whether the value is missing.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_none(&self) -> PyResult<PyExpr> {
        self.presence(true, "is_none")
    }

    /// Requires that **any** element matches. Follows a comparison against a
    /// list-like value (a temporal history or a list property):
    /// `(filter.Node.property("p").temporal() > 4).any()`.
    ///
    /// Returns:
    ///     filter.Expr:
    fn any(&self) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| Expr::Any(Box::new(e))))
    }

    /// Requires that **all** elements match. Follows a comparison against a
    /// list-like value (a temporal history or a list property):
    /// `(filter.Node.property("p").temporal() > 4).all()`.
    ///
    /// Returns:
    ///     filter.Expr:
    fn all(&self) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| Expr::All(Box::new(e))))
    }

    /// Sums the elements when the value is numeric and list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn sum(&self) -> PyExpr {
        self.agg(Agg::Sum)
    }

    /// Averages the elements when the value is numeric and list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn avg(&self) -> PyExpr {
        self.agg(Agg::Avg)
    }

    /// Selects the minimum element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn min(&self) -> PyExpr {
        self.agg(Agg::Min)
    }

    /// Selects the maximum element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn max(&self) -> PyExpr {
        self.agg(Agg::Max)
    }

    /// Selects the first element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn first(&self) -> PyExpr {
        self.agg(Agg::First)
    }

    /// Selects the last element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn last(&self) -> PyExpr {
        self.agg(Agg::Last)
    }

    /// Selects the number of elements when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn len(&self) -> PyExpr {
        self.agg(Agg::Len)
    }

    fn __and__<'py>(&self, py: Python<'py>, other: ExprOrFilter) -> PyResult<Bound<'py, PyAny>> {
        self.combine(py, other, true)
    }

    fn __or__<'py>(&self, py: Python<'py>, other: ExprOrFilter) -> PyResult<Bound<'py, PyAny>> {
        self.combine(py, other, false)
    }

    fn __invert__(&self) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| Expr::Not(Box::new(e))))
    }

    /// Shows the expression tree: what runs locally and what a server receives.
    fn __repr__(&self) -> String {
        format!("Expr({})", self.0)
    }
}

#[pymethods]
impl PyPropertyExpr {
    /// Switches from the property's latest value to its full temporal history,
    /// unlocking the aggregate chain (`sum`, `avg`, `min`, `max`, ...) and the
    /// element-wise comparisons `any()` / `all()` collapse.
    ///
    /// Returns:
    ///     filter.Expr:
    fn temporal(&self) -> PyExpr {
        PyExpr(self.history.clone())
    }
}

/// A node filter scoped to a view.
///
/// Obtained from the view methods on [`Node`] (`Node.window(...)`,
/// `Node.latest()`, ...); its field and property methods evaluate within that
/// view, and its own view methods narrow it further.
#[pyclass(frozen, name = "NodeFilter", module = "raphtory.filter")]
pub struct PyNodeFilter(pub(crate) Vec<ViewOp>);

impl PyNodeFilter {
    pub(crate) fn root() -> Self {
        PyNodeFilter(Vec::new())
    }

    fn with_view(&self, view: ViewOp) -> Self {
        let mut views = self.0.clone();
        views.push(view);
        PyNodeFilter(views)
    }

    fn read(&self, leaf: NodeLeaf) -> PyExpr {
        PyExpr(Typed::Node(Expr::Read(leaf)))
    }

    fn field(&self, field: Field) -> PyExpr {
        self.read(NodeLeaf::Field {
            views: self.0.clone(),
            field,
        })
    }

    fn degree_read(&self, direction: Direction) -> PyExpr {
        self.read(NodeLeaf::Degree {
            views: self.0.clone(),
            direction,
        })
    }

    fn property_read(&self, name: String) -> PyPropertyExpr {
        PyPropertyExpr::new(|temporal| {
            Typed::Node(Expr::Read(NodeLeaf::property(
                self.0.clone(),
                name.clone(),
                temporal,
            )))
        })
    }
}

#[pymethods]
impl PyNodeFilter {
    /// Selects the node ID field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn id(&self) -> PyExpr {
        self.field(Field::Id)
    }

    /// Selects the node name field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn name(&self) -> PyExpr {
        self.field(Field::Name)
    }

    /// Selects the node type field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn node_type(&self) -> PyExpr {
        self.field(Field::NodeType)
    }

    /// Selects incoming node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn in_degree(&self) -> PyExpr {
        self.degree_read(Direction::IN)
    }

    /// Selects total node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn degree(&self) -> PyExpr {
        self.degree_read(Direction::BOTH)
    }

    /// Selects outgoing node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn out_degree(&self) -> PyExpr {
        self.degree_read(Direction::OUT)
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
        self.property_read(name)
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
        self.read(NodeLeaf::metadata(self.0.clone(), name))
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
    ///     filter.Expr:
    fn is_active(&self) -> PyExpr {
        self.read(NodeLeaf::is_active(self.0.clone()))
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
    ///     filter.Expr:
    #[staticmethod]
    fn is_active() -> PyExpr {
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
