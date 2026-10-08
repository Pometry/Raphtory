use crate::{
    db::graph::views::filter::model::{
        expr::{
            builder::{EntityAggOps, NodeFilterFactory},
            Chain, EdgeExpr, EdgeLeaf, ExplodedEdgeExpr, ExplodedEdgeLeaf, Expr, FilterExpr, Leaf,
            NodeExpr, NodeLeaf, PropertyTerm,
        },
        filter_operator::{BinaryOp, StringOp},
        node_expr::{
            typing::{is_known, qualified_type},
            DynCreateOp,
        },
        node_filter::NodeFilter,
        validate_const_comparable, EntityExprFilterOps, PropertyExprFactory, ViewWrapOps,
    },
    errors::GraphError,
    python::{
        filter::{
            filter_expr::{ExprOrFilter, PyFilterExpr},
            repr,
        },
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
    entities::{
        properties::prop::{Prop, PropType},
        GID,
    },
    storage::timeindex::EventTime,
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

    /// The filter this yes/no expression is, on its entity.
    pub(crate) fn into_filter(self) -> FilterExpr {
        match self {
            Typed::Node(e) => NodeLeaf::filter(e),
            Typed::Edge(e) => EdgeLeaf::filter(e),
            Typed::ExplodedEdge(e) => ExplodedEdgeLeaf::filter(e),
        }
    }
}

impl From<NodeExpr> for Typed {
    fn from(expr: NodeExpr) -> Self {
        Typed::Node(expr)
    }
}

impl From<EdgeExpr> for Typed {
    fn from(expr: EdgeExpr) -> Self {
        Typed::Edge(expr)
    }
}

impl From<ExplodedEdgeExpr> for Typed {
    fn from(expr: ExplodedEdgeExpr) -> Self {
        Typed::ExplodedEdge(expr)
    }
}

/// A value expression: a field, degree, property, metadata entry, an aggregate
/// over one, or a yes/no built from them. Comparing it to a value or to another
/// expression gives a yes/no `Expr`, which is a filter on its entity.
///
/// `~` on a yes/no is the opposite yes/no: a node without the property fails
/// `property("score") > 4`, so it passes `~(property("score") > 4)`.
#[pyclass(
    frozen,
    subclass,
    name = "Expr",
    module = "raphtory.filter",
    from_py_object
)]
#[derive(Clone)]
pub struct PyExpr(pub(crate) Typed);

/// A property term, which can switch to the property's history with `temporal()`.
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

/// Both forms of the property term, as the Rust builder made them.
impl<L: Leaf> From<PropertyTerm<L>> for PyPropertyExpr
where
    Typed: From<Expr<L>>,
{
    fn from(term: PropertyTerm<L>) -> Self {
        PyPropertyExpr {
            history: term.temporal().into(),
            latest: Expr::from(term).into(),
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

/// `any()`/`all()` only follow a comparison that gives one yes/no per
/// element. When the expression's type is already known and is not that, the
/// qualifier is refused where it is written; otherwise it is checked when the
/// filter is applied.
fn check_qualifiable(inner: &Typed) -> PyResult<()> {
    let pt = static_type(inner)?;
    if is_known(&pt) {
        qualified_type(&pt).map_err(|e| PyTypeError::new_err(e.to_string()))?;
    }
    Ok(())
}

/// String operators require a string operand whatever the lhs type.
fn check_str_value(v: &Prop) -> PyResult<()> {
    validate_const_comparable(&PropType::Str, Some(v))
        .map_err(|e| PyTypeError::new_err(e.to_string()))
}

impl PyExpr {
    fn compare(&self, op: BinaryOp, other: ExprOrValue) -> PyResult<PyExpr> {
        match other {
            ExprOrValue::Expr(e) => {
                Ok(PyExpr(zip_typed!(self.0.clone(), e.0, |l, r| l.cmp(op, r))?))
            }
            ExprOrValue::Value(v) => {
                check_value(&self.0, &v)?;
                Ok(PyExpr(map_typed!(self.0.clone(), |e| e.cmp(op, v))))
            }
        }
    }

    fn string_op(&self, op: StringOp, other: ExprOrValue) -> PyResult<PyExpr> {
        match other {
            ExprOrValue::Expr(e) => Ok(PyExpr(
                zip_typed!(self.0.clone(), e.0, |l, r| l.string(op, r))?
            )),
            ExprOrValue::Value(v) => {
                check_str_value(&v)?;
                Ok(PyExpr(map_typed!(self.0.clone(), |e| e.string(op, v))))
            }
        }
    }

    fn presence(&self, none: bool, name: &str) -> PyResult<PyExpr> {
        check_nullable(&self.0, name)?;
        Ok(PyExpr(map_typed!(self.0.clone(), |e| if none {
            e.is_none()
        } else {
            e.is_some()
        })))
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
            FilterExpr::Or(vec![mine, other])
        };
        PyFilterExpr(filter).into_bound_py_any(py)
    }
}

#[pymethods]
impl PyExpr {
    fn __eq__(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(BinaryOp::Eq, other)
    }

    fn __ne__(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(BinaryOp::Ne, other)
    }

    fn __lt__(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(BinaryOp::Lt, other)
    }

    fn __le__(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(BinaryOp::Le, other)
    }

    fn __gt__(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(BinaryOp::Gt, other)
    }

    fn __ge__(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(BinaryOp::Ge, other)
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
        self.compare(BinaryOp::Eq, other)
    }

    /// `self != other`, as a method.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): The value or expression to compare with.
    ///
    /// Returns:
    ///     filter.Expr:
    fn ne(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(BinaryOp::Ne, other)
    }

    /// `self < other`, as a method.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): The value or expression to compare with.
    ///
    /// Returns:
    ///     filter.Expr:
    fn lt(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(BinaryOp::Lt, other)
    }

    /// `self <= other`, as a method.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): The value or expression to compare with.
    ///
    /// Returns:
    ///     filter.Expr:
    fn le(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(BinaryOp::Le, other)
    }

    /// `self > other`, as a method.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): The value or expression to compare with.
    ///
    /// Returns:
    ///     filter.Expr:
    fn gt(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(BinaryOp::Gt, other)
    }

    /// `self >= other`, as a method.
    ///
    /// Arguments:
    ///     other (Prop | filter.Expr): The value or expression to compare with.
    ///
    /// Returns:
    ///     filter.Expr:
    fn ge(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.compare(BinaryOp::Ge, other)
    }

    /// Checks whether the string value starts with the given prefix.
    ///
    /// Arguments:
    ///     other (str | filter.Expr): The prefix, or an expression giving it.
    ///
    /// Returns:
    ///     filter.Expr:
    fn starts_with(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.string_op(StringOp::StartsWith, other)
    }

    /// Checks whether the string value ends with the given suffix.
    ///
    /// Arguments:
    ///     other (str | filter.Expr): The suffix, or an expression giving it.
    ///
    /// Returns:
    ///     filter.Expr:
    fn ends_with(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.string_op(StringOp::EndsWith, other)
    }

    /// Checks whether the string value contains the given substring.
    ///
    /// Arguments:
    ///     other (str | filter.Expr): The substring, or an expression giving it.
    ///
    /// Returns:
    ///     filter.Expr:
    fn contains(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.string_op(StringOp::Contains, other)
    }

    /// Checks whether the string value does **not** contain the given substring.
    ///
    /// Arguments:
    ///     other (str | filter.Expr): The substring, or an expression giving it.
    ///
    /// Returns:
    ///     filter.Expr:
    fn not_contains(&self, other: ExprOrValue) -> PyResult<PyExpr> {
        self.string_op(StringOp::NotContains, other)
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
            StringOp::FuzzySearch {
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
        PyExpr(map_typed!(self.0.clone(), |e| e.is_in(values)))
    }

    /// Checks whether the value is **not** contained within the given values.
    ///
    /// Arguments:
    ///     values (list[Prop]): Values to exclude.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_not_in(&self, values: FromIterable<Prop>) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| e.is_not_in(values)))
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
    fn any(&self) -> PyResult<PyExpr> {
        check_qualifiable(&self.0)?;
        Ok(PyExpr(map_typed!(self.0.clone(), |e| e.any())))
    }

    /// Requires that **all** elements match. Follows a comparison against a
    /// list-like value (a temporal history or a list property):
    /// `(filter.Node.property("p").temporal() > 4).all()`.
    ///
    /// Returns:
    ///     filter.Expr:
    fn all(&self) -> PyResult<PyExpr> {
        check_qualifiable(&self.0)?;
        Ok(PyExpr(map_typed!(self.0.clone(), |e| e.all())))
    }

    /// Sums the elements when the value is numeric and list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn sum(&self) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| e.sum()))
    }

    /// Averages the elements when the value is numeric and list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn avg(&self) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| e.avg()))
    }

    /// Selects the minimum element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn min(&self) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| e.min()))
    }

    /// Selects the maximum element when the value is list-like.
    ///
    /// Returns:
    ///     filter.Expr:
    fn max(&self) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| e.max()))
    }

    /// Selects the first element of each innermost list. On the history of a
    /// list-valued property that is one answer per update; `earliest()` picks
    /// the first update instead.
    ///
    /// Returns:
    ///     filter.Expr:
    fn first(&self) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| e.first()))
    }

    /// Selects the last element of each innermost list. On the history of a
    /// list-valued property that is one answer per update; `latest()` picks
    /// the last update instead.
    ///
    /// Returns:
    ///     filter.Expr:
    fn last(&self) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| e.last()))
    }

    /// Selects the number of elements of each innermost list.
    ///
    /// Returns:
    ///     filter.Expr:
    fn len(&self) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| e.len()))
    }

    /// The earliest update of a temporal history, whatever its type: on a
    /// list-valued property that is the whole first list.
    ///
    /// Returns:
    ///     filter.Expr:
    fn earliest(&self) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| e.earliest()))
    }

    /// The latest update of a temporal history, whatever its type: on a
    /// list-valued property that is the whole last list.
    ///
    /// Returns:
    ///     filter.Expr:
    fn latest(&self) -> PyExpr {
        PyExpr(map_typed!(self.0.clone(), |e| e.latest()))
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

    /// The Python expression that builds this one, module-qualified, so `eval`
    /// rebuilds it after `import raphtory`.
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        repr::typed(py, &self.0)
    }
}

#[pymethods]
impl PyPropertyExpr {
    /// Switches from the property's latest value to its full history, a list
    /// that the aggregates (`sum`, `avg`, `min`, `max`, ...) reduce and that a
    /// comparison tests element by element, for `any()` / `all()` to collapse.
    ///
    /// Returns:
    ///     filter.Expr:
    fn temporal(&self) -> PyExpr {
        PyExpr(self.history.clone())
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        repr::typed(py, &self.latest)
    }
}

/// A node filter scoped to a view.
///
/// Obtained from the view methods on `Node` (`Node.window(...)`,
/// `Node.latest()`, ...); its field and property methods evaluate within that
/// view, and its own view methods narrow it further.
#[pyclass(frozen, name = "NodeFilter", module = "raphtory.filter")]
pub struct PyNodeFilter(pub(crate) Chain<NodeLeaf>);

impl PyNodeFilter {
    pub(crate) fn root() -> Self {
        PyNodeFilter(NodeFilter.into())
    }
}

#[pymethods]
impl PyNodeFilter {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        repr::factory(py, "Node", self.0.views())
    }

    /// Selects the node ID field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn id(&self) -> PyExpr {
        PyExpr(self.0.id().into())
    }

    /// Selects the node name field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn name(&self) -> PyExpr {
        PyExpr(self.0.name().into())
    }

    /// Selects the node type field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn node_type(&self) -> PyExpr {
        PyExpr(self.0.node_type().into())
    }

    /// Selects incoming node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn in_degree(&self) -> PyExpr {
        PyExpr(self.0.in_degree().into())
    }

    /// Selects total node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn degree(&self) -> PyExpr {
        PyExpr(self.0.degree().into())
    }

    /// Selects outgoing node degree for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn out_degree(&self) -> PyExpr {
        PyExpr(self.0.out_degree().into())
    }

    /// Filters a node property by name.
    ///
    /// Reads the property's latest value; `temporal()` switches to its history.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    fn property(&self, name: String) -> PyPropertyExpr {
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
    ///     filter.Expr:
    fn metadata(&self, name: String) -> PyExpr {
        PyExpr(self.0.metadata(name).into())
    }

    /// Restricts node evaluation to the given time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (TimeInput): Start time.
    ///     end (TimeInput): End time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn window(&self, start: EventTime, end: EventTime) -> PyNodeFilter {
        Self(self.0.clone().window(start, end))
    }

    /// Restricts node evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (TimeInput): Event time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn at(&self, time: EventTime) -> PyNodeFilter {
        Self(self.0.clone().at(time))
    }

    /// Restricts node evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Lower time bound.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn after(&self, time: EventTime) -> PyNodeFilter {
        Self(self.0.clone().after(time))
    }

    /// Restricts node evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Upper time bound.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn before(&self, time: EventTime) -> PyNodeFilter {
        Self(self.0.clone().before(time))
    }

    /// Evaluates filters against the latest available state of each node.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn latest(&self) -> PyNodeFilter {
        Self(self.0.clone().latest())
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Snapshot time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn snapshot_at(&self, time: EventTime) -> PyNodeFilter {
        Self(self.0.clone().snapshot_at(time))
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn snapshot_latest(&self) -> PyNodeFilter {
        Self(self.0.clone().snapshot_latest())
    }

    /// Reads through a view of the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn layer(&self, layer: String) -> PyNodeFilter {
        Self(self.0.clone().layer(layer))
    }

    /// Restricts evaluation to nodes belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn layers(&self, layers: FromIterable<String>) -> PyNodeFilter {
        Self(self.0.clone().layer(Vec::<String>::from(layers)))
    }

    /// Reads through a view of the default layer only.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn default_layer(&self) -> PyNodeFilter {
        Self(self.0.clone().default_layer())
    }

    /// Reads through a view of every layer except the given one.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn exclude_layer(&self, layer: String) -> PyNodeFilter {
        Self(self.0.clone().exclude_layer(layer))
    }

    /// Reads through a view of every layer except the given ones.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn exclude_layers(&self, layers: FromIterable<String>) -> PyNodeFilter {
        Self(self.0.clone().exclude_layers(layers))
    }

    /// Moves the start of the current window to `start` when that is later.
    ///
    /// The window only ever narrows: a start before the current one changes nothing.
    ///
    /// Arguments:
    ///     start (TimeInput): New start time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn shrink_start(&self, start: EventTime) -> PyNodeFilter {
        Self(self.0.clone().shrink_start(start))
    }

    /// Moves the end of the current window to `end` when that is earlier.
    ///
    /// The window only ever narrows: an end after the current one changes nothing.
    ///
    /// Arguments:
    ///     end (TimeInput): New end time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn shrink_end(&self, end: EventTime) -> PyNodeFilter {
        Self(self.0.clone().shrink_end(end))
    }

    /// Reads through a view of every node except the given ones, with their edges.
    ///
    /// An id the view does not hold changes nothing.
    ///
    /// Arguments:
    ///     nodes (list[str | int]): Node ids or names.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn exclude_nodes(&self, nodes: FromIterable<GID>) -> PyNodeFilter {
        Self(self.0.clone().exclude_nodes(nodes))
    }

    /// Reads through a view of the given nodes and the edges between them.
    ///
    /// An id the view does not hold is skipped.
    ///
    /// Arguments:
    ///     nodes (list[str | int]): Node ids or names.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn subgraph(&self, nodes: FromIterable<GID>) -> PyNodeFilter {
        Self(self.0.clone().subgraph(nodes))
    }

    /// Reads through a view of the nodes of the given types and the edges between them.
    ///
    /// Arguments:
    ///     node_types (list[str]): Node types.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn subgraph_node_types(&self, node_types: FromIterable<String>) -> PyNodeFilter {
        Self(self.0.clone().subgraph_node_types(node_types))
    }

    /// Reads through a view of the edges that are valid in the current view.
    ///
    /// On a persistent graph an edge is valid when its last update is an addition;
    /// on an event graph when it has at least one addition. Nodes are untouched.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    fn valid(&self) -> PyNodeFilter {
        Self(self.0.clone().valid())
    }

    /// Matches nodes that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_active(&self) -> PyExpr {
        PyExpr(self.0.is_active().into())
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
        let filter = NodeFilter::by_column(&state.inner, &col)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyFilterExpr(filter))
    }
}

/// Entry point for constructing node filter expressions.
///
/// Every method is static: `Node.property("age") > 30` selects nodes
/// directly, and the view methods (`window`, `latest`, `layer`, ...) return a
/// `NodeFilter` scoped to that view for further chaining.
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
    /// Reads the property's latest value; `temporal()` switches to its history.
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
    ///     start (TimeInput): Start time.
    ///     end (TimeInput): End time.
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
    ///     time (TimeInput): Event time.
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
    ///     time (TimeInput): Lower time bound.
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
    ///     time (TimeInput): Upper time bound.
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
    ///     time (TimeInput): Snapshot time.
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

    /// Reads through a view of the given layer.
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

    /// Reads through a view of the default layer only.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn default_layer() -> PyNodeFilter {
        PyNodeFilter::root().default_layer()
    }

    /// Reads through a view of every layer except the given one.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn exclude_layer(layer: String) -> PyNodeFilter {
        PyNodeFilter::root().exclude_layer(layer)
    }

    /// Reads through a view of every layer except the given ones.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn exclude_layers(layers: FromIterable<String>) -> PyNodeFilter {
        PyNodeFilter::root().exclude_layers(layers)
    }

    /// Moves the start of the current window to `start` when that is later.
    ///
    /// The window only ever narrows: a start before the current one changes nothing.
    ///
    /// Arguments:
    ///     start (TimeInput): New start time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn shrink_start(start: EventTime) -> PyNodeFilter {
        PyNodeFilter::root().shrink_start(start)
    }

    /// Moves the end of the current window to `end` when that is earlier.
    ///
    /// The window only ever narrows: an end after the current one changes nothing.
    ///
    /// Arguments:
    ///     end (TimeInput): New end time.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn shrink_end(end: EventTime) -> PyNodeFilter {
        PyNodeFilter::root().shrink_end(end)
    }

    /// Reads through a view of every node except the given ones, with their edges.
    ///
    /// An id the view does not hold changes nothing.
    ///
    /// Arguments:
    ///     nodes (list[str | int]): Node ids or names.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn exclude_nodes(nodes: FromIterable<GID>) -> PyNodeFilter {
        PyNodeFilter::root().exclude_nodes(nodes)
    }

    /// Reads through a view of the given nodes and the edges between them.
    ///
    /// An id the view does not hold is skipped.
    ///
    /// Arguments:
    ///     nodes (list[str | int]): Node ids or names.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn subgraph(nodes: FromIterable<GID>) -> PyNodeFilter {
        PyNodeFilter::root().subgraph(nodes)
    }

    /// Reads through a view of the nodes of the given types and the edges between them.
    ///
    /// Arguments:
    ///     node_types (list[str]): Node types.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn subgraph_node_types(node_types: FromIterable<String>) -> PyNodeFilter {
        PyNodeFilter::root().subgraph_node_types(node_types)
    }

    /// Reads through a view of the edges that are valid in the current view.
    ///
    /// On a persistent graph an edge is valid when its last update is an addition;
    /// on an event graph when it has at least one addition. Nodes are untouched.
    ///
    /// Returns:
    ///     filter.NodeFilter:
    #[staticmethod]
    fn valid() -> PyNodeFilter {
        PyNodeFilter::root().valid()
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
