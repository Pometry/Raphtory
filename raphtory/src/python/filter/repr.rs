//! The Python form of a filter: the expression a user would write to build it
//! again, module-qualified so `eval` rebuilds it after `import raphtory`.

use crate::{
    db::graph::views::filter::model::{
        expr::{Agg, EdgeLeaf, ExplodedEdgeLeaf, Expr, Field, FilterExpr, NodeLeaf, ViewOp},
        filter_operator::{BinaryOp, StringOp},
    },
    python::filter::node_expr::Typed,
};
use pyo3::{prelude::*, types::PyString};
use raphtory_api::core::{
    entities::properties::prop::Prop,
    storage::timeindex::{AsTime, EventTime},
    Direction,
};

const MODULE: &str = "raphtory.filter";

/// What a rendered piece is, for the caller that puts it inside something else:
/// a chain (`raphtory.filter.Node.property('p').sum()`, or a `~` negation, which
/// binds tighter than `&` and `|`) can take a method call or an operator as it
/// is; a comparison or an `&`/`|` combination needs parentheses.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// A dotted chain: safe as an operand and as a receiver.
    Chain,
    /// An operator expression: bracketed as an operand and as a receiver.
    Compound,
    /// A `~` expression: safe as an operand, bracketed as a receiver, since
    /// `~a.b()` is `~(a.b())` in Python.
    Unary,
}

struct Rendered {
    text: String,
    shape: Shape,
}

impl Rendered {
    fn chain(text: String) -> Self {
        Rendered {
            text,
            shape: Shape::Chain,
        }
    }

    fn compound(text: String) -> Self {
        Rendered {
            text,
            shape: Shape::Compound,
        }
    }

    fn unary(text: String) -> Self {
        Rendered {
            text,
            shape: Shape::Unary,
        }
    }

    /// The text as an operand of `&`, `|` or a comparison: parenthesised
    /// unless it is a chain or a `~`.
    fn atom(&self) -> String {
        match self.shape {
            Shape::Chain | Shape::Unary => self.text.clone(),
            Shape::Compound => format!("({})", self.text),
        }
    }

    /// The text as the receiver of a method call: parenthesised unless it is
    /// a chain.
    fn receiver(&self) -> String {
        match self.shape {
            Shape::Chain => self.text.clone(),
            Shape::Compound | Shape::Unary => format!("({})", self.text),
        }
    }
}

pub(crate) fn typed(py: Python<'_>, expr: &Typed) -> PyResult<String> {
    Ok(match expr {
        Typed::Node(e) => render(py, e, Entity::Node)?.text,
        Typed::Edge(e) => render(py, e, Entity::Edge)?.text,
        Typed::ExplodedEdge(e) => render(py, e, Entity::ExplodedEdge)?.text,
    })
}

pub(crate) fn filter(py: Python<'_>, filter: &FilterExpr) -> PyResult<String> {
    Ok(render_filter(py, filter)?.text)
}

/// A factory: an entity root seen through `views`.
pub(crate) fn factory(py: Python<'_>, root: &str, views: &[ViewOp]) -> PyResult<String> {
    Ok(format!("{MODULE}.{root}{}", view_chain(py, views)?))
}

#[derive(Clone, Copy)]
enum Entity {
    Node,
    Edge,
    ExplodedEdge,
}

impl Entity {
    fn root(self) -> &'static str {
        match self {
            Entity::Node => "Node",
            Entity::Edge => "Edge",
            Entity::ExplodedEdge => "ExplodedEdge",
        }
    }
}

/// Every term renders the same way: the entity root, its views, then the term itself.
trait Leaf {
    fn term(&self, py: Python<'_>, entity: Entity) -> PyResult<String>;
}

fn render_filter(py: Python<'_>, filter: &FilterExpr) -> PyResult<Rendered> {
    Ok(match filter {
        FilterExpr::Node(e) => render(py, e, Entity::Node)?,
        FilterExpr::Edge(e) => render(py, e, Entity::Edge)?,
        FilterExpr::ExplodedEdge(e) => render(py, e, Entity::ExplodedEdge)?,
        FilterExpr::View(ops) => Rendered::chain(factory(py, "Graph", ops)?),
        FilterExpr::And(items) => combined(
            flat(items, |i| match i {
                FilterExpr::And(inner) => Some(inner),
                _ => None,
            })
            .into_iter()
            .map(|i| render_filter(py, i)),
            " & ",
        )?,
        FilterExpr::Or(items) => combined(
            flat(items, |i| match i {
                FilterExpr::Or(inner) => Some(inner),
                _ => None,
            })
            .into_iter()
            .map(|i| render_filter(py, i)),
            " | ",
        )?,
        FilterExpr::Not(e) => Rendered::unary(format!("~{}", render_filter(py, e)?.atom())),
        FilterExpr::Opaque(_) => Rendered::chain("<a filter with no Python form>".to_owned()),
    })
}

fn render<L: Leaf>(py: Python<'_>, expr: &Expr<L>, entity: Entity) -> PyResult<Rendered> {
    Ok(match expr {
        Expr::Const(v) => Rendered::chain(literal(py, v)?),
        Expr::Term(leaf) => Rendered::chain(leaf.term(py, entity)?),
        Expr::Agg(agg, e) => {
            let name = match agg {
                Agg::Sum => "sum",
                Agg::Avg => "avg",
                Agg::Min => "min",
                Agg::Max => "max",
                Agg::First => "first",
                Agg::Last => "last",
                Agg::Len => "len",
                Agg::Earliest => "earliest",
                Agg::Latest => "latest",
            };
            Rendered::chain(format!("{}.{name}()", render(py, e, entity)?.receiver()))
        }
        Expr::Cmp(op, l, r) => {
            let sym = match op {
                BinaryOp::Eq => "==",
                BinaryOp::Ne => "!=",
                BinaryOp::Lt => "<",
                BinaryOp::Le => "<=",
                BinaryOp::Gt => ">",
                BinaryOp::Ge => ">=",
            };
            let l = render(py, l, entity)?.atom();
            let r = render(py, r, entity)?.atom();
            Rendered::compound(format!("{l} {sym} {r}"))
        }
        Expr::Str(op, l, r) => {
            let l = render(py, l, entity)?.atom();
            let r = render(py, r, entity)?.atom();
            Rendered::chain(match op {
                StringOp::StartsWith => format!("{l}.starts_with({r})"),
                StringOp::EndsWith => format!("{l}.ends_with({r})"),
                StringOp::Contains => format!("{l}.contains({r})"),
                StringOp::NotContains => format!("{l}.not_contains({r})"),
                StringOp::FuzzySearch {
                    levenshtein_distance,
                    prefix_match,
                } => format!(
                    "{l}.fuzzy_search({r}, {levenshtein_distance}, {})",
                    py_bool(*prefix_match)
                ),
            })
        }
        Expr::In {
            expr,
            values,
            negated,
        } => {
            let items = values
                .iter()
                .map(|v| literal(py, v))
                .collect::<PyResult<Vec<_>>>()?
                .join(", ");
            let name = if *negated { "is_not_in" } else { "is_in" };
            Rendered::chain(format!(
                "{}.{name}([{items}])",
                render(py, expr, entity)?.receiver()
            ))
        }
        Expr::IsSome(e) => {
            Rendered::chain(format!("{}.is_some()", render(py, e, entity)?.receiver()))
        }
        Expr::IsNone(e) => {
            Rendered::chain(format!("{}.is_none()", render(py, e, entity)?.receiver()))
        }
        Expr::Any(e) => Rendered::chain(format!("{}.any()", render(py, e, entity)?.receiver())),
        Expr::All(e) => Rendered::chain(format!("{}.all()", render(py, e, entity)?.receiver())),
        Expr::And(items) => combined(
            flat(items, |i| match i {
                Expr::And(inner) => Some(inner),
                _ => None,
            })
            .into_iter()
            .map(|i| render(py, i, entity)),
            " & ",
        )?,
        Expr::Or(items) => combined(
            flat(items, |i| match i {
                Expr::Or(inner) => Some(inner),
                _ => None,
            })
            .into_iter()
            .map(|i| render(py, i, entity)),
            " | ",
        )?,
        Expr::Not(e) => Rendered::unary(format!("~{}", render(py, e, entity)?.atom())),
    })
}

/// The operands of a combination, with any nested combination of the same
/// operator opened up, so `a & b & c` reads back the way it was written.
fn flat<'a, T>(items: &'a [T], same: impl Fn(&'a T) -> Option<&'a Vec<T>> + Copy) -> Vec<&'a T> {
    items
        .iter()
        .flat_map(|item| match same(item) {
            Some(inner) => flat(inner, same),
            None => vec![item],
        })
        .collect()
}

fn combined(items: impl Iterator<Item = PyResult<Rendered>>, sep: &str) -> PyResult<Rendered> {
    let parts = items
        .map(|item| item.map(|r| r.atom()))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(Rendered::compound(parts.join(sep)))
}

impl Leaf for NodeLeaf {
    fn term(&self, py: Python<'_>, entity: Entity) -> PyResult<String> {
        let (views, tail) = node_tail(py, self)?;
        Ok(format!("{}{tail}", factory(py, entity.root(), views)?))
    }
}

/// The views a node term carries and the call that reads it, apart, because an
/// endpoint term puts `.src()` between them.
fn node_tail<'a>(py: Python<'_>, leaf: &'a NodeLeaf) -> PyResult<(&'a [ViewOp], String)> {
    Ok(match leaf {
        NodeLeaf::Field { views, field } => {
            let name = match field {
                Field::Id => "id",
                Field::Name => "name",
                Field::NodeType => "node_type",
            };
            (views, format!(".{name}()"))
        }
        NodeLeaf::Degree { views, direction } => {
            let name = match direction {
                Direction::BOTH => "degree",
                Direction::IN => "in_degree",
                Direction::OUT => "out_degree",
            };
            (views, format!(".{name}()"))
        }
        NodeLeaf::Property {
            views,
            name,
            temporal,
        } => (views, property(py, name, *temporal)?),
        NodeLeaf::Metadata { views, name } => (views, format!(".metadata({})", py_str(py, name)?)),
        NodeLeaf::IsActive { views } => (views, ".is_active()".to_owned()),
    })
}

impl Leaf for EdgeLeaf {
    fn term(&self, py: Python<'_>, entity: Entity) -> PyResult<String> {
        let root = entity.root();
        Ok(match self {
            EdgeLeaf::Property {
                views,
                name,
                temporal,
            } => format!(
                "{}{}",
                factory(py, root, views)?,
                property(py, name, *temporal)?
            ),
            EdgeLeaf::Metadata { views, name } => {
                format!(
                    "{}.metadata({})",
                    factory(py, root, views)?,
                    py_str(py, name)?
                )
            }
            EdgeLeaf::IsActive { views } => format!("{}.is_active()", factory(py, root, views)?),
            EdgeLeaf::IsValid { views } => format!("{}.is_valid()", factory(py, root, views)?),
            EdgeLeaf::IsDeleted { views } => format!("{}.is_deleted()", factory(py, root, views)?),
            EdgeLeaf::IsSelfLoop { views } => {
                format!("{}.is_self_loop()", factory(py, root, views)?)
            }
            EdgeLeaf::Src(inner) => endpoint(py, root, "src", inner)?,
            EdgeLeaf::Dst(inner) => endpoint(py, root, "dst", inner)?,
        })
    }
}

/// `Edge.src()` reads a node field or property; the views on that term are the
/// edge factory's, so they sit before `.src()`.
fn endpoint(py: Python<'_>, root: &str, end: &str, inner: &Expr<NodeLeaf>) -> PyResult<String> {
    Ok(match inner {
        Expr::Term(leaf) => {
            let (views, tail) = node_tail(py, leaf)?;
            format!("{}.{end}(){tail}", factory(py, root, views)?)
        }
        other => format!(
            "{}.{end}()<{}>",
            factory(py, root, &[])?,
            render(py, other, Entity::Node)?.text
        ),
    })
}

impl Leaf for ExplodedEdgeLeaf {
    fn term(&self, py: Python<'_>, entity: Entity) -> PyResult<String> {
        let root = entity.root();
        Ok(match self {
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal,
            } => format!(
                "{}{}",
                factory(py, root, views)?,
                property(py, name, *temporal)?
            ),
            ExplodedEdgeLeaf::Metadata { views, name } => {
                format!(
                    "{}.metadata({})",
                    factory(py, root, views)?,
                    py_str(py, name)?
                )
            }
            ExplodedEdgeLeaf::IsActive { views } => {
                format!("{}.is_active()", factory(py, root, views)?)
            }
            ExplodedEdgeLeaf::IsValid { views } => {
                format!("{}.is_valid()", factory(py, root, views)?)
            }
            ExplodedEdgeLeaf::IsDeleted { views } => {
                format!("{}.is_deleted()", factory(py, root, views)?)
            }
            ExplodedEdgeLeaf::IsSelfLoop { views } => {
                format!("{}.is_self_loop()", factory(py, root, views)?)
            }
        })
    }
}

fn property(py: Python<'_>, name: &str, temporal: bool) -> PyResult<String> {
    let temporal = if temporal { ".temporal()" } else { "" };
    Ok(format!(".property({}){temporal}", py_str(py, name)?))
}

fn view_chain(py: Python<'_>, views: &[ViewOp]) -> PyResult<String> {
    views
        .iter()
        .map(|op| {
            Ok(match op {
                ViewOp::Window { start, end } => {
                    format!(".window({}, {})", time(start), time(end))
                }
                ViewOp::At(t) => format!(".at({})", time(t)),
                ViewOp::After(t) => format!(".after({})", time(t)),
                ViewOp::Before(t) => format!(".before({})", time(t)),
                ViewOp::Latest => ".latest()".to_owned(),
                ViewOp::SnapshotAt(t) => format!(".snapshot_at({})", time(t)),
                ViewOp::SnapshotLatest => ".snapshot_latest()".to_owned(),
                ViewOp::Layers(names) => match names.as_slice() {
                    [name] => format!(".layer({})", py_str(py, name)?),
                    names => format!(
                        ".layers([{}])",
                        names
                            .iter()
                            .map(|n| py_str(py, n))
                            .collect::<PyResult<Vec<_>>>()?
                            .join(", ")
                    ),
                },
            })
        })
        .collect::<PyResult<Vec<_>>>()
        .map(|parts| parts.concat())
}

/// A time the way a user passes it: the timestamp, or a `(t, event_id)` pair
/// when the event id matters.
fn time(t: &EventTime) -> String {
    match t.i() {
        0 => t.t().to_string(),
        i => format!("({}, {i})", t.t()),
    }
}

/// A constant as Python source. A list is written as a list literal, since
/// the Python value it becomes is an array whose `repr` needs numpy.
fn literal(py: Python<'_>, value: &Prop) -> PyResult<String> {
    match value {
        Prop::List(items) => {
            let items = items
                .iter()
                .map(|item| literal(py, &item))
                .collect::<PyResult<Vec<_>>>()?
                .join(", ");
            Ok(format!("[{items}]"))
        }
        other => Ok(other.into_pyobject(py)?.repr()?.to_string()),
    }
}

fn py_str(py: Python<'_>, s: &str) -> PyResult<String> {
    Ok(PyString::new(py, s).repr()?.to_string())
}

fn py_bool(b: bool) -> &'static str {
    if b {
        "True"
    } else {
        "False"
    }
}
