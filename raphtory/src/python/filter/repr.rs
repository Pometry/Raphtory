//! The Python form of a filter: the expression a user would write to build it
//! again, module-qualified so `eval` rebuilds it after `import raphtory`.
//! Every value inside it is the `Repr` of the Python object it converts to.

use crate::{
    db::graph::views::filter::model::{
        expr::{EdgeLeaf, ExplodedEdgeLeaf, Expr, Field, FilterExpr, NodeLeaf, ViewOp},
        filter_operator::StringOp,
    },
    python::{filter::node_expr::Typed, types::repr::Repr},
};
use pyo3::{prelude::*, IntoPyObjectExt};
use raphtory_api::core::{
    entities::{properties::prop::Prop, Layer},
    storage::timeindex::{AsTime, EventTime},
    Direction,
};

const MODULE: &str = "raphtory.filter";

/// What a value built in this process renders as.
const COMPILED: &str = "<compiled>";

/// What a rendered piece is, for the caller that puts it inside something else:
/// a chain (`raphtory.filter.Node.property('p').sum()`, or a `~` negation, which
/// binds tighter than `&` and `|`) can take a method call or an operator as it
/// is; a comparison or an `&`/`|` combination needs parentheses.
#[derive(Clone, Copy)]
enum Shape {
    /// A dotted chain: safe as an operand and as a receiver.
    Chain,
    /// An operator expression: bracketed as an operand and as a receiver.
    Compound,
    /// A `~` expression: safe as an operand, bracketed as a receiver, since
    /// `~a.b()` is `~(a.b())` in Python.
    Unary,
}

struct Rendered(String, Shape);

impl Rendered {
    /// The text as an operand of `&`, `|` or a comparison: parenthesised
    /// unless it is a chain or a `~`.
    fn atom(self) -> String {
        match self.1 {
            Shape::Chain | Shape::Unary => self.0,
            Shape::Compound => format!("({})", self.0),
        }
    }

    /// The text as the receiver of a method call: parenthesised unless it is
    /// a chain.
    fn receiver(self) -> String {
        match self.1 {
            Shape::Chain => self.0,
            Shape::Compound | Shape::Unary => format!("({})", self.0),
        }
    }
}

/// A value as Python source: the `Repr` of the Python object it converts to.
fn python<T: for<'py> IntoPyObject<'py>>(value: T) -> String {
    Python::attach(|py| value.into_bound_py_any(py).repr())
}

/// A constant as Python source. A list is written as a list literal, since
/// the Python value it becomes is an array whose `repr` needs numpy.
fn literal(value: &Prop) -> String {
    match value {
        Prop::List(items) => items
            .iter()
            .map(|item| literal(&item))
            .collect::<Vec<_>>()
            .repr(),
        other => python(other.clone()),
    }
}

/// A time the way a user passes it: the timestamp, or a `(t, event_id)` pair
/// when the event id matters.
fn time(t: &EventTime) -> String {
    match t.i() {
        0 => t.t().repr(),
        i => (t.t(), i).repr(),
    }
}

/// A factory: an entity root seen through `views`.
pub(crate) fn factory(root: &str, views: &[ViewOp]) -> String {
    let views: String = views.iter().map(Repr::repr).collect();
    format!("{MODULE}.{root}{views}")
}

impl Repr for Typed {
    fn repr(&self) -> String {
        match self {
            Typed::Node(e) => e.repr(),
            Typed::Edge(e) => e.repr(),
            Typed::ExplodedEdge(e) => e.repr(),
        }
    }
}

impl Repr for FilterExpr {
    fn repr(&self) -> String {
        render_filter(self).0
    }
}

impl<L: Repr> Repr for Expr<L> {
    fn repr(&self) -> String {
        render(self).0
    }
}

fn render_filter(filter: &FilterExpr) -> Rendered {
    match filter {
        FilterExpr::Node(e) => render(e),
        FilterExpr::Edge(e) => render(e),
        FilterExpr::ExplodedEdge(e) => render(e),
        FilterExpr::View(ops) => Rendered(factory("Graph", ops), Shape::Chain),
        FilterExpr::And(items) => combined(
            flat(items, |i| match i {
                FilterExpr::And(inner) => Some(inner),
                _ => None,
            }),
            render_filter,
            " & ",
        ),
        FilterExpr::Or(items) => combined(
            flat(items, |i| match i {
                FilterExpr::Or(inner) => Some(inner),
                _ => None,
            }),
            render_filter,
            " | ",
        ),
        FilterExpr::Not(e) => Rendered(format!("~{}", render_filter(e).atom()), Shape::Unary),
    }
}

fn render<L: Repr>(expr: &Expr<L>) -> Rendered {
    let call = |e: &Expr<L>, call: &str| format!("{}.{call}", render(e).receiver());
    let text = match expr {
        Expr::Const(v) => literal(v),
        Expr::Term(leaf) => leaf.repr(),
        Expr::Agg(agg, e) => call(e, &format!("{}()", agg.name())),
        Expr::Cmp(op, l, r) => {
            let text = format!("{} {op} {}", render(l).atom(), render(r).atom());
            return Rendered(text, Shape::Compound);
        }
        Expr::Str(op, l, r) => {
            let (l, r) = (render(l).atom(), render(r).atom());
            match op {
                StringOp::StartsWith => format!("{l}.starts_with({r})"),
                StringOp::EndsWith => format!("{l}.ends_with({r})"),
                StringOp::Contains => format!("{l}.contains({r})"),
                StringOp::NotContains => format!("{l}.not_contains({r})"),
                StringOp::FuzzySearch {
                    levenshtein_distance,
                    prefix_match,
                } => format!(
                    "{l}.fuzzy_search({r}, {}, {})",
                    levenshtein_distance.repr(),
                    python(*prefix_match)
                ),
            }
        }
        Expr::In {
            expr,
            values,
            negated,
        } => {
            let name = if *negated { "is_not_in" } else { "is_in" };
            let values = values.iter().map(literal).collect::<Vec<_>>().repr();
            call(expr, &format!("{name}({values})"))
        }
        Expr::IsSome(e) => call(e, "is_some()"),
        Expr::IsNone(e) => call(e, "is_none()"),
        Expr::Any(e) => call(e, "any()"),
        Expr::All(e) => call(e, "all()"),
        Expr::And(items) => {
            let items = flat(items, |i| match i {
                Expr::And(inner) => Some(inner),
                _ => None,
            });
            return combined(items, render, " & ");
        }
        Expr::Or(items) => {
            let items = flat(items, |i| match i {
                Expr::Or(inner) => Some(inner),
                _ => None,
            });
            return combined(items, render, " | ");
        }
        Expr::Not(e) => return Rendered(format!("~{}", render(e).atom()), Shape::Unary),
    };
    Rendered(text, Shape::Chain)
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

fn combined<T>(items: Vec<&T>, render: impl Fn(&T) -> Rendered, sep: &str) -> Rendered {
    let parts: Vec<_> = items.into_iter().map(|i| render(i).atom()).collect();
    Rendered(parts.join(sep), Shape::Compound)
}

impl Repr for NodeLeaf {
    fn repr(&self) -> String {
        match node_tail(self) {
            Some((views, tail)) => format!("{}{tail}", factory("Node", views)),
            None => COMPILED.to_owned(),
        }
    }
}

/// The views a node term carries and the call that reads it, apart, because an
/// endpoint term puts `.src()` between them; `None` for a value built in this
/// process, which has neither.
fn node_tail(leaf: &NodeLeaf) -> Option<(&[ViewOp], String)> {
    Some(match leaf {
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
        } => (views, property(name, *temporal)),
        NodeLeaf::Metadata { views, name } => (views, format!(".metadata({})", python(name))),
        NodeLeaf::IsActive { views } => (views, ".is_active()".to_owned()),
        NodeLeaf::Exists { views } => (views, ".exists()".to_owned()),
        NodeLeaf::Opaque(_) => return None,
    })
}

impl Repr for EdgeLeaf {
    fn repr(&self) -> String {
        let (views, tail) = match self {
            EdgeLeaf::Property {
                views,
                name,
                temporal,
            } => (views, property(name, *temporal)),
            EdgeLeaf::Metadata { views, name } => (views, format!(".metadata({})", python(name))),
            EdgeLeaf::IsActive { views } => (views, ".is_active()".to_owned()),
            EdgeLeaf::Exists { views } => (views, ".exists()".to_owned()),
            EdgeLeaf::IsValid { views } => (views, ".is_valid()".to_owned()),
            EdgeLeaf::IsDeleted { views } => (views, ".is_deleted()".to_owned()),
            EdgeLeaf::IsSelfLoop { views } => (views, ".is_self_loop()".to_owned()),
            EdgeLeaf::Src(inner) => return endpoint("src", inner),
            EdgeLeaf::Dst(inner) => return endpoint("dst", inner),
        };
        format!("{}{tail}", factory("Edge", views))
    }
}

/// `Edge.src()` reads a node field or property; the views on that term are the
/// edge factory's, so they sit before `.src()`.
fn endpoint(end: &str, inner: &Expr<NodeLeaf>) -> String {
    let tail = match inner {
        Expr::Term(leaf) => node_tail(leaf),
        _ => None,
    };
    match tail {
        Some((views, tail)) => format!("{}.{end}(){tail}", factory("Edge", views)),
        None => format!("{}.{end}()<{}>", factory("Edge", &[]), inner.repr()),
    }
}

impl Repr for ExplodedEdgeLeaf {
    fn repr(&self) -> String {
        let (views, tail) = match self {
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal,
            } => (views, property(name, *temporal)),
            ExplodedEdgeLeaf::Metadata { views, name } => {
                (views, format!(".metadata({})", python(name)))
            }
            ExplodedEdgeLeaf::IsActive { views } => (views, ".is_active()".to_owned()),
            ExplodedEdgeLeaf::Exists { views } => (views, ".exists()".to_owned()),
            ExplodedEdgeLeaf::IsValid { views } => (views, ".is_valid()".to_owned()),
            ExplodedEdgeLeaf::IsDeleted { views } => (views, ".is_deleted()".to_owned()),
            ExplodedEdgeLeaf::IsSelfLoop { views } => (views, ".is_self_loop()".to_owned()),
        };
        format!("{}{tail}", factory("ExplodedEdge", views))
    }
}

fn property(name: &str, temporal: bool) -> String {
    let temporal = if temporal { ".temporal()" } else { "" };
    format!(".property({}){temporal}", python(name))
}

/// A view step as the method call that applies it.
impl Repr for ViewOp {
    fn repr(&self) -> String {
        match self {
            ViewOp::Window { start, end } => format!(".window({}, {})", time(start), time(end)),
            ViewOp::At(t) => format!(".at({})", time(t)),
            ViewOp::After(t) => format!(".after({})", time(t)),
            ViewOp::Before(t) => format!(".before({})", time(t)),
            ViewOp::Latest => ".latest()".to_owned(),
            ViewOp::SnapshotAt(t) => format!(".snapshot_at({})", time(t)),
            ViewOp::SnapshotLatest => ".snapshot_latest()".to_owned(),
            ViewOp::Layers(layer) => layer_call("layer", "layers", layer),
            ViewOp::DefaultLayer => ".default_layer()".to_owned(),
            // Excluding every layer leaves none: the call that keeps no layer.
            ViewOp::ExcludeLayers(Layer::All) | ViewOp::ExcludeValidLayers(Layer::All) => {
                ".layers([])".to_owned()
            }
            ViewOp::ExcludeLayers(layer) => layer_call("exclude_layer", "exclude_layers", layer),
            ViewOp::ValidLayers(layer) => layer_list("valid_layers", layer),
            ViewOp::ExcludeValidLayers(layer) => layer_list("exclude_valid_layers", layer),
            ViewOp::ShrinkStart(t) => format!(".shrink_start({})", time(t)),
            ViewOp::ShrinkEnd(t) => format!(".shrink_end({})", time(t)),
            ViewOp::ExcludeNodes(ids) => format!(".exclude_nodes({})", python(ids.as_slice())),
            ViewOp::Subgraph(ids) => format!(".subgraph({})", python(ids.as_slice())),
            ViewOp::SubgraphNodeTypes(types) => {
                format!(".subgraph_node_types({})", python(types.as_slice()))
            }
            ViewOp::Valid => ".valid()".to_owned(),
        }
    }
}

/// A layer selection as a call: the one-name form for one name, the list form
/// otherwise. Every layer restricts nothing, so it is no call.
fn layer_call(one: &str, many: &str, layer: &Layer) -> String {
    match names(layer).as_deref() {
        None => String::new(),
        Some([name]) => format!(".{one}({})", python(*name)),
        Some(names) => format!(".{many}({})", python(names)),
    }
}

/// A layer selection as a list-form call. Every layer restricts nothing, so
/// it is no call.
fn layer_list(method: &str, layer: &Layer) -> String {
    names(layer).map_or_else(String::new, |names| format!(".{method}({})", python(names)))
}

/// The names a layer selection holds, as the filter methods take them, or
/// `None` for every layer, which no list of names spells.
fn names(layer: &Layer) -> Option<Vec<&str>> {
    Some(match layer {
        Layer::All => return None,
        Layer::None => Vec::new(),
        Layer::Default => vec!["_default"],
        Layer::One(name) => vec![&**name],
        Layer::Multiple(names) => names.iter().map(|name| &**name).collect(),
    })
}
