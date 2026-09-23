//! A readable rendering of an expression, for logs, errors and tests.

use super::{
    Agg, CmpOp, EdgeLeaf, ExplodedEdgeLeaf, Expr, Field, FilterExpr, NodeLeaf, StrOp, ViewOp,
};
use crate::{db::graph::views::filter::model::layered_filter::layer_label, prelude::Layer};
use raphtory_api::core::{storage::timeindex::AsTime, Direction};
use std::fmt::{self, Display};

impl Display for ViewOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ViewOp::Window { start, end } => write!(f, "WINDOW[{}..{}]", start.t(), end.t()),
            ViewOp::At(t) => write!(f, "AT[{}]", t.t()),
            ViewOp::After(t) => write!(f, "AFTER[{}]", t.t()),
            ViewOp::Before(t) => write!(f, "BEFORE[{}]", t.t()),
            ViewOp::Latest => write!(f, "LATEST"),
            ViewOp::SnapshotAt(t) => write!(f, "SNAPSHOT_AT[{}]", t.t()),
            ViewOp::SnapshotLatest => write!(f, "SNAPSHOT_LATEST"),
            ViewOp::Layers(names) => {
                write!(f, "LAYER[{}]", layer_label(&Layer::from(names.clone())))
            }
        }
    }
}

fn views(f: &mut fmt::Formatter<'_>, views: &[ViewOp], inner: &dyn Display) -> fmt::Result {
    if views.is_empty() {
        return write!(f, "{inner}");
    }
    let chain = views
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" . ");
    write!(f, "{chain}({inner})")
}

fn property(f: &mut fmt::Formatter<'_>, v: &[ViewOp], name: &str, temporal: bool) -> fmt::Result {
    if temporal {
        views(f, v, &format!("TEMPORAL({name})"))
    } else {
        views(f, v, &name)
    }
}

impl Display for NodeLeaf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeLeaf::Field { views: v, field } => {
                let name = match field {
                    Field::Id => "id",
                    Field::Name => "name",
                    Field::NodeType => "node_type",
                };
                views(f, v, &name)
            }
            NodeLeaf::Degree {
                views: v,
                direction,
            } => {
                let name = match direction {
                    Direction::BOTH => "degree",
                    Direction::IN => "in_degree",
                    Direction::OUT => "out_degree",
                };
                views(f, v, &name)
            }
            NodeLeaf::Property {
                views: v,
                name,
                temporal,
            } => property(f, v, name, *temporal),
            NodeLeaf::Metadata { views: v, name } => views(f, v, &format!("METADATA({name})")),
            NodeLeaf::IsActive { views: v } => views(f, v, &"IS_ACTIVE"),
        }
    }
}

impl Display for EdgeLeaf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EdgeLeaf::Property {
                views: v,
                name,
                temporal,
            } => property(f, v, name, *temporal),
            EdgeLeaf::Metadata { views: v, name } => views(f, v, &format!("METADATA({name})")),
            EdgeLeaf::IsActive { views: v } => views(f, v, &"IS_ACTIVE"),
            EdgeLeaf::IsValid { views: v } => views(f, v, &"IS_VALID"),
            EdgeLeaf::IsDeleted { views: v } => views(f, v, &"IS_DELETED"),
            EdgeLeaf::IsSelfLoop { views: v } => views(f, v, &"IS_SELF_LOOP"),
            EdgeLeaf::Src(inner) => write!(f, "SRC({inner})"),
            EdgeLeaf::Dst(inner) => write!(f, "DST({inner})"),
        }
    }
}

impl Display for ExplodedEdgeLeaf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExplodedEdgeLeaf::Property {
                views: v,
                name,
                temporal,
            } => property(f, v, name, *temporal),
            ExplodedEdgeLeaf::Metadata { views: v, name } => {
                views(f, v, &format!("METADATA({name})"))
            }
            ExplodedEdgeLeaf::IsActive { views: v } => views(f, v, &"IS_ACTIVE"),
            ExplodedEdgeLeaf::IsValid { views: v } => views(f, v, &"IS_VALID"),
            ExplodedEdgeLeaf::IsDeleted { views: v } => views(f, v, &"IS_DELETED"),
            ExplodedEdgeLeaf::IsSelfLoop { views: v } => views(f, v, &"IS_SELF_LOOP"),
        }
    }
}

impl<L: Display> Display for Expr<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Const(v) => write!(f, "{v}"),
            Expr::Read(leaf) => write!(f, "{leaf}"),
            Expr::Agg(agg, e) => {
                let name = match agg {
                    Agg::Sum => "SUM",
                    Agg::Avg => "AVG",
                    Agg::Min => "MIN",
                    Agg::Max => "MAX",
                    Agg::First => "FIRST",
                    Agg::Last => "LAST",
                    Agg::Len => "LEN",
                };
                write!(f, "{name}({e})")
            }
            Expr::Cmp(op, l, r) => {
                let sym = match op {
                    CmpOp::Eq => "==",
                    CmpOp::Ne => "!=",
                    CmpOp::Lt => "<",
                    CmpOp::Le => "<=",
                    CmpOp::Gt => ">",
                    CmpOp::Ge => ">=",
                };
                write!(f, "{l} {sym} {r}")
            }
            Expr::Str(op, l, r) => {
                let name = match op {
                    StrOp::StartsWith => "STARTS_WITH".to_string(),
                    StrOp::EndsWith => "ENDS_WITH".to_string(),
                    StrOp::Contains => "CONTAINS".to_string(),
                    StrOp::NotContains => "NOT_CONTAINS".to_string(),
                    StrOp::FuzzySearch {
                        levenshtein_distance,
                        prefix_match,
                    } => format!("FUZZY_SEARCH[{levenshtein_distance}, {prefix_match}]"),
                };
                write!(f, "{l} {name} {r}")
            }
            Expr::In {
                expr,
                values,
                negated,
            } => {
                let items = values
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                let name = if *negated { "NOT IN" } else { "IN" };
                write!(f, "{expr} {name} [{items}]")
            }
            Expr::IsSome(e) => write!(f, "IS_SOME({e})"),
            Expr::IsNone(e) => write!(f, "IS_NONE({e})"),
            Expr::Any(e) => write!(f, "ANY({e})"),
            Expr::All(e) => write!(f, "ALL({e})"),
            Expr::And(items) => joined(f, items, " AND "),
            Expr::Or(items) => joined(f, items, " OR "),
            Expr::Not(e) => write!(f, "NOT({e})"),
        }
    }
}

fn joined<T: Display>(f: &mut fmt::Formatter<'_>, items: &[T], sep: &str) -> fmt::Result {
    let parts = items
        .iter()
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(sep);
    write!(f, "{parts}")
}

impl Display for FilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterExpr::Node(e) => write!(f, "NODE({e})"),
            FilterExpr::Edge(e) => write!(f, "EDGE({e})"),
            FilterExpr::ExplodedEdge(e) => write!(f, "EXPLODED_EDGE({e})"),
            FilterExpr::View(ops) => {
                let chain = ops
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" . ");
                write!(f, "VIEW({chain})")
            }
            FilterExpr::And(items) => joined(f, items, " AND "),
            FilterExpr::Or(items) => joined(f, items, " OR "),
            FilterExpr::Not(e) => write!(f, "NOT({e})"),
            FilterExpr::Opaque(_) => write!(f, "OPAQUE"),
        }
    }
}
