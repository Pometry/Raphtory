//! A readable rendering of an expression, for logs, errors and tests.

use super::{Agg, EdgeLeaf, ExplodedEdgeLeaf, Expr, Field, FilterExpr, NodeLeaf, ViewOp};
use crate::{
    db::graph::views::filter::model::{
        filter_operator::{BinaryOp, StringOp},
        layered_filter::layer_label,
        subgraph_filter::id_list,
    },
    prelude::Layer,
};
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
            ViewOp::DefaultLayer => write!(f, "DEFAULT_LAYER"),
            ViewOp::ExcludeLayers(names) => {
                write!(
                    f,
                    "EXCLUDE_LAYER[{}]",
                    layer_label(&Layer::from(names.clone()))
                )
            }
            ViewOp::ShrinkStart(t) => write!(f, "SHRINK_START[{}]", t.t()),
            ViewOp::ShrinkEnd(t) => write!(f, "SHRINK_END[{}]", t.t()),
            ViewOp::ExcludeNodes(ids) => write!(f, "EXCLUDE_NODES[{}]", id_list(ids)),
            ViewOp::Subgraph(ids) => write!(f, "SUBGRAPH[{}]", id_list(ids)),
            ViewOp::SubgraphNodeTypes(types) => {
                write!(f, "SUBGRAPH_NODE_TYPES[{}]", types.join(", "))
            }
            ViewOp::Valid => write!(f, "VALID"),
        }
    }
}

fn views(f: &mut fmt::Formatter<'_>, views: &[ViewOp], inner: &dyn Display) -> fmt::Result {
    if views.is_empty() {
        return write!(f, "{inner}");
    }
    for (i, view) in views.iter().enumerate() {
        if i > 0 {
            f.write_str(" . ")?;
        }
        write!(f, "{view}")?;
    }
    write!(f, "({inner})")
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
            Expr::Term(leaf) => write!(f, "{leaf}"),
            Expr::Agg(agg, e) => {
                let name = match agg {
                    Agg::Sum => "SUM",
                    Agg::Avg => "AVG",
                    Agg::Min => "MIN",
                    Agg::Max => "MAX",
                    Agg::First => "FIRST",
                    Agg::Last => "LAST",
                    Agg::Len => "LEN",
                    Agg::Earliest => "EARLIEST",
                    Agg::Latest => "LATEST",
                };
                write!(f, "{name}({e})")
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
                write!(f, "{l} {sym} {r}")
            }
            Expr::Str(op, l, r) => {
                let name = match op {
                    StringOp::StartsWith => "STARTS_WITH".to_string(),
                    StringOp::EndsWith => "ENDS_WITH".to_string(),
                    StringOp::Contains => "CONTAINS".to_string(),
                    StringOp::NotContains => "NOT_CONTAINS".to_string(),
                    StringOp::FuzzySearch {
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
                let name = if *negated { "NOT IN" } else { "IN" };
                write!(f, "{expr} {name} [")?;
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{value}")?;
                }
                f.write_str("]")
            }
            Expr::IsSome(e) => write!(f, "IS_SOME({e})"),
            Expr::IsNone(e) => write!(f, "IS_NONE({e})"),
            Expr::Any(e) => write!(f, "ANY({e})"),
            Expr::All(e) => write!(f, "ALL({e})"),
            Expr::And(items) => joined(f, items, " AND "),
            Expr::Or(items) => joined(f, items, " OR "),
            Expr::Not(e) => write!(f, "NOT({e})"),
            Expr::Opaque(_) => write!(f, "OPAQUE"),
        }
    }
}

fn joined<T: Display>(f: &mut fmt::Formatter<'_>, items: &[T], sep: &str) -> fmt::Result {
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            f.write_str(sep)?;
        }
        write!(f, "({item})")?;
    }
    Ok(())
}

impl Display for FilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterExpr::Node(e) => write!(f, "NODE({e})"),
            FilterExpr::Edge(e) => write!(f, "EDGE({e})"),
            FilterExpr::ExplodedEdge(e) => write!(f, "EXPLODED_EDGE({e})"),
            FilterExpr::View(ops) => {
                f.write_str("VIEW(")?;
                for (i, op) in ops.iter().enumerate() {
                    if i > 0 {
                        f.write_str(" . ")?;
                    }
                    write!(f, "{op}")?;
                }
                f.write_str(")")
            }
            FilterExpr::And(items) => joined(f, items, " AND "),
            FilterExpr::Or(items) => joined(f, items, " OR "),
            FilterExpr::Not(e) => write!(f, "NOT({e})"),
        }
    }
}
