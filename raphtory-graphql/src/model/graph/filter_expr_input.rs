//! The GraphQL form of the filter expression tree.
//!
//! One input type per entity, mirroring [`expr::NodeExpr`], [`expr::EdgeExpr`]
//! and [`expr::ExplodedEdgeExpr`] field for field, so a filter written in
//! python, rust or a GraphQL document is the same tree spelled three ways. The
//! entity is the key; each term on the entity is a plain field of it.
//!
//! ```graphql
//! filter(expr: { node: { gt: { lhs: { degree: BOTH }, rhs: { degree: IN } } } })
//! filter(expr: { edge: { eq: { lhs: { src: { field: NAME } }, rhs: { const: { str: "alice" } } } } })
//! filter(expr: { node: { any: { gt: { lhs: { temporalProperty: "score" }, rhs: { const: { f64: 4 } } } } } })
//! ```
//!
//! Views scope terms: `{ node: { viewed: { views: [...], expr: { property: "score" } } } }`
//! applies the views to every term inside `expr`.

use crate::model::graph::{
    filtering::{Window, Wrapped},
    property::Value,
    timeindex::GqlTimeInput,
};
use dynamic_graphql::{Enum, InputObject, OneOfInput};
use raphtory::{
    db::{
        api::{
            state::NodeOp,
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                expr::{
                    self, Agg, EdgeLeaf, ExplodedEdgeLeaf, Expr, Field, Leaf, NodeLeaf, ViewOp,
                    OPAQUE_FILTER_ERROR,
                },
                filter_operator::{BinaryOp, StringOp},
            },
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::properties::prop::Prop,
    storage::timeindex::EventTime,
    utils::time::{InputTime, IntoTime},
    Direction,
};
use serde::{Deserialize, Serialize};
use std::{ops::Deref, sync::Arc};

/// A built-in node field.
#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[graphql(name = "NodeFieldName")]
pub enum GqlNodeField {
    /// The node's id.
    Id,
    /// The node's name.
    Name,
    /// The node's type.
    NodeType,
}

/// One view restriction, applied in list order.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "ViewOp")]
pub enum GqlViewOp {
    /// Between `start` (inclusive) and `end` (exclusive).
    Window(Window),
    /// At one time.
    At(GqlTimeInput),
    /// Strictly after a time.
    After(GqlTimeInput),
    /// Strictly before a time.
    Before(GqlTimeInput),
    /// At the latest time; written `latest: true`.
    Latest(bool),
    /// Everything up to and including a time; written `snapshotAt: t`.
    SnapshotAt(GqlTimeInput),
    /// Everything up to the latest time; written `snapshotLatest: true`.
    SnapshotLatest(bool),
    /// Only the named layers.
    Layers(Vec<String>),
}

/// The direction a node degree counts.
#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DegreeDirection {
    In,
    Out,
    Both,
}

impl From<DegreeDirection> for Direction {
    fn from(d: DegreeDirection) -> Self {
        match d {
            DegreeDirection::In => Direction::IN,
            DegreeDirection::Out => Direction::OUT,
            DegreeDirection::Both => Direction::BOTH,
        }
    }
}

fn degree_direction(d: Direction) -> DegreeDirection {
    match d {
        Direction::IN => DegreeDirection::In,
        Direction::OUT => DegreeDirection::Out,
        Direction::BOTH => DegreeDirection::Both,
    }
}

impl From<GqlNodeField> for Field {
    fn from(f: GqlNodeField) -> Self {
        match f {
            GqlNodeField::Id => Field::Id,
            GqlNodeField::Name => Field::Name,
            GqlNodeField::NodeType => Field::NodeType,
        }
    }
}

impl From<Field> for GqlNodeField {
    fn from(f: Field) -> Self {
        match f {
            Field::Id => GqlNodeField::Id,
            Field::Name => GqlNodeField::Name,
            Field::NodeType => GqlNodeField::NodeType,
        }
    }
}

// ── the per-entity expression inputs ─────────────────────────────────────────

/// Stamps out the input types of one entity: the expression itself, its two-sided
/// tests, its membership test and its view wrapper. The variants every entity has
/// are written once here; the entity's own terms are passed in.
macro_rules! entity_expr_input {
    (
        $expr:ident = $expr_name:literal,
        $cmp:ident = $cmp_name:literal,
        $fuzzy:ident = $fuzzy_name:literal,
        $membership:ident = $membership_name:literal,
        $viewed:ident = $viewed_name:literal,
        leaf = $leaf:ident,
        own terms { $( $(#[$own_meta:meta])* $own:ident($own_ty:ty) => $own_conv:expr ),* $(,)? }
    ) => {
        /// Two expressions to compare.
        #[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        #[serde(rename_all = "camelCase")]
        #[graphql(name = $cmp_name)]
        pub struct $cmp {
            /// The left side.
            pub lhs: Wrapped<$expr>,
            /// The right side.
            pub rhs: Wrapped<$expr>,
        }

        /// A fuzzy string match: `lhs` is within `levenshteinDistance` edits of
        /// `rhs`, optionally matching by prefix.
        #[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        #[serde(rename_all = "camelCase")]
        #[graphql(name = $fuzzy_name)]
        pub struct $fuzzy {
            /// The string to test.
            pub lhs: Wrapped<$expr>,
            /// The string to match.
            pub rhs: Wrapped<$expr>,
            /// The largest edit distance that still matches.
            pub levenshtein_distance: usize,
            /// Whether a match on a prefix counts.
            pub prefix_match: bool,
        }

        /// A membership test. `values` is a list; a policy may also leave a single
        /// placeholder here (`{"var": …}`) that resolves to the list per caller.
        #[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        #[serde(rename_all = "camelCase")]
        #[graphql(name = $membership_name)]
        pub struct $membership {
            /// The value to look for.
            pub expr: Wrapped<$expr>,
            /// The values it may be one of.
            pub values: Value,
        }

        /// Views applied to every term inside `expr`, in list order.
        #[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        #[serde(rename_all = "camelCase")]
        #[graphql(name = $viewed_name)]
        pub struct $viewed {
            /// The views, applied in list order.
            pub views: Vec<GqlViewOp>,
            /// The expression evaluated inside them.
            pub expr: Wrapped<$expr>,
        }

        /// A value or yes/no on one entity: a term, an aggregate over one, a
        /// comparison or test, or a combination of yes/nos.
        #[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        #[graphql(name = $expr_name)]
        pub enum $expr {
            /// A literal.
            Const(Value),
            /// The latest value of a property.
            Property(String),
            /// The history of a property, as a list.
            TemporalProperty(String),
            /// A metadata entry.
            Metadata(String),
            /// Whether the entity is active; written `isActive: true`.
            IsActive(bool),
            $( $(#[$own_meta])* $own($own_ty), )*
            /// Views applied to every term inside.
            Viewed(Wrapped<$viewed>),
            /// The sum of the innermost list.
            Sum(Wrapped<$expr>),
            /// The mean of the innermost list.
            Avg(Wrapped<$expr>),
            /// The smallest element of the innermost list.
            Min(Wrapped<$expr>),
            /// The largest element of the innermost list.
            Max(Wrapped<$expr>),
            /// The first element of the innermost list.
            First(Wrapped<$expr>),
            /// The last element of the innermost list.
            Last(Wrapped<$expr>),
            /// The number of elements of the innermost list.
            Len(Wrapped<$expr>),
            /// The earliest update of a temporal history.
            Earliest(Wrapped<$expr>),
            /// The latest update of a temporal history.
            Latest(Wrapped<$expr>),
            /// `lhs == rhs`.
            Eq($cmp),
            /// `lhs != rhs`.
            Ne($cmp),
            /// `lhs < rhs`.
            Lt($cmp),
            /// `lhs <= rhs`.
            Le($cmp),
            /// `lhs > rhs`.
            Gt($cmp),
            /// `lhs >= rhs`.
            Ge($cmp),
            /// The string `lhs` starts with the string `rhs`.
            StartsWith($cmp),
            /// The string `lhs` ends with the string `rhs`.
            EndsWith($cmp),
            /// The string `lhs` contains the string `rhs`.
            Contains($cmp),
            /// The string `lhs` does not contain the string `rhs`.
            NotContains($cmp),
            /// The string `lhs` is within an edit distance of `rhs`.
            FuzzySearch($fuzzy),
            /// `expr` is one of `values`.
            IsIn($membership),
            /// `expr` is none of `values`.
            IsNotIn($membership),
            /// The value is present.
            IsSome(Wrapped<$expr>),
            /// The value is absent.
            IsNone(Wrapped<$expr>),
            /// Holds when the element-wise result inside holds for any element.
            Any(Wrapped<$expr>),
            /// Holds when the element-wise result inside holds for every element.
            All(Wrapped<$expr>),
            /// Every yes/no inside holds.
            And(Vec<$expr>),
            /// Any yes/no inside holds.
            Or(Vec<$expr>),
            /// The yes/no inside does not hold.
            Not(Wrapped<$expr>),
        }

        impl TryFrom<$expr> for Expr<$leaf> {
            type Error = GraphError;

            fn try_from(e: $expr) -> Result<Self, Self::Error> {
                let inner = |w: Wrapped<$expr>| -> Result<Box<Expr<$leaf>>, GraphError> {
                    Ok(Box::new(Expr::try_from(w.into_inner())?))
                };
                let cmp = |op: BinaryOp, c: $cmp| -> Result<Expr<$leaf>, GraphError> {
                    Ok(Expr::Cmp(
                        op,
                        Box::new(Expr::try_from(c.lhs.into_inner())?),
                        Box::new(Expr::try_from(c.rhs.into_inner())?),
                    ))
                };
                let str_op = |op: StringOp, c: $cmp| -> Result<Expr<$leaf>, GraphError> {
                    Ok(Expr::Str(
                        op,
                        Box::new(Expr::try_from(c.lhs.into_inner())?),
                        Box::new(Expr::try_from(c.rhs.into_inner())?),
                    ))
                };
                let members = |m: $membership, negated: bool| -> Result<Expr<$leaf>, GraphError> {
                    Ok(Expr::In {
                        expr: Box::new(Expr::try_from(m.expr.into_inner())?),
                        values: member_values(m.values, negated)?,
                        negated,
                    })
                };
                Ok(match e {
                    $expr::Const(v) => Expr::Const(prop(v)?),
                    $expr::Property(name) => Expr::Term($leaf::property(Vec::new(), name, false)),
                    $expr::TemporalProperty(name) => {
                        Expr::Term($leaf::property(Vec::new(), name, true))
                    }
                    $expr::Metadata(name) => Expr::Term($leaf::metadata(Vec::new(), name)),
                    $expr::IsActive(applied) => {
                        applied_test(applied, "isActive")?;
                        Expr::Term($leaf::is_active(Vec::new()))
                    }
                    $( $expr::$own(v) => {
                        let convert: &dyn Fn($own_ty) -> Result<Expr<$leaf>, GraphError> =
                            &$own_conv;
                        convert(v)?
                    } )*
                    $expr::Viewed(v) => {
                        let v = v.into_inner();
                        let mut expr = Expr::try_from(v.expr.into_inner())?;
                        for op in view_ops(Some(v.views))? {
                            expr.push_view(op);
                        }
                        expr
                    }
                    $expr::Sum(e) => Expr::Agg(Agg::Sum, inner(e)?),
                    $expr::Avg(e) => Expr::Agg(Agg::Avg, inner(e)?),
                    $expr::Min(e) => Expr::Agg(Agg::Min, inner(e)?),
                    $expr::Max(e) => Expr::Agg(Agg::Max, inner(e)?),
                    $expr::First(e) => Expr::Agg(Agg::First, inner(e)?),
                    $expr::Last(e) => Expr::Agg(Agg::Last, inner(e)?),
                    $expr::Len(e) => Expr::Agg(Agg::Len, inner(e)?),
                    $expr::Earliest(e) => Expr::Agg(Agg::Earliest, inner(e)?),
                    $expr::Latest(e) => Expr::Agg(Agg::Latest, inner(e)?),
                    $expr::Eq(c) => cmp(BinaryOp::Eq, c)?,
                    $expr::Ne(c) => cmp(BinaryOp::Ne, c)?,
                    $expr::Lt(c) => cmp(BinaryOp::Lt, c)?,
                    $expr::Le(c) => cmp(BinaryOp::Le, c)?,
                    $expr::Gt(c) => cmp(BinaryOp::Gt, c)?,
                    $expr::Ge(c) => cmp(BinaryOp::Ge, c)?,
                    $expr::StartsWith(c) => str_op(StringOp::StartsWith, c)?,
                    $expr::EndsWith(c) => str_op(StringOp::EndsWith, c)?,
                    $expr::Contains(c) => str_op(StringOp::Contains, c)?,
                    $expr::NotContains(c) => str_op(StringOp::NotContains, c)?,
                    $expr::FuzzySearch(f) => Expr::Str(
                        StringOp::FuzzySearch {
                            levenshtein_distance: f.levenshtein_distance,
                            prefix_match: f.prefix_match,
                        },
                        Box::new(Expr::try_from(f.lhs.into_inner())?),
                        Box::new(Expr::try_from(f.rhs.into_inner())?),
                    ),
                    $expr::IsIn(m) => members(m, false)?,
                    $expr::IsNotIn(m) => members(m, true)?,
                    $expr::IsSome(e) => Expr::IsSome(inner(e)?),
                    $expr::IsNone(e) => Expr::IsNone(inner(e)?),
                    $expr::Any(e) => Expr::Any(inner(e)?),
                    $expr::All(e) => Expr::All(inner(e)?),
                    $expr::And(items) => Expr::And(
                        items
                            .into_iter()
                            .map(Expr::try_from)
                            .collect::<Result<_, _>>()?,
                    ),
                    $expr::Or(items) => Expr::Or(
                        items
                            .into_iter()
                            .map(Expr::try_from)
                            .collect::<Result<_, _>>()?,
                    ),
                    $expr::Not(e) => Expr::Not(inner(e)?),
                })
            }
        }

        impl TryFrom<&Expr<$leaf>> for $expr {
            type Error = GraphError;

            fn try_from(e: &Expr<$leaf>) -> Result<Self, Self::Error> {
                let inner = |e: &Expr<$leaf>| -> Result<Wrapped<$expr>, GraphError> {
                    Ok(Wrapped::from($expr::try_from(e)?))
                };
                let cmp = |l: &Expr<$leaf>, r: &Expr<$leaf>| -> Result<$cmp, GraphError> {
                    Ok($cmp {
                        lhs: Wrapped::from($expr::try_from(l)?),
                        rhs: Wrapped::from($expr::try_from(r)?),
                    })
                };
                Ok(match e {
                    Expr::Const(p) => $expr::Const(value(p)?),
                    Expr::Term(leaf) => {
                        let (views, term) = $expr::leaf_term(leaf)?;
                        if views.is_empty() {
                            term
                        } else {
                            $expr::Viewed(Wrapped::from($viewed {
                                views: views.iter().map(GqlViewOp::from).collect(),
                                expr: Wrapped::from(term),
                            }))
                        }
                    }
                    Expr::Agg(Agg::Sum, e) => $expr::Sum(inner(e)?),
                    Expr::Agg(Agg::Avg, e) => $expr::Avg(inner(e)?),
                    Expr::Agg(Agg::Min, e) => $expr::Min(inner(e)?),
                    Expr::Agg(Agg::Max, e) => $expr::Max(inner(e)?),
                    Expr::Agg(Agg::First, e) => $expr::First(inner(e)?),
                    Expr::Agg(Agg::Last, e) => $expr::Last(inner(e)?),
                    Expr::Agg(Agg::Len, e) => $expr::Len(inner(e)?),
                    Expr::Agg(Agg::Earliest, e) => $expr::Earliest(inner(e)?),
                    Expr::Agg(Agg::Latest, e) => $expr::Latest(inner(e)?),
                    Expr::Cmp(BinaryOp::Eq, l, r) => $expr::Eq(cmp(l, r)?),
                    Expr::Cmp(BinaryOp::Ne, l, r) => $expr::Ne(cmp(l, r)?),
                    Expr::Cmp(BinaryOp::Lt, l, r) => $expr::Lt(cmp(l, r)?),
                    Expr::Cmp(BinaryOp::Le, l, r) => $expr::Le(cmp(l, r)?),
                    Expr::Cmp(BinaryOp::Gt, l, r) => $expr::Gt(cmp(l, r)?),
                    Expr::Cmp(BinaryOp::Ge, l, r) => $expr::Ge(cmp(l, r)?),
                    Expr::Str(StringOp::StartsWith, l, r) => $expr::StartsWith(cmp(l, r)?),
                    Expr::Str(StringOp::EndsWith, l, r) => $expr::EndsWith(cmp(l, r)?),
                    Expr::Str(StringOp::Contains, l, r) => $expr::Contains(cmp(l, r)?),
                    Expr::Str(StringOp::NotContains, l, r) => $expr::NotContains(cmp(l, r)?),
                    Expr::Str(
                        StringOp::FuzzySearch {
                            levenshtein_distance,
                            prefix_match,
                        },
                        l,
                        r,
                    ) => $expr::FuzzySearch($fuzzy {
                        lhs: Wrapped::from($expr::try_from(l.deref())?),
                        rhs: Wrapped::from($expr::try_from(r.deref())?),
                        levenshtein_distance: *levenshtein_distance,
                        prefix_match: *prefix_match,
                    }),
                    Expr::In {
                        expr,
                        values,
                        negated,
                    } => {
                        let m = $membership {
                            expr: Wrapped::from($expr::try_from(expr.deref())?),
                            values: Value::List(
                                values.iter().map(value).collect::<Result<Vec<_>, _>>()?,
                            ),
                        };
                        if *negated {
                            $expr::IsNotIn(m)
                        } else {
                            $expr::IsIn(m)
                        }
                    }
                    Expr::IsSome(e) => $expr::IsSome(inner(e)?),
                    Expr::IsNone(e) => $expr::IsNone(inner(e)?),
                    Expr::Any(e) => $expr::Any(inner(e)?),
                    Expr::All(e) => $expr::All(inner(e)?),
                    Expr::And(items) => $expr::And(
                        items
                            .iter()
                            .map($expr::try_from)
                            .collect::<Result<_, _>>()?,
                    ),
                    Expr::Or(items) => $expr::Or(
                        items
                            .iter()
                            .map($expr::try_from)
                            .collect::<Result<_, _>>()?,
                    ),
                    Expr::Not(e) => $expr::Not(inner(e)?),
                })
            }
        }
    };
}

entity_expr_input! {
    GqlNodeExpr = "NodeExpr",
    GqlNodeCmp = "NodeCmp",
    GqlNodeFuzzyCmp = "NodeFuzzyCmp",
    GqlNodeMembership = "NodeMembership",
    GqlNodeViewed = "NodeViewed",
    leaf = NodeLeaf,
    own terms {
        /// A built-in node field.
        Field(GqlNodeField) => |f: GqlNodeField| {
            Ok(Expr::Term(NodeLeaf::Field {
                views: Vec::new(),
                field: f.into(),
            }))
        },
        /// The node's degree in a direction.
        Degree(DegreeDirection) => |d: DegreeDirection| {
            Ok(Expr::Term(NodeLeaf::Degree {
                views: Vec::new(),
                direction: d.into(),
            }))
        },
    }
}

entity_expr_input! {
    GqlEdgeExpr = "EdgeExpr",
    GqlEdgeCmp = "EdgeCmp",
    GqlEdgeFuzzyCmp = "EdgeFuzzyCmp",
    GqlEdgeMembership = "EdgeMembership",
    GqlEdgeViewed = "EdgeViewed",
    leaf = EdgeLeaf,
    own terms {
        /// Whether the edge is valid; written `isValid: true`.
        IsValid(bool) => |applied: bool| {
            applied_test(applied, "isValid")?;
            Ok(Expr::Term(EdgeLeaf::IsValid { views: Vec::new() }))
        },
        /// Whether the edge is deleted; written `isDeleted: true`.
        IsDeleted(bool) => |applied: bool| {
            applied_test(applied, "isDeleted")?;
            Ok(Expr::Term(EdgeLeaf::IsDeleted { views: Vec::new() }))
        },
        /// Whether the edge is a self loop; written `isSelfLoop: true`.
        IsSelfLoop(bool) => |applied: bool| {
            applied_test(applied, "isSelfLoop")?;
            Ok(Expr::Term(EdgeLeaf::IsSelfLoop { views: Vec::new() }))
        },
        /// A node expression evaluated on the edge's source node.
        Src(Wrapped<GqlNodeExpr>) => |e: Wrapped<GqlNodeExpr>| {
            Ok(Expr::Term(EdgeLeaf::Src(Box::new(Expr::try_from(e.into_inner())?))))
        },
        /// A node expression evaluated on the edge's destination node.
        Dst(Wrapped<GqlNodeExpr>) => |e: Wrapped<GqlNodeExpr>| {
            Ok(Expr::Term(EdgeLeaf::Dst(Box::new(Expr::try_from(e.into_inner())?))))
        },
    }
}

entity_expr_input! {
    GqlExplodedEdgeExpr = "ExplodedEdgeExpr",
    GqlExplodedEdgeCmp = "ExplodedEdgeCmp",
    GqlExplodedEdgeFuzzyCmp = "ExplodedEdgeFuzzyCmp",
    GqlExplodedEdgeMembership = "ExplodedEdgeMembership",
    GqlExplodedEdgeViewed = "ExplodedEdgeViewed",
    leaf = ExplodedEdgeLeaf,
    own terms {
        /// Whether the edge update is valid; written `isValid: true`.
        IsValid(bool) => |applied: bool| {
            applied_test(applied, "isValid")?;
            Ok(Expr::Term(ExplodedEdgeLeaf::IsValid { views: Vec::new() }))
        },
        /// Whether the edge update is deleted; written `isDeleted: true`.
        IsDeleted(bool) => |applied: bool| {
            applied_test(applied, "isDeleted")?;
            Ok(Expr::Term(ExplodedEdgeLeaf::IsDeleted { views: Vec::new() }))
        },
        /// Whether the edge update is a self loop; written `isSelfLoop: true`.
        IsSelfLoop(bool) => |applied: bool| {
            applied_test(applied, "isSelfLoop")?;
            Ok(Expr::Term(ExplodedEdgeLeaf::IsSelfLoop { views: Vec::new() }))
        },
    }
}

// ── the entity-specific terms, tree → GraphQL ────────────────────────────────

impl GqlNodeExpr {
    /// A leaf as the term it is, and the views it carries.
    fn leaf_term(leaf: &NodeLeaf) -> Result<(&[ViewOp], GqlNodeExpr), GraphError> {
        Ok(match leaf {
            NodeLeaf::Field { views, field } => (views, GqlNodeExpr::Field((*field).into())),
            NodeLeaf::Degree { views, direction } => {
                (views, GqlNodeExpr::Degree(degree_direction(*direction)))
            }
            NodeLeaf::Property {
                views,
                name,
                temporal: false,
            } => (views, GqlNodeExpr::Property(name.clone())),
            NodeLeaf::Property {
                views,
                name,
                temporal: true,
            } => (views, GqlNodeExpr::TemporalProperty(name.clone())),
            NodeLeaf::Metadata { views, name } => (views, GqlNodeExpr::Metadata(name.clone())),
            NodeLeaf::IsActive { views } => (views, GqlNodeExpr::IsActive(true)),
        })
    }
}

impl GqlEdgeExpr {
    fn leaf_term(leaf: &EdgeLeaf) -> Result<(&[ViewOp], GqlEdgeExpr), GraphError> {
        Ok(match leaf {
            EdgeLeaf::Property {
                views,
                name,
                temporal: false,
            } => (views, GqlEdgeExpr::Property(name.clone())),
            EdgeLeaf::Property {
                views,
                name,
                temporal: true,
            } => (views, GqlEdgeExpr::TemporalProperty(name.clone())),
            EdgeLeaf::Metadata { views, name } => (views, GqlEdgeExpr::Metadata(name.clone())),
            EdgeLeaf::IsActive { views } => (views, GqlEdgeExpr::IsActive(true)),
            EdgeLeaf::IsValid { views } => (views, GqlEdgeExpr::IsValid(true)),
            EdgeLeaf::IsDeleted { views } => (views, GqlEdgeExpr::IsDeleted(true)),
            EdgeLeaf::IsSelfLoop { views } => (views, GqlEdgeExpr::IsSelfLoop(true)),
            // The endpoint's own terms carry their views; there are none here.
            EdgeLeaf::Src(inner) => (
                &[],
                GqlEdgeExpr::Src(Wrapped::from(GqlNodeExpr::try_from(inner.deref())?)),
            ),
            EdgeLeaf::Dst(inner) => (
                &[],
                GqlEdgeExpr::Dst(Wrapped::from(GqlNodeExpr::try_from(inner.deref())?)),
            ),
        })
    }
}

impl GqlExplodedEdgeExpr {
    fn leaf_term(leaf: &ExplodedEdgeLeaf) -> Result<(&[ViewOp], GqlExplodedEdgeExpr), GraphError> {
        Ok(match leaf {
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal: false,
            } => (views, GqlExplodedEdgeExpr::Property(name.clone())),
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal: true,
            } => (views, GqlExplodedEdgeExpr::TemporalProperty(name.clone())),
            ExplodedEdgeLeaf::Metadata { views, name } => {
                (views, GqlExplodedEdgeExpr::Metadata(name.clone()))
            }
            ExplodedEdgeLeaf::IsActive { views } => (views, GqlExplodedEdgeExpr::IsActive(true)),
            ExplodedEdgeLeaf::IsValid { views } => (views, GqlExplodedEdgeExpr::IsValid(true)),
            ExplodedEdgeLeaf::IsDeleted { views } => (views, GqlExplodedEdgeExpr::IsDeleted(true)),
            ExplodedEdgeLeaf::IsSelfLoop { views } => {
                (views, GqlExplodedEdgeExpr::IsSelfLoop(true))
            }
        })
    }
}

// ── the filter ───────────────────────────────────────────────────────────────

/// The filter itself: a yes/no over one kind of entity, a view, or a combination.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "FilterExpr")]
pub enum GqlFilter {
    /// A yes/no over nodes.
    Node(GqlNodeExpr),
    /// A yes/no over edges.
    Edge(GqlEdgeExpr),
    /// A yes/no over exploded edges, one per update of an edge.
    ExplodedEdge(GqlExplodedEdgeExpr),
    /// A graph-level view with no predicate: the result is the view.
    View(Vec<GqlViewOp>),
    /// Every leg holds. A view leg applies first and the others run inside it.
    And(Vec<GqlFilter>),
    /// Any leg holds. Node legs combine on nodes and edge legs on edges; a leg
    /// of the other kind leaves that side unconstrained. No view legs.
    Or(Vec<GqlFilter>),
    /// The filter that keeps what the inner one drops: a negated node filter
    /// keeps the nodes that fail it and the edges between them. No views.
    Not(Wrapped<GqlFilter>),
}

fn invalid(msg: impl Into<String>) -> GraphError {
    GraphError::InvalidGqlFilter(msg.into())
}

/// A test with no payload is written `name: true`; `false` is refused rather
/// than ignored, since a test that is not applied has no place in a filter.
fn applied_test(applied: bool, name: &str) -> Result<(), GraphError> {
    if applied {
        Ok(())
    } else {
        Err(invalid(format!(
            "{name}: false is not a test; write {name}: true"
        )))
    }
}

impl TryFrom<GqlViewOp> for ViewOp {
    type Error = GraphError;

    /// `latest: false` and `snapshotLatest: false` are refused rather than
    /// ignored: a view op that is not applied has no place in a view list.
    fn try_from(op: GqlViewOp) -> Result<Self, Self::Error> {
        Ok(match op {
            GqlViewOp::Window(w) => ViewOp::Window {
                start: w.start.into_time(),
                end: w.end.into_time(),
            },
            GqlViewOp::At(t) => ViewOp::At(t.into_time()),
            GqlViewOp::After(t) => ViewOp::After(t.into_time()),
            GqlViewOp::Before(t) => ViewOp::Before(t.into_time()),
            GqlViewOp::Latest(true) => ViewOp::Latest,
            GqlViewOp::Latest(false) => return Err(invalid("latest: false is not a view")),
            GqlViewOp::SnapshotAt(t) => ViewOp::SnapshotAt(t.into_time()),
            GqlViewOp::SnapshotLatest(true) => ViewOp::SnapshotLatest,
            GqlViewOp::SnapshotLatest(false) => {
                return Err(invalid("snapshotLatest: false is not a view"))
            }
            GqlViewOp::Layers(names) => ViewOp::Layers(names),
        })
    }
}

fn view_ops(views: Option<Vec<GqlViewOp>>) -> Result<Vec<ViewOp>, GraphError> {
    views
        .unwrap_or_default()
        .into_iter()
        .map(ViewOp::try_from)
        .collect()
}

fn time(t: EventTime) -> GqlTimeInput {
    GqlTimeInput(InputTime::Indexed(t.0, t.1))
}

impl From<&ViewOp> for GqlViewOp {
    fn from(op: &ViewOp) -> Self {
        match op {
            ViewOp::Window { start, end } => GqlViewOp::Window(Window {
                start: time(*start),
                end: time(*end),
            }),
            ViewOp::At(t) => GqlViewOp::At(time(*t)),
            ViewOp::After(t) => GqlViewOp::After(time(*t)),
            ViewOp::Before(t) => GqlViewOp::Before(time(*t)),
            ViewOp::Latest => GqlViewOp::Latest(true),
            ViewOp::SnapshotAt(t) => GqlViewOp::SnapshotAt(time(*t)),
            ViewOp::SnapshotLatest => GqlViewOp::SnapshotLatest(true),
            ViewOp::Layers(names) => GqlViewOp::Layers(names.clone()),
        }
    }
}

fn prop(value: Value) -> Result<Prop, GraphError> {
    Prop::try_from(value).map_err(|e| invalid(format!("invalid constant: {e}")))
}

fn value(p: &Prop) -> Result<Value, GraphError> {
    Value::try_from(p).map_err(|e| invalid(format!("constant has no wire form: {e}")))
}

/// The members of a set: a list of constants. Anything else, a placeholder a
/// policy failed to resolve included, is refused.
fn member_values(values: Value, negated: bool) -> Result<Vec<Prop>, GraphError> {
    let op = if negated { "isNotIn" } else { "isIn" };
    match values {
        Value::List(items) => items.into_iter().map(prop).collect(),
        other => Err(invalid(format!("{op} requires a list value, got {other}"))),
    }
}

impl TryFrom<GqlFilter> for expr::FilterExpr {
    type Error = GraphError;

    fn try_from(filter: GqlFilter) -> Result<Self, Self::Error> {
        use expr::FilterExpr as F;
        Ok(match filter {
            GqlFilter::Node(e) => F::Node(e.try_into()?),
            GqlFilter::Edge(e) => F::Edge(e.try_into()?),
            GqlFilter::ExplodedEdge(e) => F::ExplodedEdge(e.try_into()?),
            GqlFilter::View(ops) => F::View(view_ops(Some(ops))?),
            GqlFilter::And(items) => F::And(
                items
                    .into_iter()
                    .map(F::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            GqlFilter::Or(items) => F::Or(
                items
                    .into_iter()
                    .map(F::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            GqlFilter::Not(inner) => F::Not(Box::new(inner.into_inner().try_into()?)),
        })
    }
}

// Clients build trees and send them; this is the spelling they send.
impl TryFrom<&expr::FilterExpr> for GqlFilter {
    type Error = GraphError;

    fn try_from(filter: &expr::FilterExpr) -> Result<Self, Self::Error> {
        use expr::FilterExpr as F;
        Ok(match filter {
            F::Opaque(_) => return Err(invalid(OPAQUE_FILTER_ERROR)),
            F::Node(e) => GqlFilter::Node(e.try_into()?),
            F::Edge(e) => GqlFilter::Edge(e.try_into()?),
            F::ExplodedEdge(e) => GqlFilter::ExplodedEdge(e.try_into()?),
            F::View(ops) => GqlFilter::View(ops.iter().map(GqlViewOp::from).collect()),
            F::And(items) => GqlFilter::And(
                items
                    .iter()
                    .map(GqlFilter::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            F::Or(items) => GqlFilter::Or(
                items
                    .iter()
                    .map(GqlFilter::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            F::Not(inner) => GqlFilter::Not(Wrapped::from(GqlFilter::try_from(inner.deref())?)),
        })
    }
}

impl TryFrom<expr::FilterExpr> for GqlFilter {
    type Error = GraphError;

    fn try_from(filter: expr::FilterExpr) -> Result<Self, Self::Error> {
        GqlFilter::try_from(&filter)
    }
}

impl CreateFilter for GqlFilter {
    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = DynEdgeFilter<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        expr::FilterExpr::try_from(self)?.create_graph_filter(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        expr::FilterExpr::try_from(self)?.create_node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        expr::FilterExpr::try_from(self)?.create_edge_filter(graph)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use expr::FilterExpr as F;
    use raphtory::db::graph::views::filter::model::{
        node_filter::NodeFilter, EntityExprFilterOps, PropertyExprFactory,
    };

    fn degree(direction: Direction) -> Expr<NodeLeaf> {
        Expr::Term(NodeLeaf::Degree {
            views: Vec::new(),
            direction,
        })
    }

    #[test]
    fn a_tree_survives_the_trip_through_the_wire_type_and_json() {
        let tree = F::And(vec![
            F::Node(Expr::Any(Box::new(Expr::Cmp(
                BinaryOp::Gt,
                Box::new(Expr::Agg(
                    Agg::Sum,
                    Box::new(Expr::Term(NodeLeaf::Property {
                        views: vec![ViewOp::Window {
                            start: EventTime::from(0),
                            end: EventTime::from(5),
                        }],
                        name: "score".into(),
                        temporal: true,
                    })),
                )),
                Box::new(Expr::Const(Prop::F64(10.0))),
            )))),
            F::Node(Expr::Cmp(
                BinaryOp::Gt,
                Box::new(degree(Direction::BOTH)),
                Box::new(degree(Direction::IN)),
            )),
            F::Edge(Expr::Term(EdgeLeaf::IsActive {
                views: vec![ViewOp::Layers(vec!["works".into()])],
            })),
            F::Not(Box::new(F::Edge(Expr::In {
                expr: Box::new(Expr::Term(EdgeLeaf::Src(Box::new(Expr::Term(
                    NodeLeaf::Field {
                        views: Vec::new(),
                        field: Field::Name,
                    },
                ))))),
                values: vec![Prop::str("alice"), Prop::str("bob")],
                negated: false,
            }))),
            F::ExplodedEdge(Expr::Str(
                StringOp::FuzzySearch {
                    levenshtein_distance: 2,
                    prefix_match: false,
                },
                Box::new(Expr::Term(ExplodedEdgeLeaf::Property {
                    views: Vec::new(),
                    name: "tag".into(),
                    temporal: false,
                })),
                Box::new(Expr::Const(Prop::str("rock"))),
            )),
            F::View(vec![ViewOp::Latest]),
        ]);
        let wire = GqlFilter::try_from(&tree).unwrap();
        let json = serde_json::to_string(&wire).unwrap();
        let wire_back: GqlFilter = serde_json::from_str(&json).unwrap();
        let tree_back = F::try_from(wire_back).unwrap();
        assert_eq!(tree_back, tree);
    }

    #[test]
    fn the_json_spelling_keys_on_the_entity_and_the_term() {
        let tree = F::Edge(Expr::Cmp(
            BinaryOp::Eq,
            Box::new(Expr::Term(EdgeLeaf::Src(Box::new(Expr::Term(
                NodeLeaf::Property {
                    views: vec![ViewOp::Latest],
                    name: "score".into(),
                    temporal: false,
                },
            ))))),
            Box::new(Expr::Const(Prop::I64(1))),
        ));
        let wire = GqlFilter::try_from(&tree).unwrap();
        assert_eq!(
            serde_json::to_value(&wire).unwrap(),
            serde_json::json!({
                "edge": { "eq": {
                    "lhs": { "src": { "viewed": { "views": [{ "latest": true }], "expr": { "property": "score" } } } },
                    "rhs": { "const": { "i64": 1 } }
                } }
            })
        );
    }

    #[test]
    fn a_test_that_is_not_applied_is_refused() {
        for (op, name) in [
            (GqlViewOp::Latest(false), "latest"),
            (GqlViewOp::SnapshotLatest(false), "snapshotLatest"),
        ] {
            let err = ViewOp::try_from(op).unwrap_err();
            assert!(err.to_string().contains(name), "{err}");
        }
        let err = Expr::<NodeLeaf>::try_from(GqlNodeExpr::IsActive(false)).unwrap_err();
        assert!(err.to_string().contains("isActive"), "{err}");
        let err = Expr::<EdgeLeaf>::try_from(GqlEdgeExpr::IsValid(false)).unwrap_err();
        assert!(err.to_string().contains("isValid"), "{err}");
    }

    #[test]
    fn an_opaque_filter_has_no_wire_form() {
        let opaque = F::Opaque(expr::OpaqueFilter::new(
            NodeFilter.property("score").is_some(),
        ));
        let err = GqlFilter::try_from(&opaque).unwrap_err();
        assert!(err.to_string().contains(OPAQUE_FILTER_ERROR), "{err}");
    }
}
