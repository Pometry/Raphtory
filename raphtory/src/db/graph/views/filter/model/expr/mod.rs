//! A filter as data, one expression type per entity.
//!
//! The typed expression API (`NodeFilter.property("score").gt(4)`) is what
//! rust callers write and what the engine compiles. Everything that has to
//! *carry* a filter — a python object, a GraphQL request, a stored permission
//! grant — needs the same filter as plain data. These enums are that data.
//!
//! [`Expr`] holds what every entity can do: constants, aggregates,
//! comparisons, string and set tests, presence tests, `any`/`all` over an
//! element-wise result, and `and`/`or`/`not`. What an entity can *read* is
//! its leaf type: [`NodeLeaf`] mirrors `NodeFilter`, [`EdgeLeaf`] mirrors
//! `EdgeFilter` (with `src`/`dst` holding a node expression), and
//! [`ExplodedEdgeLeaf`] mirrors `ExplodedEdgeFilter`. A combination the API
//! does not have cannot be written down, so it need not be rejected.
//!
//! Every expression has a result type. A comparison of two comparable values
//! is a `Bool`; a comparison of a list-valued side against a value its
//! elements are comparable with is a `List<Bool>`, one answer per element,
//! which `Any` or `All` turn into a `Bool`. A [`FilterExpr`] accepts an
//! expression only when it is a `Bool`. Property types are known only once a
//! graph is at hand, so that check runs when the filter is built against a
//! graph, in [`compile`].

mod compile;
pub mod convert;
mod display;
#[cfg(test)]
mod tests;

pub use compile::Leaf;
pub use convert::{FactoryLeaf, MarkerLeaf, ToExpr, ToFilterExpr};

use super::DynCreateFilter;
use raphtory_api::core::{
    entities::properties::prop::Prop, storage::timeindex::EventTime, Direction,
};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

/// One view restriction, in the order it was applied.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewOp {
    Window { start: EventTime, end: EventTime },
    At(EventTime),
    After(EventTime),
    Before(EventTime),
    Latest,
    SnapshotAt(EventTime),
    SnapshotLatest,
    Layers(Vec<String>),
}

/// A built-in node field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Field {
    Id,
    Name,
    NodeType,
}

/// A reduction over a list-valued expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Agg {
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    Len,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrOp {
    StartsWith,
    EndsWith,
    Contains,
    NotContains,
    FuzzySearch {
        levenshtein_distance: usize,
        prefix_match: bool,
    },
}

/// An already compiled filter carried inside a tree. It exists for filters
/// built from data that lives only in this process, so it runs but does not
/// serialise: asking for its wire form is an error, not a guess.
#[derive(Clone)]
pub struct OpaqueFilter(pub Arc<dyn DynCreateFilter>);

pub const OPAQUE_FILTER_ERROR: &str =
    "this filter has no server-side form; it was built from in-process state";

impl fmt::Debug for OpaqueFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OpaqueFilter")
    }
}

impl PartialEq for OpaqueFilter {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Serialize for OpaqueFilter {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(OPAQUE_FILTER_ERROR))
    }
}

impl<'de> Deserialize<'de> for OpaqueFilter {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(OPAQUE_FILTER_ERROR))
    }
}

/// What every entity can do with a value, whatever the entity reads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expr<L> {
    Const(Prop),
    Agg(Agg, Box<Expr<L>>),
    Cmp(CmpOp, Box<Expr<L>>, Box<Expr<L>>),
    Str(StrOp, Box<Expr<L>>, Box<Expr<L>>),
    In {
        expr: Box<Expr<L>>,
        values: Vec<Prop>,
        negated: bool,
    },
    IsSome(Box<Expr<L>>),
    IsNone(Box<Expr<L>>),
    /// Holds when the element-wise result inside holds for any element.
    Any(Box<Expr<L>>),
    /// Holds when the element-wise result inside holds for every element.
    All(Box<Expr<L>>),
    And(Vec<Expr<L>>),
    Or(Vec<Expr<L>>),
    Not(Box<Expr<L>>),
    /// A read the entity offers; see [`NodeLeaf`], [`EdgeLeaf`], [`ExplodedEdgeLeaf`].
    /// Serialised as the leaf itself, so the read's name is the key.
    #[serde(untagged)]
    Read(L),
}

/// What a node offers: the surface of `NodeFilter`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeLeaf {
    Field {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        field: Field,
    },
    Degree {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        direction: Direction,
    },
    Property {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        name: String,
        /// The property's history as a list instead of its latest value.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        temporal: bool,
    },
    Metadata {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        name: String,
    },
    IsActive {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
}

/// What an edge offers: the surface of `EdgeFilter`, plus a look at either
/// endpoint node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeLeaf {
    Property {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        name: String,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        temporal: bool,
    },
    Metadata {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        name: String,
    },
    IsActive {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    IsValid {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    IsDeleted {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    IsSelfLoop {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    /// A node expression evaluated on the edge's source node.
    Src(Box<NodeExpr>),
    /// A node expression evaluated on the edge's destination node.
    Dst(Box<NodeExpr>),
}

/// What an exploded edge offers: the surface of `ExplodedEdgeFilter`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplodedEdgeLeaf {
    Property {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        name: String,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        temporal: bool,
    },
    Metadata {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        name: String,
    },
    IsActive {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    IsValid {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    IsDeleted {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    IsSelfLoop {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
}

pub type NodeExpr = Expr<NodeLeaf>;
pub type EdgeExpr = Expr<EdgeLeaf>;
pub type ExplodedEdgeExpr = Expr<ExplodedEdgeLeaf>;

/// The filter itself: a yes/no on one kind of entity, a view, or a
/// combination of filters.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterExpr {
    Node(NodeExpr),
    Edge(EdgeExpr),
    ExplodedEdge(ExplodedEdgeExpr),
    /// A graph-level view with no predicate: the result *is* the view.
    View(Vec<ViewOp>),
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
    Not(Box<FilterExpr>),
    /// A filter over in-process state (a node-state column) that has no wire
    /// form: it runs where it was built and cannot be sent anywhere.
    Opaque(OpaqueFilter),
}

impl<L> Expr<L> {
    /// Whether a view appears in any read of this expression.
    pub fn has_view(&self) -> bool
    where
        L: Leaf,
    {
        match self {
            Expr::Const(_) => false,
            Expr::Read(leaf) => leaf.has_view(),
            Expr::Agg(_, e)
            | Expr::IsSome(e)
            | Expr::IsNone(e)
            | Expr::Any(e)
            | Expr::All(e)
            | Expr::Not(e) => e.has_view(),
            Expr::In { expr, .. } => expr.has_view(),
            Expr::Cmp(_, l, r) | Expr::Str(_, l, r) => l.has_view() || r.has_view(),
            Expr::And(items) | Expr::Or(items) => items.iter().any(Self::has_view),
        }
    }
}

impl FilterExpr {
    /// Whether a graph-level view appears anywhere in this filter.
    pub fn has_view(&self) -> bool {
        match self {
            FilterExpr::View(_) => true,
            FilterExpr::And(items) | FilterExpr::Or(items) => items.iter().any(Self::has_view),
            FilterExpr::Not(inner) => inner.has_view(),
            _ => false,
        }
    }

    /// Whether any part of this filter tests edges. An edge test says nothing
    /// about which nodes belong in a node collection, so a node-collection
    /// subscript refuses such a filter.
    pub fn tests_edges(&self) -> bool {
        match self {
            FilterExpr::Edge(_) | FilterExpr::ExplodedEdge(_) => true,
            FilterExpr::Node(_) | FilterExpr::View(_) | FilterExpr::Opaque(_) => false,
            FilterExpr::And(items) | FilterExpr::Or(items) => items.iter().any(Self::tests_edges),
            FilterExpr::Not(inner) => inner.tests_edges(),
        }
    }
}
