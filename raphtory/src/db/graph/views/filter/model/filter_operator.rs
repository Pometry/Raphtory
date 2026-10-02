use crate::db::graph::views::filter::model::property_filter::PropertyFilterValue;
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::arc_str::ArcStr,
};
use serde::{Deserialize, Serialize};
use std::{fmt, fmt::Display, ops::Deref};
use strsim::levenshtein;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    IsIn,
    IsNotIn,
    IsSome,
    IsNone,
    StartsWith,
    EndsWith,
    Contains,
    NotContains,
    FuzzySearch {
        levenshtein_distance: usize,
        prefix_match: bool,
    },
}

impl Display for FilterOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let operator = match self {
            FilterOperator::Eq => "==",
            FilterOperator::Ne => "!=",
            FilterOperator::Lt => "<",
            FilterOperator::Le => "<=",
            FilterOperator::Gt => ">",
            FilterOperator::Ge => ">=",
            FilterOperator::IsIn => "IS_IN",
            FilterOperator::IsNotIn => "IS_NOT_IN",
            FilterOperator::IsSome => "IS_SOME",
            FilterOperator::IsNone => "IS_NONE",
            FilterOperator::StartsWith => "STARTS_WITH",
            FilterOperator::EndsWith => "ENDS_WITH",
            FilterOperator::Contains => "CONTAINS",
            FilterOperator::NotContains => "NOT_CONTAINS",
            FilterOperator::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            } => {
                return write!(f, "FUZZY_SEARCH({},{})", levenshtein_distance, prefix_match);
            }
        };
        write!(f, "{}", operator)
    }
}

impl FilterOperator {
    pub fn is_strictly_numeric_operation(&self) -> bool {
        matches!(
            self,
            FilterOperator::Lt | FilterOperator::Le | FilterOperator::Gt | FilterOperator::Ge
        )
    }

    pub fn is_string_operation(&self) -> bool {
        matches!(
            self,
            Self::StartsWith
                | Self::EndsWith
                | Self::Contains
                | Self::NotContains
                | Self::FuzzySearch { .. }
        )
    }

    /// Fuzzy search
    ///
    /// Arguments:
    ///     levenshtein_distance (int):
    ///     prefix_match (bool):
    ///
    /// Returns:
    ///     bool:
    pub fn fuzzy_search(
        &self,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> impl Fn(&str, &str) -> bool {
        move |left: &str, right: &str| {
            let left = left.to_lowercase();
            let right = right.to_lowercase();
            let levenshtein_match = levenshtein(&left, &right) <= levenshtein_distance;
            let prefix_match = prefix_match && right.starts_with(&left);
            levenshtein_match || prefix_match
        }
    }

    pub fn apply_to_property(&self, left: &PropertyFilterValue, right: Option<&Prop>) -> bool {
        use std::cmp::Ordering::*;
        use FilterOperator::*;
        use PropertyFilterValue::*;

        let cmp = |op: &FilterOperator, r: &Prop, l: &Prop| -> bool {
            match op {
                Eq => r.equals(l),
                Ne => !r.equals(l),
                Lt => r.compare(l).map(|o| o == Less).unwrap_or(false),
                Le => r.compare(l).map(|o| o != Greater).unwrap_or(false),
                Gt => r.compare(l).map(|o| o == Greater).unwrap_or(false),
                Ge => r.compare(l).map(|o| o != Less).unwrap_or(false),
                _ => false,
            }
        };

        match left {
            None => match self {
                IsSome => right.is_some(),
                IsNone => right.is_none(),
                _ => false, // Missing RHS never matches for other ops
            },

            Single(lv) => match self {
                Eq | Ne | Lt | Le | Gt | Ge => {
                    if let Some(r) = right {
                        cmp(self, r, lv)
                    } else {
                        false
                    }
                }

                StartsWith => {
                    if let (Some(Prop::Str(rs)), Prop::Str(ls)) = (right, lv) {
                        rs.deref().starts_with(ls.deref())
                    } else {
                        false
                    }
                }
                EndsWith => {
                    if let (Some(Prop::Str(rs)), Prop::Str(ls)) = (right, lv) {
                        rs.deref().ends_with(ls.deref())
                    } else {
                        false
                    }
                }
                Contains => {
                    if let (Some(Prop::Str(rs)), Prop::Str(ls)) = (right, lv) {
                        rs.deref().contains(ls.deref())
                    } else {
                        false
                    }
                }
                NotContains => {
                    if let (Some(Prop::Str(rs)), Prop::Str(ls)) = (right, lv) {
                        !rs.deref().contains(ls.deref())
                    } else {
                        false
                    }
                }

                FuzzySearch {
                    levenshtein_distance,
                    prefix_match,
                } => {
                    if let (Some(Prop::Str(rs)), Prop::Str(ls)) = (right, lv) {
                        let f = self.fuzzy_search(*levenshtein_distance, *prefix_match);
                        f(ls, rs)
                    } else {
                        false
                    }
                }

                IsIn | IsNotIn | IsSome | IsNone => false,
            },

            Set(set) => match self {
                IsIn => {
                    if let Some(r) = right {
                        set.contains(r.as_ref())
                    } else {
                        false
                    }
                }
                IsNotIn => {
                    if let Some(r) = right {
                        !set.contains(r.as_ref())
                    } else {
                        false
                    }
                }
                _ => false,
            },
        }
    }
}

// ── expr-layer operator kinds (consumed by node_expr/edge_expr) ──

pub trait Comparable: Clone + Send + Sync + 'static {
    fn binary_cmp(op: &BinaryOp, left: &Self, right: &Self) -> bool;
}

pub trait StringComparable: Clone + Send + Sync + 'static {
    fn string_cmp(op: &StringOp, left: &Self, right: &Self) -> bool;
}

/// Ordering and equality operators used by `BinaryCmpExpr`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// String-only operators used by `StringExpr`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StringOp {
    StartsWith,
    EndsWith,
    Contains,
    NotContains,
    FuzzySearch {
        levenshtein_distance: usize,
        prefix_match: bool,
    },
}

/// Unary presence operators used by `UnaryExpr`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnaryOp {
    IsSome,
    IsNone,
}

/// Set membership operators used by `SetNodeFilter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetOp {
    IsIn,
    IsNotIn,
}

impl Comparable for usize {
    fn binary_cmp(op: &BinaryOp, left: &usize, right: &usize) -> bool {
        match op {
            BinaryOp::Eq => left == right,
            BinaryOp::Ne => left != right,
            BinaryOp::Lt => left < right,
            BinaryOp::Le => left <= right,
            BinaryOp::Gt => left > right,
            BinaryOp::Ge => left >= right,
        }
    }
}

impl Comparable for Prop {
    /// Numbers compare by value whatever their width or sign — `i64(3)` is
    /// below `u64::MAX`, `1i64` equals `1.0f64` — exactly, through `i128` and
    /// `Decimal`, the way the property filters always have. Every other type
    /// compares structurally.
    fn binary_cmp(op: &BinaryOp, left: &Prop, right: &Prop) -> bool {
        use std::cmp::Ordering::*;
        match op {
            BinaryOp::Eq => left.equals(right),
            BinaryOp::Ne => !left.equals(right),
            BinaryOp::Lt => left.compare(right) == Some(Less),
            BinaryOp::Le => matches!(left.compare(right), Some(Less | Equal)),
            BinaryOp::Gt => left.compare(right) == Some(Greater),
            BinaryOp::Ge => matches!(left.compare(right), Some(Greater | Equal)),
        }
    }
}

impl Comparable for GID {
    fn binary_cmp(op: &BinaryOp, left: &GID, right: &GID) -> bool {
        match (left, right) {
            (GID::U64(l), GID::U64(r)) => match op {
                BinaryOp::Eq => l == r,
                BinaryOp::Ne => l != r,
                BinaryOp::Lt => l < r,
                BinaryOp::Le => l <= r,
                BinaryOp::Gt => l > r,
                BinaryOp::Ge => l >= r,
            },
            (GID::Str(l), GID::Str(r)) => String::binary_cmp(op, l, r),
            _ => matches!(op, BinaryOp::Ne),
        }
    }
}

impl<T: Comparable> Comparable for Option<T> {
    fn binary_cmp(op: &BinaryOp, left: &Option<T>, right: &Option<T>) -> bool {
        match (left, right) {
            (Some(l), Some(r)) => T::binary_cmp(op, l, r),
            _ => false,
        }
    }
}

impl StringComparable for Prop {
    fn string_cmp(op: &StringOp, left: &Prop, right: &Prop) -> bool {
        match (left, right) {
            (Prop::Str(l), Prop::Str(r)) => ArcStr::string_cmp(op, l, r),
            _ => false,
        }
    }
}

impl StringComparable for GID {
    fn string_cmp(op: &StringOp, left: &GID, right: &GID) -> bool {
        match (left, right) {
            (GID::Str(l), GID::Str(r)) => String::string_cmp(op, l, r),
            _ => false,
        }
    }
}

impl<T: StringComparable> StringComparable for Option<T> {
    fn string_cmp(op: &StringOp, left: &Option<T>, right: &Option<T>) -> bool {
        match (left, right) {
            (Some(l), Some(r)) => T::string_cmp(op, l, r),
            _ => false,
        }
    }
}

impl BinaryOp {
    /// The comparison with its sides swapped.
    pub fn flipped(self) -> BinaryOp {
        match self {
            BinaryOp::Lt => BinaryOp::Gt,
            BinaryOp::Le => BinaryOp::Ge,
            BinaryOp::Gt => BinaryOp::Lt,
            BinaryOp::Ge => BinaryOp::Le,
            same => same,
        }
    }
}

impl Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOp::Eq => write!(f, "=="),
            BinaryOp::Ne => write!(f, "!="),
            BinaryOp::Lt => write!(f, "<"),
            BinaryOp::Le => write!(f, "<="),
            BinaryOp::Gt => write!(f, ">"),
            BinaryOp::Ge => write!(f, ">="),
        }
    }
}

impl Display for StringOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StringOp::StartsWith => write!(f, "STARTS_WITH"),
            StringOp::EndsWith => write!(f, "ENDS_WITH"),
            StringOp::Contains => write!(f, "CONTAINS"),
            StringOp::NotContains => write!(f, "NOT_CONTAINS"),
            StringOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            } => write!(f, "FUZZY_SEARCH({},{})", levenshtein_distance, prefix_match),
        }
    }
}

impl Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOp::IsSome => write!(f, "IS_SOME"),
            UnaryOp::IsNone => write!(f, "IS_NONE"),
        }
    }
}

impl Display for SetOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetOp::IsIn => write!(f, "IS_IN"),
            SetOp::IsNotIn => write!(f, "IS_NOT_IN"),
        }
    }
}

macro_rules! impl_comparable_str {
    ($ty:ty) => {
        impl Comparable for $ty {
            fn binary_cmp(op: &BinaryOp, left: &$ty, right: &$ty) -> bool {
                let (l, r): (&str, &str) = (left, right);
                match op {
                    BinaryOp::Eq => l == r,
                    BinaryOp::Ne => l != r,
                    BinaryOp::Lt => l < r,
                    BinaryOp::Le => l <= r,
                    BinaryOp::Gt => l > r,
                    BinaryOp::Ge => l >= r,
                }
            }
        }
    };
}

impl_comparable_str!(String);
impl_comparable_str!(ArcStr);
impl_comparable_str!(&'static str);

macro_rules! impl_string_comparable_str {
    ($ty:ty) => {
        impl StringComparable for $ty {
            fn string_cmp(op: &StringOp, left: &$ty, right: &$ty) -> bool {
                let (l, r): (&str, &str) = (left, right);
                match op {
                    StringOp::StartsWith => l.starts_with(r),
                    StringOp::EndsWith => l.ends_with(r),
                    StringOp::Contains => l.contains(r),
                    StringOp::NotContains => !l.contains(r),
                    StringOp::FuzzySearch {
                        levenshtein_distance,
                        prefix_match,
                    } => {
                        let l = l.to_lowercase();
                        let r = r.to_lowercase();
                        let lev = levenshtein(&r, &l) <= *levenshtein_distance;
                        let prefix = *prefix_match && l.as_str().starts_with(r.as_str());
                        lev || prefix
                    }
                }
            }
        }
    };
}

impl_string_comparable_str!(String);
impl_string_comparable_str!(ArcStr);
impl_string_comparable_str!(&'static str);
