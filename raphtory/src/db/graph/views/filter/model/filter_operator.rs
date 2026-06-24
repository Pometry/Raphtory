use crate::db::graph::views::filter::model::{
    filter::FieldFilterValue, filter_value::FilterValue, property_filter::PropertyFilterValue,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, GidRef, GID},
    storage::arc_str::ArcStr,
};
use std::{collections::HashSet, fmt, fmt::Display, ops::Deref};
use strsim::levenshtein;

// ─────────────────────────────────────────────────────────────────────────────
// Comparable — type-driven ordering/equality comparison for BinaryCmpNodeOp
// ─────────────────────────────────────────────────────────────────────────────

pub trait Comparable: Clone + Send + Sync + 'static {
    fn binary_cmp(op: &BinaryOp, left: &Self, right: &Self) -> bool;
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

impl Comparable for Prop {
    fn binary_cmp(op: &BinaryOp, left: &Prop, right: &Prop) -> bool {
        use std::cmp::Ordering::*;

        // Try casting right to left's type for cross-type numeric comparisons
        // (e.g. Prop::I32(1) vs Prop::U64(1), or Prop::F64(3.0) vs Prop::U64(3)).
        let right_casted = right.clone().try_cast(left.dtype());
        let right = right_casted.as_ref().unwrap_or(right);

        match op {
            BinaryOp::Eq => left == right,
            BinaryOp::Ne => left != right,
            BinaryOp::Lt => left.partial_cmp(right).map(|o| o == Less).unwrap_or(false),
            BinaryOp::Le => left
                .partial_cmp(right)
                .map(|o| o != Greater)
                .unwrap_or(false),
            BinaryOp::Gt => left
                .partial_cmp(right)
                .map(|o| o == Greater)
                .unwrap_or(false),
            BinaryOp::Ge => left.partial_cmp(right).map(|o| o != Less).unwrap_or(false),
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

// ─────────────────────────────────────────────────────────────────────────────
// StringComparable — type-driven string comparison for StringOpNodeOp
// ─────────────────────────────────────────────────────────────────────────────

pub trait StringComparable: Clone + Send + Sync + 'static {
    fn string_cmp(op: &StringOp, left: &Self, right: &Self) -> bool;
}

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

// ─────────────────────────────────────────────────────────────────────────────
// Focused operator enums for the NodeExpr expression system
// ─────────────────────────────────────────────────────────────────────────────

/// Ordering and equality operators used by `BinaryCmpExpr`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
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

/// String-only operators used by `StringExpr`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Unary presence operators used by `UnaryExpr`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    IsSome,
    IsNone,
}

impl Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOp::IsSome => write!(f, "IS_SOME"),
            UnaryOp::IsNone => write!(f, "IS_NONE"),
        }
    }
}

/// Set membership operators used by `SetNodeFilter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    IsIn,
    IsNotIn,
}

impl Display for SetOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetOp::IsIn => write!(f, "IS_IN"),
            SetOp::IsNotIn => write!(f, "IS_NOT_IN"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FilterOperator — kept for the PropertyFilter system
// ─────────────────────────────────────────────────────────────────────────────

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

    fn operation<T>(&self) -> impl Fn(&T, &T) -> bool
    where
        T: ?Sized + PartialEq + PartialOrd,
    {
        match self {
            FilterOperator::Eq => T::eq,
            FilterOperator::Ne => T::ne,
            FilterOperator::Lt => T::lt,
            FilterOperator::Le => T::le,
            FilterOperator::Gt => T::gt,
            FilterOperator::Ge => T::ge,
            _ => panic!("Operation not supported for this operator"),
        }
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

    fn collection_operation<T>(&self) -> impl Fn(&HashSet<T>, &T) -> bool
    where
        T: Eq + std::hash::Hash,
    {
        match self {
            FilterOperator::IsIn => |set: &HashSet<T>, value: &T| set.contains(value),
            FilterOperator::IsNotIn => |set: &HashSet<T>, value: &T| !set.contains(value),
            _ => panic!("Collection operation not supported for this operator"),
        }
    }

    pub fn apply_to_property(&self, left: &PropertyFilterValue, right: Option<&Prop>) -> bool {
        use std::cmp::Ordering::*;
        use FilterOperator::*;
        use FilterValue::{None as FNone, Set as FSet, Single as FSingle};

        let cmp = |op: &FilterOperator, r: &Prop, l: &Prop| -> bool {
            match op {
                Eq => r == l,
                Ne => r != l,
                Lt => r.partial_cmp(l).map(|o| o == Less).unwrap_or(false),
                Le => r.partial_cmp(l).map(|o| o != Greater).unwrap_or(false),
                Gt => r.partial_cmp(l).map(|o| o == Greater).unwrap_or(false),
                Ge => r.partial_cmp(l).map(|o| o != Less).unwrap_or(false),
                _ => false,
            }
        };

        match left {
            FNone => match self {
                IsSome => right.is_some(),
                IsNone => right.is_none(),
                _ => false,
            },

            FSingle(lv) => match self {
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
                        f(ls.deref(), rs.deref())
                    } else {
                        false
                    }
                }

                IsIn | IsNotIn | IsSome | IsNone => false,
            },

            FSet(set) => match self {
                IsIn => right.map(|r| set.contains(r)).unwrap_or(false),
                IsNotIn => right.map(|r| !set.contains(r)).unwrap_or(false),
                _ => false,
            },
        }
    }

    pub fn apply(&self, left: &FieldFilterValue, right: Option<&str>) -> bool {
        match left {
            FieldFilterValue::Single(l) => match self {
                FilterOperator::Eq | FilterOperator::Ne => match right {
                    Some(r) => self.operation()(r, l),
                    None => matches!(self, FilterOperator::Ne),
                },
                FilterOperator::StartsWith => right.is_some_and(|r| r.starts_with(l)),
                FilterOperator::EndsWith => right.is_some_and(|r| r.ends_with(l)),
                FilterOperator::Contains => right.is_some_and(|r| r.contains(l)),
                FilterOperator::NotContains => right.is_some_and(|r| !r.contains(l)),
                FilterOperator::FuzzySearch {
                    levenshtein_distance,
                    prefix_match,
                } => right.is_some_and(|r| {
                    let fuzzy_fn = self.fuzzy_search(*levenshtein_distance, *prefix_match);
                    fuzzy_fn(l, r)
                }),
                _ => unreachable!(),
            },

            FieldFilterValue::Set(l) => match self {
                FilterOperator::IsIn | FilterOperator::IsNotIn => match right {
                    Some(r) => self.collection_operation()(l, &r.to_string()),
                    None => matches!(self, FilterOperator::IsNotIn),
                },
                _ => unreachable!(),
            },

            FieldFilterValue::ID(_) | FieldFilterValue::IDSet(_) => unreachable!(),
        }
    }

    pub fn apply_id(&self, left: &FieldFilterValue, right: GidRef<'_>) -> bool {
        match left {
            FieldFilterValue::ID(GID::U64(l)) => match right {
                GidRef::U64(r) => match self {
                    FilterOperator::Eq
                    | FilterOperator::Ne
                    | FilterOperator::Lt
                    | FilterOperator::Le
                    | FilterOperator::Gt
                    | FilterOperator::Ge => self.operation()(&r, l),
                    _ => false,
                },
                GidRef::Str(_) => false,
            },

            FieldFilterValue::ID(GID::Str(ls)) | FieldFilterValue::Single(ls) => match right {
                GidRef::Str(rs) => match self {
                    FilterOperator::Eq | FilterOperator::Ne => self.operation()(&rs, &ls.as_str()),
                    FilterOperator::StartsWith => rs.starts_with(ls),
                    FilterOperator::EndsWith => rs.ends_with(ls),
                    FilterOperator::Contains => rs.contains(ls),
                    FilterOperator::NotContains => !rs.contains(ls),
                    FilterOperator::FuzzySearch {
                        levenshtein_distance,
                        prefix_match,
                    } => {
                        let f = self.fuzzy_search(*levenshtein_distance, *prefix_match);
                        f(ls, rs)
                    }
                    _ => false,
                },
                GidRef::U64(_) => false,
            },

            FieldFilterValue::IDSet(set) => match right {
                GidRef::U64(r) => match self {
                    FilterOperator::IsIn => set.contains(&GID::U64(r)),
                    FilterOperator::IsNotIn => !set.contains(&GID::U64(r)),
                    _ => false,
                },
                GidRef::Str(s) => match self {
                    FilterOperator::IsIn => set.contains(&GID::Str(s.to_string())),
                    FilterOperator::IsNotIn => !set.contains(&GID::Str(s.to_string())),
                    _ => false,
                },
            },

            FieldFilterValue::Set(set) => match right {
                GidRef::U64(_) => false,
                GidRef::Str(s) => match self {
                    FilterOperator::IsIn => set.contains(s),
                    FilterOperator::IsNotIn => !set.contains(s),
                    _ => false,
                },
            },
        }
    }

    /// Compare two optional values symmetrically.
    ///
    /// Used by `BinaryCmpExpr` where both sides are expressions that may return `None`.
    /// Supports Eq, Ne, Lt, Le, Gt, Ge.  All other operators return `false`.
    pub fn compare_values<T>(&self, left: Option<&T>, right: Option<&T>) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        use std::cmp::Ordering::*;
        use FilterOperator::*;

        match (left, right) {
            (Some(l), Some(r)) => match self {
                Eq => l == r,
                Ne => l != r,
                Lt => l.partial_cmp(r).map(|o| o == Less).unwrap_or(false),
                Le => l.partial_cmp(r).map(|o| o != Greater).unwrap_or(false),
                Gt => l.partial_cmp(r).map(|o| o == Greater).unwrap_or(false),
                Ge => l.partial_cmp(r).map(|o| o != Less).unwrap_or(false),
                _ => false,
            },
            // both absent → treat as equal
            (None, None) => matches!(self, Eq),
            // one absent, one present → not equal
            (None, Some(_)) | (Some(_), None) => matches!(self, Ne),
        }
    }

    /// Apply a filter against any ordered/hashable value type.
    ///
    /// Supports: Eq, Ne, Lt, Le, Gt, Ge, IsIn, IsNotIn, IsSome, IsNone.
    /// String and fuzzy operators return `false` — use `apply_to_property` for those.
    pub fn apply_value<T>(&self, left: &FilterValue<T>, right: Option<&T>) -> bool
    where
        T: PartialEq + PartialOrd + Eq + std::hash::Hash,
    {
        use std::cmp::Ordering::*;
        use FilterOperator::*;

        match left {
            FilterValue::None => match self {
                IsSome => right.is_some(),
                IsNone => right.is_none(),
                _ => false,
            },
            FilterValue::Single(lv) => {
                let Some(r) = right else {
                    return matches!(self, Ne);
                };
                match self {
                    Eq => r == lv,
                    Ne => r != lv,
                    Lt => r.partial_cmp(lv).map(|o| o == Less).unwrap_or(false),
                    Le => r.partial_cmp(lv).map(|o| o != Greater).unwrap_or(false),
                    Gt => r.partial_cmp(lv).map(|o| o == Greater).unwrap_or(false),
                    Ge => r.partial_cmp(lv).map(|o| o != Less).unwrap_or(false),
                    _ => false,
                }
            }
            FilterValue::Set(set) => match self {
                IsIn => right.map(|r| set.contains(r)).unwrap_or(false),
                IsNotIn => right.map(|r| !set.contains(r)).unwrap_or(false),
                _ => false,
            },
        }
    }
}
