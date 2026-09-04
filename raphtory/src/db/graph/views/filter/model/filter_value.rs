use std::{collections::HashSet, fmt, hash::Hash, sync::Arc};

/// A generic filter value container used by both property and attribute filters.
///
/// `T` is the value type being compared against (e.g. `Prop` for stored properties,
/// `usize` for degree, etc.).
#[derive(Debug, Clone)]
pub enum FilterValue<T> {
    /// Sentinel for `IS_SOME` / `IS_NONE` operators — no RHS value.
    None,
    /// Single value for equality/ordering comparisons.
    Single(T),
    /// Set of values for `IS_IN` / `IS_NOT_IN` comparisons.
    Set(Arc<HashSet<T>>),
}

impl<T: PartialEq + Eq + Hash> PartialEq for FilterValue<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FilterValue::None, FilterValue::None) => true,
            (FilterValue::Single(a), FilterValue::Single(b)) => a == b,
            (FilterValue::Set(a), FilterValue::Set(b)) => a == b,
            _ => false,
        }
    }
}

impl<T: PartialEq + Eq + Hash> Eq for FilterValue<T> {}

impl<T: fmt::Display + Eq + Hash + Ord> fmt::Display for FilterValue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterValue::None => write!(f, "<none>"),
            FilterValue::Single(v) => write!(f, "{}", v),
            FilterValue::Set(vs) => {
                let mut sorted: Vec<&T> = vs.iter().collect();
                sorted.sort();
                write!(
                    f,
                    "[{}]",
                    sorted
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    }
}
