use crate::db::graph::views::filter::model::FilterOperator;
use raphtory_api::core::entities::{GidRef, GID};
use std::{collections::HashSet, fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterValue {
    Single(String),
    Set(Arc<HashSet<String>>),
    ID(GID),
    IDSet(Arc<HashSet<GID>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    pub field_name: String,
    pub field_value: FilterValue,
    pub operator: FilterOperator,
}

impl Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.field_value {
            FilterValue::Single(value) => {
                write!(f, "{} {} {}", self.field_name, self.operator, value)
            }
            FilterValue::Set(values) => {
                let mut sorted: Vec<&String> = values.iter().collect();
                sorted.sort();
                let values_str = sorted
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} {} [{}]", self.field_name, self.operator, values_str)
            }
            FilterValue::ID(id) => {
                write!(f, "{} {} {}", self.field_name, self.operator, id)
            }
            FilterValue::IDSet(values) => {
                let mut sorted: Vec<&GID> = values.iter().collect();
                sorted.sort();
                let values_str = sorted
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} {} [{}]", self.field_name, self.operator, values_str)
            }
        }
    }
}

impl Filter {
    pub fn eq(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::Eq,
        }
    }

    pub fn ne(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::Ne,
        }
    }

    pub fn is_in(
        field_name: impl Into<String>,
        field_values: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Set(Arc::new(field_values.into_iter().collect())),
            operator: FilterOperator::IsIn,
        }
    }

    /// Is not in
    ///
    /// Arguments:
    ///     field_name (str)
    ///     field_values (list[str]):
    pub fn is_not_in(
        field_name: impl Into<String>,
        field_values: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Set(Arc::new(field_values.into_iter().collect())),
            operator: FilterOperator::IsNotIn,
        }
    }

    pub fn starts_with(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::StartsWith,
        }
    }

    pub fn ends_with(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::EndsWith,
        }
    }

    pub fn contains(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::Contains,
        }
    }

    pub fn not_contains(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::NotContains,
        }
    }

    /// Returns a filter expression that checks if the specified properties approximately match the specified string.
    ///
    /// Uses a specified Levenshtein distance and optional prefix matching.
    ///
    /// Arguments:
    ///     levenshtein_distance (int):
    ///     prefix_match (bool):
    ///
    /// Returns:
    ///     PropValue (str):
    pub fn fuzzy_search(
        field_name: impl Into<String>,
        field_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
        }
    }

    pub fn eq_id(field_name: impl Into<String>, field_value: impl Into<GID>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::ID(field_value.into()),
            operator: FilterOperator::Eq,
        }
    }

    pub fn ne_id(field_name: impl Into<String>, field_value: impl Into<GID>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::ID(field_value.into()),
            operator: FilterOperator::Ne,
        }
    }

    pub fn is_in_id<I, V>(field_name: impl Into<String>, field_values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<GID>,
    {
        let set: HashSet<GID> = field_values.into_iter().map(|x| x.into()).collect();
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::IDSet(Arc::new(set)),
            operator: FilterOperator::IsIn,
        }
    }

    pub fn is_not_in_id<I, V>(field_name: impl Into<String>, field_values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<GID>,
    {
        let set: HashSet<GID> = field_values.into_iter().map(|x| x.into()).collect();
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::IDSet(Arc::new(set)),
            operator: FilterOperator::IsNotIn,
        }
    }

    pub fn lt<V: Into<GID>>(field_name: impl Into<String>, field_value: V) -> Self {
        Filter {
            field_name: field_name.into(),
            field_value: FilterValue::ID(field_value.into()),
            operator: FilterOperator::Lt,
        }
        .into()
    }

    pub fn le<V: Into<GID>>(field_name: impl Into<String>, field_value: V) -> Self {
        Filter {
            field_name: field_name.into(),
            field_value: FilterValue::ID(field_value.into()),
            operator: FilterOperator::Le,
        }
        .into()
    }

    pub fn gt<V: Into<GID>>(field_name: impl Into<String>, field_value: V) -> Self {
        Filter {
            field_name: field_name.into(),
            field_value: FilterValue::ID(field_value.into()),
            operator: FilterOperator::Gt,
        }
        .into()
    }

    pub fn ge<V: Into<GID>>(field_name: impl Into<String>, field_value: V) -> Self {
        Filter {
            field_name: field_name.into(),
            field_value: FilterValue::ID(field_value.into()),
            operator: FilterOperator::Ge,
        }
        .into()
    }

    pub fn matches(&self, node_value: Option<&str>) -> bool {
        self.operator.apply(&self.field_value, node_value)
    }

    pub fn id_matches(&self, node_value: GidRef<'_>) -> bool {
        self.operator.apply_id(&self.field_value, node_value)
    }
}
