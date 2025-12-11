use crate::{
    db::graph::views::filter::model::{
        filter::FilterValue,
        FilterOperator::{
            Contains, EndsWith, Eq, Ge, Gt, IsIn, IsNone, IsNotIn, IsSome, Le, Lt, Ne, NotContains,
            StartsWith, *,
        },
    },
    errors::GraphError,
    prelude::Filter,
};
use raphtory_api::core::entities::{
    GidType,
    GidType::{Str, U64},
    GID,
};

pub fn validate(id_dtype: Option<GidType>, filter: &Filter) -> Result<(), GraphError> {
    let Some(kind) = id_dtype else {
        return Ok(());
    };

    fn filter_value_kind(fv: &FilterValue) -> &'static str {
        match fv {
            FilterValue::ID(GID::U64(_)) => "U64",
            FilterValue::ID(GID::Str(_)) => "Str",
            FilterValue::IDSet(set) => {
                if set.iter().all(|g| matches!(g, GID::U64(_))) {
                    "U64"
                } else if set.iter().all(|g| matches!(g, GID::Str(_))) {
                    "Str"
                } else {
                    "heterogeneous id set"
                }
            }
            FilterValue::Single(_) => "Str",
            FilterValue::Set(_) => "Str",
        }
    }

    let value_matches_kind = |fv: &FilterValue, expect: GidType| -> bool {
        match (fv, expect) {
            (FilterValue::ID(GID::U64(_)), U64) => true,
            (FilterValue::IDSet(set), U64) => set.iter().all(|g| matches!(g, GID::U64(_))),

            (FilterValue::ID(GID::Str(_)), Str) => true,
            (FilterValue::IDSet(set), Str) => set.iter().all(|g| matches!(g, GID::Str(_))),
            (FilterValue::Single(_), Str) => true,
            (FilterValue::Set(_), Str) => true,

            _ => false,
        }
    };

    let op_allowed = match kind {
        U64 => matches!(
            filter.operator,
            Eq | Ne | Lt | Le | Gt | Ge | IsIn | IsNotIn
        ),
        Str => matches!(
            filter.operator,
            Eq | Ne
                | StartsWith
                | EndsWith
                | Contains
                | NotContains
                | FuzzySearch { .. }
                | IsIn
                | IsNotIn
        ),
    };

    if !op_allowed {
        return Err(GraphError::InvalidGqlFilter(format!(
            "Operator {} not allowed for {:?} ID",
            filter.operator, kind
        )));
    }

    if !value_matches_kind(&filter.field_value, kind) {
        return Err(GraphError::InvalidGqlFilter(format!(
            "Filter value type does not match node ID type. Expected {:?} but got {:?}",
            kind,
            filter_value_kind(&filter.field_value)
        )));
    }

    match filter.operator {
        IsIn | IsNotIn => {
            if !matches!(
                filter.field_value,
                FilterValue::IDSet(_) | FilterValue::Set(_)
            ) {
                return Err(GraphError::InvalidGqlFilter(
                    "IN/NOT_IN on ID expects a set of IDs".into(),
                ));
            }
        }
        StartsWith | EndsWith | Contains | NotContains | FuzzySearch { .. } => {
            if !matches!(
                filter.field_value,
                FilterValue::ID(GID::Str(_)) | FilterValue::Single(_)
            ) {
                return Err(GraphError::InvalidGqlFilter(
                    "String operators on ID expect a single string ID".into(),
                ));
            }
        }
        Lt | Le | Gt | Ge => {
            if !matches!(filter.field_value, FilterValue::ID(GID::U64(_))) {
                return Err(GraphError::InvalidGqlFilter(
                    "Numeric operators on ID expect a single numeric (u64) ID".into(),
                ));
            }
        }
        IsSome | IsNone => {
            return Err(GraphError::InvalidGqlFilter(
                "IsSome/IsNone are not supported as filter operators".into(),
            ));
        }
        Eq | Ne => {
            // Eq/Ne already type-checked above
        }
    }

    Ok(())
}
