pub use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::filter::{
            model::{
                edge_filter::EdgeFilter,
                exploded_edge_filter::ExplodedEdgeFilter,
                filter_operator::{
                    BinaryOp, Comparable, FilterOperator, SetOp, StringComparable, StringOp,
                    UnaryOp,
                },
                node_expr::EntityAggOps,
                node_filter::{NodeFilter, NodeFilterFactory},
            },
            CreateFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::Prop;

pub mod edge_expr;
pub mod edge_filter;
pub mod exploded_edge_filter;
pub mod expr;
pub mod filter_operator;
pub mod graph_filter;
pub mod node_expr;
pub mod node_filter;
pub mod node_state_filter;
pub mod property_filter;

pub use expr::builder::{
    ComposableFilter, EdgeViewFilterOps, EntityExprFilterOps, PropertyExprFactory, ViewWrapOps,
};

use crate::db::graph::views::filter::model::expr::Agg;
use raphtory_api::core::entities::properties::prop::{unify_types, PropType};

/// Reject ordering operators on a type that has no ordering.
///
/// An unresolved type (`PropType::Empty`) passes; the check runs again once
/// the type is known.
pub fn validate_binary_op(op: &BinaryOp, prop_type: &PropType) -> Result<(), GraphError> {
    let ordering = matches!(
        op,
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    );
    if ordering && !prop_type.has_cmp() {
        let kind = match prop_type {
            PropType::List(_) => "list".to_string(),
            PropType::Map(_) => "map".to_string(),
            other => other.to_string(),
        };
        return Err(GraphError::InvalidFilter(format!(
            "operator {op} is not valid for {kind} properties"
        )));
    }
    Ok(())
}

/// A string operator applied to a value that is not a string.
pub fn not_a_string_error(prop_type: &PropType) -> GraphError {
    GraphError::InvalidFilter(format!(
        "string operator requires a Str property, but the property type is {prop_type}"
    ))
}

/// The type of an expression, from what the tree knows before any graph is
/// at hand (`expr_pt`) and what the op it compiled to reports (`op_pt`).
///
/// Each side may know only part of the type: a temporal property term knows
/// it is a list (`List<Empty>`) but not of what, while its op knows the
/// element type; an `is_active()` term knows it is a `Bool`, while the op it
/// compiles to, a mapped history, reports nothing. The two are unified, an
/// unknown part on one side filled in from the other, at any depth of lists
/// and maps. When they conflict, the tree's type is kept: an op can report the
/// type of an op it wraps rather than its own.
pub fn resolved_prop_type(expr_pt: PropType, op_pt: PropType) -> PropType {
    // `unify_types` reports whether it filled anything in; nothing here
    // depends on that.
    let mut unified = false;
    unify_types(&expr_pt, &op_pt, &mut unified).unwrap_or(expr_pt)
}

/// Reject a constant compared against an expression whose type it can never
/// equal.
///
/// Constants are never converted: a numeric constant compares by value with
/// any numeric expression (`degree() > 2.5` keeps the `.5`), a string constant
/// never compares with a number, and so on. A missing constant (`None`) and an
/// unresolved expression type both pass.
pub fn validate_const_comparable(
    lhs_pt: &PropType,
    value: Option<&Prop>,
) -> Result<(), GraphError> {
    match value {
        Some(v) if !lhs_pt.is_comparable_with(&v.dtype()) => Err(const_mismatch_error(v, lhs_pt)),
        _ => Ok(()),
    }
}

/// A constant compared with an expression whose type it can never equal.
pub fn const_mismatch_error(value: &Prop, expected: &PropType) -> GraphError {
    GraphError::InvalidFilter(format!(
        "value {value} of type {} cannot be compared with {expected}",
        value.dtype()
    ))
}

/// Two expressions whose types can never be equal.
pub fn types_mismatch_error(lhs_pt: &PropType, rhs_pt: &PropType) -> GraphError {
    GraphError::InvalidFilter(format!("type mismatch: lhs is {lhs_pt}, rhs is {rhs_pt}"))
}

/// Reject an aggregate the expression's type cannot support.
///
/// Aggregates collapse a list, so a declared scalar type (`is_active()` ->
/// `Bool`, `degree()` -> `U64`) is refused up front. `sum()`, `avg()`,
/// `min()` and `max()` also need numeric elements, as they always have: a
/// string or boolean history has no sum and no least value. An unresolved
/// type (`PropType::Empty`) passes and is checked again once known.
pub fn require_aggregable(pt: &PropType, agg: Agg) -> Result<(), GraphError> {
    let op = format!("{}()", agg.name());
    match pt {
        PropType::Empty => Ok(()),
        PropType::List(_) => {
            let elem = innermost_element(pt);
            if matches!(agg, Agg::Sum | Agg::Avg | Agg::Min | Agg::Max)
                && !(elem.is_numeric() || elem.is_unknown())
            {
                return Err(GraphError::InvalidFilter(format!(
                    "{op} requires numeric values, but the elements are {elem}"
                )));
            }
            Ok(())
        }
        _ => Err(GraphError::InvalidFilter(format!(
            "{op} is not valid on a scalar expression of type {pt}"
        ))),
    }
}

/// The element type under every list level of `pt`.
fn innermost_element(pt: &PropType) -> &PropType {
    match pt {
        PropType::List(inner) => innermost_element(inner),
        other => other,
    }
}

/// Narrow an `is_in`/`is_not_in` set to the members that could equal the LHS.
///
/// Set membership asks whether a value is present, so a member of a type the
/// LHS can never equal simply is not present: it is dropped rather than
/// rejected, leaving `is_in` answering "no" where a comparison would refuse
/// the question. The members that remain are kept exactly as written; the
/// runtime comparison handles mixed numeric widths by value. An unresolved
/// LHS type keeps every member.
pub fn comparable_set_values(lhs_pt: &PropType, values: &[Prop]) -> Vec<Prop> {
    values
        .iter()
        .filter(|v| lhs_pt.is_comparable_with(&v.dtype()))
        .cloned()
        .collect()
}
