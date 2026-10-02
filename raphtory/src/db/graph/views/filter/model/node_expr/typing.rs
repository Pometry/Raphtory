//! The type rules of the yes/no expressions, and the kernels that evaluate
//! them once a graph has fixed the property types.
//!
//! A comparison of two comparable values is one yes/no answer. A comparison of
//! a list-valued side against a value its elements are comparable with is one
//! answer per element, a `List<Bool>`, which `any()` or `all()` turn into one.
//! The shape is decided here, when the expression is built against a graph,
//! because only then are property types known.

use crate::{
    db::graph::views::filter::model::{
        comparable_set_values,
        filter_operator::{BinaryOp, Comparable, StringComparable, StringOp},
        node_expr::ops::{broadcast_binary, broadcast_unary},
        validate_binary_op, validate_const_comparable, validate_string_op,
        validate_types_comparable,
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::{prop_hashable::HashableProp, Prop, PropType};
use std::{collections::HashSet, sync::Arc};

fn invalid(msg: impl Into<String>) -> GraphError {
    GraphError::InvalidFilter(msg.into())
}

/// How a two-sided test evaluates: on the whole values, or once per element
/// of a list-valued side.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Shape {
    Whole,
    Elementwise,
}

pub(crate) fn list(inner: PropType) -> PropType {
    PropType::List(Box::new(inner))
}

/// The result type of comparing `lhs` with `rhs`: `Bool` when the two are
/// comparable as they stand, `List<Bool>` (nested to match) when a list-valued
/// side is compared element by element against the other.
pub(crate) fn comparison_shape(
    op: &BinaryOp,
    lhs: &PropType,
    rhs: &PropType,
    rhs_const: Option<&Prop>,
) -> Result<(PropType, Shape), GraphError> {
    if lhs.is_comparable_with(rhs) {
        validate_binary_op(op, lhs)?;
        return Ok((PropType::Bool, Shape::Whole));
    }
    if let PropType::List(inner) = lhs {
        if let Ok((out, _)) = comparison_shape(op, inner, rhs, rhs_const) {
            return Ok((list(out), Shape::Elementwise));
        }
    }
    if let PropType::List(inner) = rhs {
        if let Ok((out, _)) = comparison_shape(op, lhs, inner, None) {
            return Ok((list(out), Shape::Elementwise));
        }
    }
    let mismatch = match rhs_const {
        Some(value) => validate_const_comparable(lhs, Some(value)),
        None => validate_types_comparable(lhs, rhs),
    };
    Err(mismatch
        .err()
        .unwrap_or_else(|| invalid(format!("type mismatch: lhs is {lhs}, rhs is {rhs}"))))
}

/// The result type of a string test: the left side must be a string, or a
/// list whose elements are, and the right side a string.
pub(crate) fn string_shape(
    lhs: &PropType,
    rhs: &PropType,
    rhs_const: Option<&Prop>,
) -> Result<(PropType, Shape), GraphError> {
    if lhs.is_unknown() || lhs.is_str() {
        match rhs_const {
            Some(value) => validate_const_comparable(&PropType::Str, Some(value))?,
            None => validate_types_comparable(&PropType::Str, rhs)?,
        }
        return Ok((PropType::Bool, Shape::Whole));
    }
    if let PropType::List(inner) = lhs {
        if let Ok((out, _)) = string_shape(inner, rhs, rhs_const) {
            return Ok((list(out), Shape::Elementwise));
        }
    }
    Err(validate_string_op(lhs).err().unwrap_or_else(|| {
        invalid(format!(
            "string operator requires a Str property, but the property type is {lhs}"
        ))
    }))
}

/// The result type of a membership test, and the members that can match. A
/// list-valued side whose whole value no member can equal is tested element
/// by element instead.
pub(crate) fn set_shape(lhs: &PropType, values: &[Prop]) -> (PropType, Shape, Vec<Prop>) {
    let whole = comparable_set_values(lhs, values.to_vec());
    if let PropType::List(inner) = lhs {
        if whole.is_empty() && !values.is_empty() {
            let (out, _, members) = set_shape(inner, values);
            return (list(out), Shape::Elementwise, members);
        }
    }
    (PropType::Bool, Shape::Whole, whole)
}

/// Whether `pt` is one yes/no per element, at any list depth: a `List<Bool>`,
/// or a list of those, with `Bool` innermost.
fn is_elementwise_bool(pt: &PropType) -> bool {
    match pt {
        PropType::List(inner) => matches!(**inner, PropType::Bool) || is_elementwise_bool(inner),
        _ => false,
    }
}

/// The type `any()`/`all()` produce over `inner`: one list level fewer, and
/// only over an element-wise yes/no result.
pub(crate) fn qualified_type(inner: &PropType) -> Result<PropType, GraphError> {
    match inner {
        PropType::List(elem) if matches!(**elem, PropType::Bool) || is_elementwise_bool(elem) => {
            Ok((**elem).clone())
        }
        other => Err(invalid(format!(
            "any()/all() collapse an element-wise comparison (a list of yes/no answers), \
             but this expression has type {other}"
        ))),
    }
}

pub(crate) fn require_bool(pt: &PropType, what: &str) -> Result<(), GraphError> {
    match pt {
        PropType::Bool => Ok(()),
        elementwise if is_elementwise_bool(elementwise) => Err(invalid(format!(
            "{what} needs a yes/no answer, but this comparison gives one answer per \
             element ({pt}); add any() or all() to say which elements must match"
        ))),
        other => Err(invalid(format!(
            "{what} needs a yes/no answer, but this expression has type {other}"
        ))),
    }
}

#[cfg(test)]
mod shape_tests {
    use super::*;

    #[test]
    fn any_and_all_collapse_only_an_elementwise_yes_no() {
        assert_eq!(
            qualified_type(&list(PropType::Bool)).unwrap(),
            PropType::Bool
        );
        assert_eq!(
            qualified_type(&list(list(PropType::Bool))).unwrap(),
            list(PropType::Bool)
        );
        // A list whose innermost type is not a yes/no is not an element-wise answer.
        assert!(qualified_type(&list(list(PropType::Str))).is_err());
        assert!(qualified_type(&list(PropType::I64)).is_err());
        assert!(qualified_type(&PropType::Bool).is_err());
    }

    #[test]
    fn a_filter_needs_one_yes_no_and_says_when_to_add_a_qualifier() {
        assert!(require_bool(&PropType::Bool, "a filter").is_ok());
        let per_element = require_bool(&list(PropType::Bool), "a filter").unwrap_err();
        assert!(per_element.to_string().contains("add any() or all()"));
        let nested = require_bool(&list(list(PropType::Bool)), "a filter").unwrap_err();
        assert!(nested.to_string().contains("add any() or all()"));
        // A list of strings is not an element-wise yes/no, so the hint does not apply.
        let strings = require_bool(&list(list(PropType::Str)), "a filter").unwrap_err();
        assert!(!strings.to_string().contains("add any() or all()"));
    }
}

pub(crate) fn truthy(v: &Option<Prop>) -> bool {
    matches!(v, Some(Prop::Bool(true)))
}

pub(crate) fn cmp_kernel(
    op: BinaryOp,
    shape: Shape,
) -> impl Fn(Option<Prop>, Option<Prop>) -> Option<Prop> + Clone {
    move |l, r| match shape {
        Shape::Whole => Some(Prop::Bool(Option::<Prop>::binary_cmp(&op, &l, &r))),
        Shape::Elementwise => broadcast_binary(l, r, &|l, r| {
            Some(Prop::Bool(Prop::binary_cmp(&op, &l?, &r?)))
        }),
    }
}

pub(crate) fn str_kernel(
    op: StringOp,
    shape: Shape,
) -> impl Fn(Option<Prop>, Option<Prop>) -> Option<Prop> + Clone {
    move |l, r| match shape {
        Shape::Whole => Some(Prop::Bool(Option::<Prop>::string_cmp(&op, &l, &r))),
        Shape::Elementwise => broadcast_binary(l, r, &|l, r| {
            Some(Prop::Bool(Option::<Prop>::string_cmp(&op, &l, &r)))
        }),
    }
}

pub(crate) fn set_kernel(
    values: Vec<Prop>,
    negated: bool,
    shape: Shape,
) -> impl Fn(Option<Prop>) -> Option<Prop> + Clone {
    let values: Arc<HashSet<HashableProp>> =
        Arc::new(values.into_iter().map(HashableProp).collect());
    move |v| {
        let member = |v: Option<Prop>| {
            let present = values.contains(&HashableProp(v?));
            Some(Prop::Bool(present != negated))
        };
        match shape {
            Shape::Whole => member(v),
            Shape::Elementwise => broadcast_unary(v, member),
        }
    }
}

pub(crate) fn not_kernel(v: Option<Prop>) -> Option<Prop> {
    Some(Prop::Bool(!truthy(&v)))
}
