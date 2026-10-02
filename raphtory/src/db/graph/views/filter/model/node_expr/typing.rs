//! The type rules of the yes/no expressions, and the kernels that evaluate
//! them once a graph has fixed the property types.
//!
//! A comparison of two comparable values is one yes/no answer. A comparison of
//! a list-valued side against a value its elements are comparable with is one
//! answer per element, a `List<Bool>`, which `any()` or `all()` turn into one.
//! The shape is decided here, from the types alone, when the expression is
//! built against a graph; the caller words a mismatch, since it is the one
//! holding the constant a message can name.

use crate::{
    db::graph::views::filter::model::{
        comparable_set_values, const_mismatch_error,
        filter_operator::{BinaryOp, Comparable, StringComparable, StringOp, UnaryOp},
        node_expr::ops::{broadcast_binary, broadcast_unary},
        not_a_string_error, types_mismatch_error, validate_binary_op,
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::{prop_hashable::HashableProp, Prop, PropType};
use std::collections::HashSet;

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

/// Why two sides cannot be tested against each other.
#[derive(Debug)]
pub(crate) enum ShapeError {
    /// The other side can never equal a value of this type.
    Mismatch(PropType),
    /// The operator does not apply to the type, whatever the other side.
    Invalid(GraphError),
}

impl ShapeError {
    /// The error to report for a test against `rhs`, naming the constant when
    /// the right side is one.
    pub(crate) fn into_error(self, rhs: &PropType, constant: Option<&Prop>) -> GraphError {
        match self {
            ShapeError::Invalid(error) => error,
            ShapeError::Mismatch(expected) => match constant {
                Some(value) => const_mismatch_error(value, &expected),
                None => types_mismatch_error(&expected, rhs),
            },
        }
    }
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
) -> Result<(PropType, Shape), ShapeError> {
    if lhs.is_comparable_with(rhs) {
        validate_binary_op(op, lhs).map_err(ShapeError::Invalid)?;
        return Ok((PropType::Bool, Shape::Whole));
    }
    if let PropType::List(inner) = lhs {
        if let Ok((out, _)) = comparison_shape(op, inner, rhs) {
            return Ok((list(out), Shape::Elementwise));
        }
    }
    if let PropType::List(inner) = rhs {
        if let Ok((out, _)) = comparison_shape(op, lhs, inner) {
            return Ok((list(out), Shape::Elementwise));
        }
    }
    Err(ShapeError::Mismatch(lhs.clone()))
}

/// The result type of a string test: the left side must be a string, or a
/// list whose elements are, and the right side a string.
pub(crate) fn string_shape(
    lhs: &PropType,
    rhs: &PropType,
) -> Result<(PropType, Shape), ShapeError> {
    if lhs.is_unknown() || lhs.is_str() {
        if !PropType::Str.is_comparable_with(rhs) {
            return Err(ShapeError::Mismatch(PropType::Str));
        }
        return Ok((PropType::Bool, Shape::Whole));
    }
    if let PropType::List(inner) = lhs {
        if let Ok((out, _)) = string_shape(inner, rhs) {
            return Ok((list(out), Shape::Elementwise));
        }
    }
    Err(ShapeError::Invalid(not_a_string_error(lhs)))
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

// ── kernels ──────────────────────────────────────────────────────────────────
//
// Plain functions, one per shape, chosen once when the op is built: the op
// holds a pointer to the one that applies, and no per-entity call asks which
// shape it is.

/// A kernel over two values, with the test's own parameter (its operator, or
/// its member set) passed alongside.
pub(crate) type BinaryKernel<P> = fn(&P, Option<Prop>, Option<Prop>) -> Option<Prop>;

/// A kernel over one value.
pub(crate) type UnaryKernel<P> = fn(&P, Option<Prop>) -> Option<Prop>;

fn cmp_whole(op: &BinaryOp, l: Option<Prop>, r: Option<Prop>) -> Option<Prop> {
    Some(Prop::Bool(Option::<Prop>::binary_cmp(op, &l, &r)))
}

fn cmp_elementwise(op: &BinaryOp, l: Option<Prop>, r: Option<Prop>) -> Option<Prop> {
    broadcast_binary(l, r, &|l, r| {
        Some(Prop::Bool(Prop::binary_cmp(op, &l?, &r?)))
    })
}

/// The comparison kernel for `shape`.
pub(crate) fn cmp_kernel(shape: Shape) -> BinaryKernel<BinaryOp> {
    match shape {
        Shape::Whole => cmp_whole,
        Shape::Elementwise => cmp_elementwise,
    }
}

fn str_whole(op: &StringOp, l: Option<Prop>, r: Option<Prop>) -> Option<Prop> {
    Some(Prop::Bool(Option::<Prop>::string_cmp(op, &l, &r)))
}

fn str_elementwise(op: &StringOp, l: Option<Prop>, r: Option<Prop>) -> Option<Prop> {
    broadcast_binary(l, r, &|l, r| {
        Some(Prop::Bool(Option::<Prop>::string_cmp(op, &l, &r)))
    })
}

/// The string-test kernel for `shape`.
pub(crate) fn str_kernel(shape: Shape) -> BinaryKernel<StringOp> {
    match shape {
        Shape::Whole => str_whole,
        Shape::Elementwise => str_elementwise,
    }
}

/// The members of a membership test, hashed once, and whether the test is
/// for absence.
pub(crate) struct SetMembers {
    members: HashSet<HashableProp>,
    negated: bool,
}

impl SetMembers {
    pub(crate) fn new(values: Vec<Prop>, negated: bool) -> Self {
        SetMembers {
            members: values.into_iter().map(HashableProp).collect(),
            negated,
        }
    }

    fn holds(&self, v: Option<Prop>) -> Option<Prop> {
        let present = self.members.contains(&HashableProp(v?));
        Some(Prop::Bool(present != self.negated))
    }
}

fn set_whole(set: &SetMembers, v: Option<Prop>) -> Option<Prop> {
    set.holds(v)
}

fn set_elementwise(set: &SetMembers, v: Option<Prop>) -> Option<Prop> {
    broadcast_unary(v, |v| set.holds(v))
}

/// The membership kernel for `shape`.
pub(crate) fn set_kernel(shape: Shape) -> UnaryKernel<SetMembers> {
    match shape {
        Shape::Whole => set_whole,
        Shape::Elementwise => set_elementwise,
    }
}

/// Whether a value is present (`is_some`) or missing (`is_none`).
pub(crate) fn presence_kernel(op: &UnaryOp, v: Option<Prop>) -> Option<Prop> {
    Some(Prop::Bool(match op {
        UnaryOp::IsSome => v.is_some(),
        UnaryOp::IsNone => v.is_none(),
    }))
}

/// The opposite of a yes/no value.
pub(crate) fn not_kernel(_: &(), v: Option<Prop>) -> Option<Prop> {
    Some(Prop::Bool(!truthy(&v)))
}
