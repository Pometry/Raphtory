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
    let whole = comparable_set_values(lhs, values);
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

/// What `any()`/`all()` take, the opening of every refusal of a misplaced one.
const QUALIFIER_NEEDS: &str = "any()/all() need one yes/no answer per element, which comparing \
                               a list or temporal property gives";

/// The type `any()`/`all()` produce over `inner`: one list level fewer, and
/// only over an element-wise yes/no result. A refusal says what the
/// expression gives instead and what to write.
pub(crate) fn qualified_type(inner: &PropType) -> Result<PropType, GraphError> {
    match inner {
        PropType::List(elem) if matches!(**elem, PropType::Bool) || is_elementwise_bool(elem) => {
            Ok((**elem).clone())
        }
        PropType::Bool => Err(GraphError::invalid_filter(format!(
            "{QUALIFIER_NEEDS}; this expression gives a single yes/no answer, so drop the \
             any()/all()"
        ))),
        PropType::List(_) => Err(GraphError::invalid_filter(format!(
            "{QUALIFIER_NEEDS}; this expression gives the list itself ({inner}), so compare it \
             first and put any()/all() after the comparison"
        ))),
        other => Err(GraphError::invalid_filter(format!(
            "{QUALIFIER_NEEDS}; this expression gives a single {other}, so compare it without \
             any()/all()"
        ))),
    }
}

/// The result type of comparing `lhs` with `rhs`, when both are known before
/// the filter meets a graph; `Empty` when either is not, or when the
/// comparison is refused (the refusal is reported when the filter is applied).
pub(crate) fn static_comparison_type(op: &BinaryOp, lhs: &PropType, rhs: &PropType) -> PropType {
    if !is_known(lhs) || !is_known(rhs) {
        return PropType::Empty;
    }
    comparison_shape(op, lhs, rhs)
        .map(|(out, _)| out)
        .unwrap_or(PropType::Empty)
}

/// Whether `pt` is fully known: no `Empty` anywhere inside it.
pub(crate) fn is_known(pt: &PropType) -> bool {
    match pt {
        PropType::Empty => false,
        PropType::List(inner) => is_known(inner),
        PropType::Map(fields) => fields.values().all(is_known),
        _ => true,
    }
}

pub(crate) fn require_bool(pt: &PropType, what: &str) -> Result<(), GraphError> {
    match pt {
        PropType::Bool => Ok(()),
        elementwise if is_elementwise_bool(elementwise) => {
            Err(GraphError::invalid_filter(format!(
                "{what} needs one yes/no answer, but comparing a list or temporal property gives \
             one per element ({pt}); add any() or all() to say whether any or every element \
             must match"
            )))
        }
        other => Err(GraphError::invalid_filter(format!(
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
        let needs = "Invalid filter: any()/all() need one yes/no answer per element, which \
                     comparing a list or temporal property gives; ";
        let refused = |pt: PropType| qualified_type(&pt).unwrap_err().to_string();
        assert_eq!(
            refused(PropType::Bool),
            format!("{needs}this expression gives a single yes/no answer, so drop the any()/all()")
        );
        // A list whose innermost type is not a yes/no is not an element-wise answer.
        assert_eq!(
            refused(list(list(PropType::Str))),
            format!(
                "{needs}this expression gives the list itself (List<List<Str>>), so compare it \
                 first and put any()/all() after the comparison"
            )
        );
        assert_eq!(
            refused(list(PropType::I64)),
            format!(
                "{needs}this expression gives the list itself (List<I64>), so compare it first \
                 and put any()/all() after the comparison"
            )
        );
        assert_eq!(
            refused(PropType::I64),
            format!("{needs}this expression gives a single I64, so compare it without any()/all()")
        );
    }

    #[test]
    fn a_filter_needs_one_yes_no_and_says_when_to_add_a_qualifier() {
        assert!(require_bool(&PropType::Bool, "a filter").is_ok());
        let refused = |pt: PropType| require_bool(&pt, "a filter").unwrap_err().to_string();
        let per_element = |pt: &str| {
            format!(
                "Invalid filter: a filter needs one yes/no answer, but comparing a list or \
                 temporal property gives one per element ({pt}); add any() or all() to say \
                 whether any or every element must match"
            )
        };
        assert_eq!(refused(list(PropType::Bool)), per_element("List<Bool>"));
        assert_eq!(
            refused(list(list(PropType::Bool))),
            per_element("List<List<Bool>>")
        );
        // A list of strings is not an element-wise yes/no, so the hint does not apply.
        assert_eq!(
            refused(list(list(PropType::Str))),
            "Invalid filter: a filter needs a yes/no answer, but this expression has type \
             List<List<Str>>"
        );
    }

    #[test]
    fn a_comparison_has_a_static_type_only_when_both_sides_are_known() {
        assert_eq!(
            static_comparison_type(&BinaryOp::Eq, &PropType::Str, &PropType::Str),
            PropType::Bool
        );
        assert_eq!(
            static_comparison_type(&BinaryOp::Eq, &list(PropType::I64), &PropType::I64),
            list(PropType::Bool)
        );
        assert_eq!(
            static_comparison_type(&BinaryOp::Eq, &PropType::Empty, &PropType::I64),
            PropType::Empty
        );
        assert_eq!(
            static_comparison_type(&BinaryOp::Eq, &list(PropType::Empty), &PropType::I64),
            PropType::Empty
        );
        // A refused comparison is left to be reported when the filter is applied.
        assert_eq!(
            static_comparison_type(&BinaryOp::Eq, &PropType::Str, &PropType::I64),
            PropType::Empty
        );
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
