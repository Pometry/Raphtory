//! The predicate forms of the typed expression API: a comparison, a string
//! test, a presence test and a set membership test. They are builders: each
//! converts to the filter expression tree (see `model::expr::convert`) and
//! compiles through it, so what a rust caller writes and what a python object
//! or GraphQL request carries run through the same compiler.

use super::{EntityExpr, Marker, PredicateLhs};
use crate::db::graph::views::filter::model::{
    filter_operator::{BinaryOp, SetOp, StringOp, UnaryOp},
    ComposableFilter,
};
use raphtory_api::core::entities::properties::prop::Prop;

/// Two values compared with a [`BinaryOp`]; either side may be a constant.
///
/// ```rust,ignore
/// NodeFilter.degree().gt(2usize)
/// NodeFilter.property("age").eq(30i64)
/// NodeFilter.out_degree().gt(NodeFilter.in_degree())
/// ```
#[derive(Clone)]
pub struct BinaryCmpExpr<L, R, Entity> {
    pub left: L,
    pub op: BinaryOp,
    pub right: R,
    pub entity: Entity,
}

impl<L, R, E> BinaryCmpExpr<L, R, E> {
    pub fn new(left: L, op: BinaryOp, right: R, entity: E) -> Self {
        Self {
            left,
            op,
            right,
            entity,
        }
    }
}

impl<L, R, E> ComposableFilter for BinaryCmpExpr<L, R, E> {}

impl<L: EntityExpr, R: EntityExpr, E: Marker> PredicateLhs for BinaryCmpExpr<L, R, E> {}

impl<L: EntityExpr, R: EntityExpr, E: Marker> EntityExpr for BinaryCmpExpr<L, R, E> {
    type Marker = E;

    fn entity(&self) -> Self::Marker {
        self.entity
    }

    fn nullable(&self) -> bool {
        false
    }
}

/// A presence test: `is_some()` / `is_none()`.
#[derive(Clone)]
pub struct UnaryExpr<E, Entity> {
    pub expr: E,
    pub op: UnaryOp,
    pub entity: Entity,
}

impl<E, Entity> ComposableFilter for UnaryExpr<E, Entity> {}

impl<E: EntityExpr, M: Marker> PredicateLhs for UnaryExpr<E, M> {}

impl<E: EntityExpr, M: Marker> EntityExpr for UnaryExpr<E, M> {
    type Marker = M;

    fn entity(&self) -> Self::Marker {
        self.entity
    }

    fn nullable(&self) -> bool {
        false
    }
}

/// A string test with a [`StringOp`]: `starts_with`, `contains`, `fuzzy_search`, …
#[derive(Clone)]
pub struct StringExpr<L, R, Entity> {
    pub left: L,
    pub op: StringOp,
    pub right: R,
    pub entity: Entity,
}

impl<L, R, Entity> StringExpr<L, R, Entity> {
    pub fn new(left: L, op: StringOp, right: R, entity: Entity) -> Self {
        Self {
            left,
            op,
            right,
            entity,
        }
    }
}

impl<L, R, Entity> ComposableFilter for StringExpr<L, R, Entity> {}

impl<L: EntityExpr, R: EntityExpr, M: Marker> PredicateLhs for StringExpr<L, R, M> {}

impl<L: EntityExpr, R: EntityExpr, M: Marker> EntityExpr for StringExpr<L, R, M> {
    type Marker = M;

    fn entity(&self) -> Self::Marker {
        self.entity
    }

    fn nullable(&self) -> bool {
        false
    }
}

/// A membership test against a fixed set of values: `is_in` / `is_not_in`.
#[derive(Clone)]
pub struct PropValueSetExpr<E, Entity> {
    pub(crate) expr: E,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
    pub(crate) entity: Entity,
}

impl<E, Entity> ComposableFilter for PropValueSetExpr<E, Entity> {}

impl<E: EntityExpr, M: Marker> PredicateLhs for PropValueSetExpr<E, M> {}

impl<E: EntityExpr, M: Marker> EntityExpr for PropValueSetExpr<E, M> {
    type Marker = M;

    fn entity(&self) -> Self::Marker {
        self.entity
    }

    fn nullable(&self) -> bool {
        false
    }
}
