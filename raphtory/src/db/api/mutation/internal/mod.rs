mod internal_addition_ops;
mod internal_deletion_ops;
mod internal_property_additions_ops;

use crate::db::api::view::internal::Base;
pub use internal_addition_ops::*;
pub use internal_deletion_ops::*;
pub use internal_property_additions_ops::*;

pub trait InheritMutationOps: Base {}

impl<G: InheritMutationOps> InheritAdditionOps for G {}
impl<G: InheritMutationOps> InheritDeletionOps for G {}
impl<G: InheritMutationOps> InheritPropertyAdditionOps for G {}
