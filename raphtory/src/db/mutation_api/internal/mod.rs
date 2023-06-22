mod internal_addition_ops;
mod internal_deletion_ops;
mod internal_property_additions_ops;

use crate::db::view_api::internal::Inheritable;
pub use internal_addition_ops::*;
pub use internal_deletion_ops::*;
pub use internal_property_additions_ops::*;

pub trait InheritMutationOps: Inheritable {}

impl<G: InheritMutationOps> InheritAdditionOps for G {}
impl<G: InheritMutationOps> InheritDeletionOps for G {}
impl<G: InheritMutationOps> InheritPropertyAdditionOps for G {}
