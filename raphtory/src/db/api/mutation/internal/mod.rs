mod internal_addition_ops;
mod internal_deletion_ops;
mod internal_property_additions_ops;

pub use internal_addition_ops::*;
pub use internal_deletion_ops::*;
pub use internal_property_additions_ops::*;
use raphtory_memstorage::db::api::view::internal::inherit::Base;

pub trait InheritMutationOps: Base {}

impl<G: InheritMutationOps> InheritAdditionOps for G {}
impl<G: InheritMutationOps> InheritDeletionOps for G {}
impl<G: InheritMutationOps> InheritPropertyAdditionOps for G {}
