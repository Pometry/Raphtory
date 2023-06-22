mod internal_addition_ops;
mod internal_deletion_ops;
mod internal_property_additions_ops;

pub use internal_addition_ops::*;
pub use internal_deletion_ops::*;
pub use internal_property_additions_ops::*;


pub trait InheritMutationOps {
    type Internal: InternalAdditionOps + InternalDeletionOps + InternalPropertyAdditionOps;
    
    fn graph(&self) -> &Self::Internal;
}

impl<G: InheritMutationOps> InheritAdditionOps for G {
    type Internal = G::Internal;

    fn graph(&self) -> &Self::Internal {
        self.graph()
    }
}

impl<G: InheritMutationOps> InheritDeletionOps for G {
    type Internal = G::Internal;

    fn graph(&self) -> &Self::Internal {
        self.graph()
    }
}

impl<G: InheritMutationOps> InheritPropertyAdditionOps for G {
    type Internal = G::Internal;

    fn graph(&self) -> &Self::Internal {
        self.graph()
    }
}
