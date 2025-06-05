use std::ops::Deref;

/// Get a base for inheriting methods
pub trait Base {
    type Base: ?Sized;

    fn base(&self) -> &Self::Base;
}

/// Deref implies Base
impl<T: Deref> Base for T {
    type Base = T::Target;

    #[inline(always)]
    fn base(&self) -> &Self::Base {
        self.deref()
    }
}
