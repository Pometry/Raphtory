/// Get a base for inheriting methods
pub trait Base {
    type Base: ?Sized;

    fn base(&self) -> &Self::Base;
}
