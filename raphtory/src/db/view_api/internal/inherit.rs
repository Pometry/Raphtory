pub trait Inheritable {
    type Base: ?Sized;

    fn base(&self) -> &Self::Base;
}
