pub mod atomic_extra;
pub mod core;
#[cfg(feature = "python")]
pub mod python;

pub type BoxedIter<T> = Box<dyn Iterator<Item = T> + Send>;
pub type BoxedLIter<'a, T> = Box<dyn Iterator<Item = T> + Send + 'a>;

pub trait IntoDynBoxed<'a, T> {
    fn into_dyn_boxed(self) -> BoxedLIter<'a, T>;
}

impl<'a, T, I: Iterator<Item = T> + Send + 'a> IntoDynBoxed<'a, T> for I {
    fn into_dyn_boxed(self) -> BoxedLIter<'a, T> {
        Box::new(self)
    }
}
