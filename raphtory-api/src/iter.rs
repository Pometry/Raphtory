pub type BoxedLIter<'a, T> = Box<dyn Iterator<Item = T> + Send + Sync + 'a>;
pub type BoxedLDIter<'a, T> = Box<dyn DoubleEndedIterator<Item = T> + Send + Sync + 'a>;
pub type BoxedIter<T> = BoxedLIter<'static, T>;

pub trait IntoDynBoxed<'a, T> {
    fn into_dyn_boxed(self) -> BoxedLIter<'a, T>;
}

impl<'a, T, I: Iterator<Item = T> + Send + Sync + 'a> IntoDynBoxed<'a, T> for I {
    fn into_dyn_boxed(self) -> BoxedLIter<'a, T> {
        Box::new(self)
    }
}

pub trait IntoDynDBoxed<'a, T> {
    fn into_dyn_dboxed(self) -> BoxedLDIter<'a, T>;
}

impl<'a, T, I: DoubleEndedIterator<Item = T> + Send + Sync + 'a> IntoDynDBoxed<'a, T> for I {
    fn into_dyn_dboxed(self) -> BoxedLDIter<'a, T> {
        Box::new(self)
    }
}
