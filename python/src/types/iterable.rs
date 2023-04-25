use crate::types::repr::{iterator_repr, Repr};
use raphtory::db::view_api::BoxedIter;
use std::sync::Arc;

pub struct Iterable<I: Send> {
    pub name: String,
    pub builder: Arc<dyn Fn() -> BoxedIter<I> + Send + Sync + 'static>,
}

impl<I: Send> Iterable<I> {
    pub fn iter(&self) -> BoxedIter<I> {
        (self.builder)()
    }
    pub fn new<F: Fn() -> BoxedIter<I> + Send + Sync + 'static>(name: String, builder: F) -> Self {
        Self {
            name,
            builder: Arc::new(builder),
        }
    }
}

impl<I: Send + Repr> Repr for Iterable<I> {
    fn repr(&self) -> String {
        format!("{}([{}])", self.name, iterator_repr(self.iter()))
    }
}

pub struct NestedIterable<I: Send> {
    pub name: String,
    pub builder: Arc<dyn Fn() -> BoxedIter<BoxedIter<I>> + Send + Sync + 'static>,
}

impl<I: Send> NestedIterable<I> {
    pub fn iter(&self) -> BoxedIter<BoxedIter<I>> {
        (self.builder)()
    }
    pub fn new<F: Fn() -> BoxedIter<BoxedIter<I>> + Send + Sync + 'static>(
        name: String,
        builder: F,
    ) -> Self {
        Self {
            name,
            builder: Arc::new(builder),
        }
    }
}

impl<I: Send + Repr> Repr for NestedIterable<I> {
    fn repr(&self) -> String {
        format!(
            "{}([{}])",
            self.name,
            iterator_repr(self.iter().map(|it| format!("[{}]", iterator_repr(it))))
        )
    }
}
