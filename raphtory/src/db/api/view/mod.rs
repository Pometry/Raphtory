//! Defines the `ViewApi` trait, which represents the API for querying a view of the graph.

mod edge;
mod graph;
pub mod internal;
mod layer;
mod time;
mod vertex;

pub use edge::*;
pub use graph::*;
pub use layer::*;
pub use time::*;
pub use vertex::*;

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
