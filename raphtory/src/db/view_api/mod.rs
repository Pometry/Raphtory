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
pub use vertex::*;

pub type BoxedIter<T> = Box<dyn Iterator<Item = T> + Send>;
