//! Defines the `ViewApi` trait, which represents the API for querying a view of the graph.

pub mod edge;
pub mod graph;
pub mod internal;
pub mod layer;
pub mod time;
pub mod vertex;

pub use edge::EdgeListOps;
pub use graph::GraphViewOps;
pub use time::TimeOps;
pub use vertex::VertexListOps;
pub use vertex::VertexViewOps;

pub type BoxedIter<T> = Box<dyn Iterator<Item = T> + Send>;
