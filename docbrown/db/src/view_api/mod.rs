//! Defines the `ViewApi` trait, which represents the API for querying a view of the graph.

pub mod edge;
pub mod graph;
pub mod internal;
pub mod vertex;

pub use edge::EdgeListOps;
pub use graph::GraphViewOps;
pub use vertex::VertexListOps;
