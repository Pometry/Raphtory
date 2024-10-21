//! Defines the `ViewApi` trait, which represents the API for querying a view of the graph.

mod edge;
mod edge_property_filter;
mod exploded_edge_property_filter;
mod graph;
pub mod internal;
mod layer;
pub(crate) mod node;
mod reset_filter;
pub(crate) mod time;

pub(crate) use edge::BaseEdgeViewOps;
pub use edge::EdgeViewOps;

pub use edge_property_filter::EdgePropertyFilterOps;
pub use exploded_edge_property_filter::ExplodedEdgePropertyFilterOps;
pub use graph::*;
pub use internal::{
    Base, BoxableGraphView, DynamicGraph, InheritViewOps, IntoDynamic, MaterializedGraph,
};
pub use layer::*;
pub(crate) use node::BaseNodeViewOps;
pub use node::NodeViewOps;
pub use reset_filter::*;
pub use time::*;
