//! Defines the `ViewApi` trait, which represents the API for querying a view of the graph.

mod edge;
mod edge_property_filter;
// mod exploded_edge_property_filter;
pub(crate) mod graph;
pub mod history;
pub mod internal;
mod layer;
pub(crate) mod node;
mod node_property_filter;
mod reset_filter;
pub(crate) mod time;

pub(crate) use edge::BaseEdgeViewOps;
pub use edge::EdgeViewOps;

pub use edge_property_filter::EdgePropertyFilterOps;
// pub use exploded_edge_property_filter::ExplodedEdgePropertyFilterOps;
pub use graph::*;
pub use internal::{
    Base, BoxableGraphView, DynamicGraph, InheritViewOps, IntoDynHop, IntoDynamic,
    MaterializedGraph,
};
pub use layer::*;
pub(crate) use node::BaseNodeViewOps;
pub use node::NodeViewOps;
pub use node_property_filter::NodePropertyFilterOps;
pub use reset_filter::*;
pub use time::*;

pub use raphtory_api::iter::{BoxedIter, BoxedLDIter, BoxedLIter, IntoDynBoxed};
