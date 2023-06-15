mod core_ops;
mod exploded_edge_ops;
mod graph_ops;
mod graph_properties_ops;
mod graph_window_ops;
pub(crate) mod time_semantics;
mod wrapped_graph;

pub use core_ops::{CoreGraphOps, InheritCoreOps};
pub use exploded_edge_ops::ExplodedEdgeOps;
pub use graph_ops::{GraphOps, InheritGraphOps};
pub use graph_properties_ops::GraphPropertiesOps;
pub use graph_window_ops::GraphWindowOps;
pub use time_semantics::{InheritTimeSemantics, TimeSemantics};
pub use wrapped_graph::WrappedGraph;

/// Marker trait to indicate that an object is a valid graph view
pub trait GraphViewInternalOps:
    CoreGraphOps + GraphOps + TimeSemantics + Send + Sync + Clone + 'static
{
}

impl<G: CoreGraphOps + GraphOps + TimeSemantics + Send + Sync + Clone + 'static + ?Sized>
    GraphViewInternalOps for G
{
}
