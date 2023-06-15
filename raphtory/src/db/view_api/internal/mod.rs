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
pub trait BoxableGraphView:
    CoreGraphOps + GraphOps + TimeSemantics + Send + Sync + 'static
{
}

impl<G: CoreGraphOps + GraphOps + TimeSemantics + Send + Sync + 'static + ?Sized> BoxableGraphView
    for G
{
}

#[cfg(test)]
mod test {
    use crate::db::graph::Graph;
    use crate::db::view_api::internal::BoxableGraphView;
    use crate::db::view_api::*;
    use itertools::Itertools;
    use std::sync::Arc;

    #[test]
    fn test_boxing() {
        // this tests that a boxed graph actually compiles
        let g = Graph::new(1);
        g.add_vertex(0, 1, &vec![]).unwrap();
        let boxed: Arc<dyn BoxableGraphView> = Arc::new(g);
        assert_eq!(boxed.vertices().id().collect_vec(), vec![1])
    }
}
