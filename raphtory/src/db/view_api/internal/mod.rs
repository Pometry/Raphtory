mod core_deletion_ops;
mod core_ops;
mod exploded_edge_ops;
mod graph_ops;
mod graph_properties_ops;
mod graph_window_ops;
mod inherit;
mod into_dynamic;
pub(crate) mod time_semantics;
mod wrapped_graph;
mod materialize;

pub use core_deletion_ops::*;
pub use core_ops::*;
pub use exploded_edge_ops::ExplodedEdgeOps;
pub use graph_ops::*;
pub use graph_properties_ops::GraphPropertiesOps;
pub use graph_window_ops::GraphWindowOps;
pub use inherit::Inheritable;
pub use into_dynamic::IntoDynamic;
use std::sync::Arc;
pub use time_semantics::*;

/// Marker trait to indicate that an object is a valid graph view
pub trait BoxableGraphView:
    CoreGraphOps + GraphOps + TimeSemantics + Send + Sync + 'static
{
}

impl<G: CoreGraphOps + GraphOps + TimeSemantics + Send + Sync + 'static + ?Sized> BoxableGraphView
    for G
{
}

pub trait InheritViewOps: Inheritable {}

impl<G: InheritViewOps> InheritCoreDeletionOps for G {}
impl<G: InheritViewOps> InheritGraphOps for G {}
impl<G: InheritViewOps + CoreGraphOps + GraphOps> InheritTimeSemantics for G {}
impl<G: InheritViewOps> InheritCoreOps for G {}

pub type DynamicGraph = Arc<dyn BoxableGraphView>;

#[cfg(test)]
mod test {
    use crate::db::graph::Graph;
    use crate::db::mutation_api::AdditionOps;
    use crate::db::view_api::internal::BoxableGraphView;
    use crate::db::view_api::*;
    use itertools::Itertools;
    use std::sync::Arc;

    #[test]
    fn test_boxing() {
        // this tests that a boxed graph actually compiles
        let g = Graph::new(1);
        g.add_vertex(0, 1, []).unwrap();
        let boxed: Arc<dyn BoxableGraphView> = Arc::new(g);
        assert_eq!(boxed.vertices().id().collect_vec(), vec![1])
    }
}
