mod core_deletion_ops;
mod core_ops;
mod exploded_edge_ops;
mod graph_ops;
mod graph_properties_ops;
mod graph_window_ops;
mod inherit;
mod into_dynamic;
mod materialize;
pub(crate) mod time_semantics;
mod wrapped_graph;

use crate::db::api::properties::internal::{
    PropertiesOps, InheritPropertiesOps, StaticPropertiesOps,
};
use crate::prelude::GraphViewOps;
pub use core_deletion_ops::*;
pub use core_ops::*;
pub use exploded_edge_ops::ExplodedEdgeOps;
pub use graph_ops::*;
pub use graph_properties_ops::GraphPropertiesOps;
pub use graph_window_ops::GraphWindowOps;
pub use inherit::Base;
pub use into_dynamic::IntoDynamic;
pub use materialize::*;
use std::sync::Arc;
pub use time_semantics::*;

/// Marker trait to indicate that an object is a valid graph view
pub trait BoxableGraphView:
    CoreGraphOps
    + GraphOps
    + TimeSemantics
    + InternalMaterialize
    + PropertiesOps
    + StaticPropertiesOps
    + Send
    + Sync
    + 'static
{
}

impl<
        G: CoreGraphOps
            + GraphOps
            + TimeSemantics
            + InternalMaterialize
            + PropertiesOps
            + StaticPropertiesOps
            + Send
            + Sync
            + 'static
            + ?Sized,
    > BoxableGraphView for G
{
}

pub trait InheritViewOps: Base {}

impl<G: InheritViewOps> InheritCoreDeletionOps for G {}
impl<G: InheritViewOps> InheritGraphOps for G {}
impl<G: InheritViewOps + CoreGraphOps + GraphOps> InheritTimeSemantics for G {}
impl<G: InheritViewOps> InheritCoreOps for G {}
impl<G: InheritViewOps> InheritMaterialize for G {}
impl<G: InheritViewOps> InheritPropertiesOps for G {}

/// Trait for marking a struct as not dynamically dispatched.
/// Used to avoid conflicts when implementing `From` for dynamic wrappers.
pub trait Static {}

impl<G: BoxableGraphView + Static> From<G> for DynamicGraph {
    fn from(value: G) -> Self {
        DynamicGraph(Arc::new(value))
    }
}

impl From<Arc<dyn BoxableGraphView>> for DynamicGraph {
    fn from(value: Arc<dyn BoxableGraphView>) -> Self {
        DynamicGraph(value)
    }
}

#[derive(Clone)]
pub struct DynamicGraph(pub(crate) Arc<dyn BoxableGraphView>);

impl DynamicGraph {
    pub fn new<G: GraphViewOps>(graph: G) -> Self {
        Self(Arc::new(graph))
    }

    pub fn new_from_arc<G: GraphViewOps>(graph_arc: Arc<G>) -> Self {
        Self(graph_arc)
    }
}

impl Base for DynamicGraph {
    type Base = dyn BoxableGraphView;

    fn base(&self) -> &Self::Base {
        &self.0
    }
}

impl InheritViewOps for DynamicGraph {}

#[cfg(test)]
mod test {
    use crate::{
        db::{
            api::{
                mutation::AdditionOps,
                view::{internal::BoxableGraphView, *},
            },
            graph::graph::Graph,
        },
        prelude::NO_PROPS,
    };
    use itertools::Itertools;
    use std::sync::Arc;

    #[test]
    fn test_boxing() {
        // this tests that a boxed graph actually compiles
        let g = Graph::new();
        g.add_vertex(0, 1, NO_PROPS).unwrap();
        let boxed: Arc<dyn BoxableGraphView> = Arc::new(g);
        assert_eq!(boxed.vertices().id().collect_vec(), vec![1])
    }
}
