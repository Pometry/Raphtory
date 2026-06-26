use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, GraphView},
        graph::views::filter::{
            model::{windowed_filter::Windowed, InternalViewWrapOps, Wrap},
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::storage::timeindex::EventTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GraphFilter;

impl std::fmt::Display for GraphFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GRAPH")
    }
}

impl Wrap for GraphFilter {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalViewWrapOps for GraphFilter {
    type Window = Windowed<GraphFilter>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

impl CreateFilter for GraphFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = G;

    type NodeFilter<'graph, G: GraphView + 'graph> = NodeExistsOp<G>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeExistsOp::new(graph))
    }
}
