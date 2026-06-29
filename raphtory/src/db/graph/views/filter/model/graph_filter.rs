use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, GraphView},
        graph::views::filter::{
            model::{
                latest_filter::Latest,
                layered_filter::Layered,
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                CreateView, InternalViewWrapOps, Wrap,
            },
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

pub trait GraphFilterOps: InternalViewWrapOps<Window = Self::GraphWindow> + CreateFilter {
    type GraphWindow: GraphFilterOps;
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

impl GraphFilterOps for GraphFilter {
    type GraphWindow = Self::Window;
}

impl<T: GraphFilterOps> GraphFilterOps for Windowed<T> {
    type GraphWindow = Self::Window;
}

impl<T: GraphFilterOps> GraphFilterOps for Layered<T> {
    type GraphWindow = Self::Window;
}

impl<T: GraphFilterOps> GraphFilterOps for Latest<T> {
    type GraphWindow = Self::Window;
}

impl<T: GraphFilterOps> GraphFilterOps for SnapshotAt<T> {
    type GraphWindow = Self::Window;
}

impl<T: GraphFilterOps> GraphFilterOps for SnapshotLatest<T> {
    type GraphWindow = Self::Window;
}
