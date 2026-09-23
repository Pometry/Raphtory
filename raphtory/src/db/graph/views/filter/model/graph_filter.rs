use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, GraphView},
        graph::views::filter::{
            model::{
                edge_expr::ops::EdgeExistsOp,
                latest_filter::Latest,
                layered_filter::Layered,
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                CombinedFilter, InternalViewWrapOps,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::storage::timeindex::EventTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GraphFilter;

impl std::fmt::Display for GraphFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GRAPH")
    }
}

impl InternalViewWrapOps for GraphFilter {
    type Window = Windowed<GraphFilter>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

impl CreateFilter for GraphFilter {
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = NodeExistsOp<G>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = EdgeExistsOp<G>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeExistsOp::new(graph))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        Ok(EdgeExistsOp::new(graph))
    }
}

// ── expr-layer view ops ──

pub trait GraphFilterOps:
    InternalViewWrapOps<Window = Self::GraphWindow> + CombinedFilter + Send + Sync + 'static
{
    type GraphWindow: GraphFilterOps + CombinedFilter;
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
