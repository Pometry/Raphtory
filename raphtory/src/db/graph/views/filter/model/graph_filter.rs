use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, GraphView},
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter,
                latest_filter::Latest,
                layered_filter::Layered,
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                CombinedFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter, FilterTree,
                InternalViewWrapOps, TryAsCompositeFilter, Wrap,
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
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = F;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NodeExistsOp<F>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        Ok(filtered)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Ok(NodeExistsOp::new(filtered))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl TryAsCompositeFilter for GraphFilter {
    fn try_as_filter_tree(&self) -> Result<FilterTree, GraphError> {
        // The bare graph anchor restricts nothing — an empty view chain.
        Ok(FilterTree::View(Vec::new()))
    }

    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
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
