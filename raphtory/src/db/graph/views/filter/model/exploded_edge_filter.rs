use crate::{
    db::{
        api::{state::ops::NotANodeFilter, view::internal::GraphView},
        graph::views::filter::{
            exploded_edge_node_filtered_graph::ExplodedEdgeNodeFilteredGraph,
            model::{
                edge_filter::Endpoint,
                is_active_edge_filter::IsActiveEdge,
                is_deleted_filter::IsDeletedEdge,
                is_self_loop_filter::IsSelfLoopEdge,
                is_valid_filter::IsValidEdge,
                latest_filter::Latest,
                layered_filter::Layered,
                node_filter::{CompositeNodeFilter, NodeFilter},
                property_filter::PropertyFilter,
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                CombinedFilter, EdgeViewFilterOps, EntityMarker, InternalViewWrapOps, Wrap,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::storage::timeindex::EventTime;
use std::{fmt, fmt::Display};

#[derive(Clone, Debug, Copy, Default, PartialEq, Eq)]
pub struct ExplodedEdgeFilter;

impl From<ExplodedEdgeFilter> for EntityMarker {
    fn from(_value: ExplodedEdgeFilter) -> Self {
        EntityMarker::ExplodedEdge
    }
}

impl ExplodedEdgeFilter {
    #[inline]
    pub fn src() -> ExplodedEdgeEndpointWrapper<NodeFilter> {
        ExplodedEdgeEndpointWrapper::new(NodeFilter, Endpoint::Src)
    }

    #[inline]
    pub fn dst() -> ExplodedEdgeEndpointWrapper<NodeFilter> {
        ExplodedEdgeEndpointWrapper::new(NodeFilter, Endpoint::Dst)
    }
}

impl Wrap for ExplodedEdgeFilter {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalViewWrapOps for ExplodedEdgeFilter {
    type Window = Windowed<ExplodedEdgeFilter>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

impl EdgeViewFilterOps for ExplodedEdgeFilter {
    type Output<T: CombinedFilter> = T;

    fn is_active(&self) -> Self::Output<IsActiveEdge> {
        IsActiveEdge
    }

    fn is_valid(&self) -> Self::Output<IsValidEdge> {
        IsValidEdge
    }

    fn is_deleted(&self) -> Self::Output<IsDeletedEdge> {
        IsDeletedEdge
    }

    fn is_self_loop(&self) -> Self::Output<IsSelfLoopEdge> {
        IsSelfLoopEdge
    }
}

#[derive(Debug, Clone)]
pub struct ExplodedEdgeEndpointWrapper<T> {
    pub(crate) inner: T,
    endpoint: Endpoint,
}

impl<T: Display> Display for ExplodedEdgeEndpointWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> ExplodedEdgeEndpointWrapper<T> {
    #[inline]
    pub fn new(inner: T, endpoint: Endpoint) -> Self {
        Self { inner, endpoint }
    }

    #[inline]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> ExplodedEdgeEndpointWrapper<U> {
        ExplodedEdgeEndpointWrapper {
            inner: f(self.inner),
            endpoint: self.endpoint,
        }
    }
}

impl<M> Wrap for ExplodedEdgeEndpointWrapper<M> {
    type Wrapped<T> = ExplodedEdgeEndpointWrapper<T>;

    fn wrap<T>(&self, inner: T) -> Self::Wrapped<T> {
        ExplodedEdgeEndpointWrapper {
            inner,
            endpoint: self.endpoint,
        }
    }
}

impl<T: CreateFilter + Clone + 'static> CreateFilter for ExplodedEdgeEndpointWrapper<T> {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = ExplodedEdgeNodeFilteredGraph<G, T::NodeFilter<'graph, G, F>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G, F>
        = NotANodeFilter
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;
    type FilteredGraph<'graph, G>
        = T::FilteredGraph<'graph, G>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError>
    where
        T: 'graph,
    {
        let filter = self.inner.create_node_filter(graph.clone(), filtered)?;
        Ok(ExplodedEdgeNodeFilteredGraph::new(
            graph,
            self.endpoint,
            filter,
        ))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        self.inner.filter_graph_view(graph)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeExplodedEdgeFilter {
    Src(CompositeNodeFilter),
    Dst(CompositeNodeFilter),
    Property(PropertyFilter<ExplodedEdgeFilter>),
    Windowed(Box<Windowed<CompositeExplodedEdgeFilter>>),
    Latest(Box<Latest<CompositeExplodedEdgeFilter>>),
    SnapshotAt(Box<SnapshotAt<CompositeExplodedEdgeFilter>>),
    SnapshotLatest(Box<SnapshotLatest<CompositeExplodedEdgeFilter>>),
    Layered(Box<Layered<CompositeExplodedEdgeFilter>>),
    IsActiveEdge(IsActiveEdge),
    IsValidEdge(IsValidEdge),
    IsDeletedEdge(IsDeletedEdge),
    IsSelfLoopEdge(IsSelfLoopEdge),
    And(
        Box<CompositeExplodedEdgeFilter>,
        Box<CompositeExplodedEdgeFilter>,
    ),
    Or(
        Box<CompositeExplodedEdgeFilter>,
        Box<CompositeExplodedEdgeFilter>,
    ),
    Not(Box<CompositeExplodedEdgeFilter>),
}

impl Display for CompositeExplodedEdgeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeExplodedEdgeFilter::Src(filter) => write!(f, "SRC({})", filter),
            CompositeExplodedEdgeFilter::Dst(filter) => write!(f, "DST({})", filter),
            CompositeExplodedEdgeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::Windowed(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::Latest(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::SnapshotAt(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::SnapshotLatest(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::IsActiveEdge(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::IsValidEdge(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::IsDeletedEdge(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::IsSelfLoopEdge(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::Layered(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeExplodedEdgeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeExplodedEdgeFilter::Not(filter) => write!(f, "(NOT {})", filter),
        }
    }
}
