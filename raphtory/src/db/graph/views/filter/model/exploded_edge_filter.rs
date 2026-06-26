use crate::{
    db::{
        api::{state::ops::NotANodeFilter, view::internal::GraphView},
        graph::views::filter::{
            exploded_edge_node_filtered_graph::ExplodedEdgeNodeFilteredGraph,
            model::{
                edge_filter::Endpoint,
                node_filter::{builders::InternalNodeFilterBuilder, NodeFilter},
                windowed_filter::Windowed,
                EntityMarker, InternalViewWrapOps, Wrap,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
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

impl<T: InternalNodeFilterBuilder> InternalNodeFilterBuilder for ExplodedEdgeEndpointWrapper<T> {
    type FilterType = T::FilterType;

    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: CreateFilter + Clone + 'static> CreateFilter for ExplodedEdgeEndpointWrapper<T> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = ExplodedEdgeNodeFilteredGraph<G, T::NodeFilter<'graph, G>>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>
        = NotANodeFilter
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>
    where
        T: 'graph,
    {
        let filter = self.inner.create_node_filter(graph.clone())?;
        Ok(ExplodedEdgeNodeFilteredGraph::new(
            graph,
            self.endpoint,
            filter,
        ))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}
