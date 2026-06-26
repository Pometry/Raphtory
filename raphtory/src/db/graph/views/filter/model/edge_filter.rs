use crate::{
    db::{
        api::{
            state::ops::{Id, Name, NotANodeFilter, Type},
            view::internal::GraphView,
        },
        graph::views::filter::{
            edge_node_filtered_graph::EdgeNodeFilteredGraph,
            model::{
                edge_expr::{ops::EdgeEndpointNodeOp, EdgeOp},
                exploded_edge_filter::ExplodedEdgeFilter,
                latest_filter::Latest,
                layered_filter::Layered,
                node_expr::{CreateOp, EntityExpr},
                node_filter::{builders::InternalNodeFilterBuilder, NodeFilter},
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                AllExpr, AnyExpr, AvgExpr, ComposableFilter, CreateView, EdgeFilterFactory,
                EntityAggOps, EntityMarker, FirstExpr, InternalViewWrapOps, LastExpr, LenExpr,
                MaxExpr, MetadataExpr, MinExpr, PropertyExpr, PropertyFilterFactory, SumExpr,
                TemporalPropExpr, Wrap,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
use std::{fmt, fmt::Display, sync::Arc};

// User facing entry for building edge filters.
#[derive(Clone, Debug, Copy, Default, PartialEq, Eq)]
pub struct EdgeFilter;

impl From<EdgeFilter> for EntityMarker {
    fn from(_value: EdgeFilter) -> Self {
        EntityMarker::Edge
    }
}

impl EdgeFilter {
    #[inline]
    pub fn src() -> EdgeEndpointWrapper<NodeFilter> {
        EdgeEndpointWrapper::new(NodeFilter, Endpoint::Src)
    }

    #[inline]
    pub fn dst() -> EdgeEndpointWrapper<NodeFilter> {
        EdgeEndpointWrapper::new(NodeFilter, Endpoint::Dst)
    }
}

impl Wrap for EdgeFilter {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalViewWrapOps for EdgeFilter {
    type Window = Windowed<EdgeFilter>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum Endpoint {
    Src,
    Dst,
}

// Generic wrapper that pairs node-side builders with a concrete endpoint.
// The objective is to carry the endpoint through builder chain without having to change node builders
// and at the end convert into a composite node filter via TryAsCompositeFilter
#[derive(Debug, Clone)]
pub struct EdgeEndpointWrapper<T> {
    pub(crate) inner: T,
    endpoint: Endpoint,
}

impl<T: Display> Display for EdgeEndpointWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> EdgeEndpointWrapper<T> {
    #[inline]
    pub fn new(inner: T, endpoint: Endpoint) -> Self {
        Self { inner, endpoint }
    }

    #[inline]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> EdgeEndpointWrapper<U> {
        EdgeEndpointWrapper {
            inner: f(self.inner),
            endpoint: self.endpoint,
        }
    }
}

impl EdgeEndpointWrapper<NodeFilter> {
    #[inline]
    pub fn id(&self) -> EdgeEndpointWrapper<Id> {
        EdgeEndpointWrapper::new(Id, self.endpoint)
    }

    #[inline]
    pub fn name(&self) -> EdgeEndpointWrapper<Name> {
        EdgeEndpointWrapper::new(Name, self.endpoint)
    }

    #[inline]
    pub fn node_type(&self) -> EdgeEndpointWrapper<Type> {
        EdgeEndpointWrapper::new(Type, self.endpoint)
    }

    #[inline]
    pub fn property(
        &self,
        name: impl Into<String>,
    ) -> EdgeEndpointWrapper<PropertyExpr<NodeFilter>> {
        EdgeEndpointWrapper::new(NodeFilter.property(name), self.endpoint)
    }

    #[inline]
    pub fn metadata(
        &self,
        name: impl Into<String>,
    ) -> EdgeEndpointWrapper<MetadataExpr<NodeFilter>> {
        EdgeEndpointWrapper::new(NodeFilter.metadata(name), self.endpoint)
    }
}

impl<E: CreateView + Clone + Send + Sync + 'static> EdgeEndpointWrapper<PropertyExpr<E>> {
    #[inline]
    pub fn temporal(&self) -> EdgeEndpointWrapper<TemporalPropExpr<E>> {
        EdgeEndpointWrapper::new(self.inner.temporal(), self.endpoint)
    }
}

impl<E: CreateView + EntityExpr + Clone + Send + Sync + 'static>
    EdgeEndpointWrapper<TemporalPropExpr<E>>
{
    #[inline]
    pub fn sum(self) -> EdgeEndpointWrapper<SumExpr<TemporalPropExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.sum(), endpoint)
    }
    #[inline]
    pub fn avg(self) -> EdgeEndpointWrapper<AvgExpr<TemporalPropExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.avg(), endpoint)
    }
    #[inline]
    pub fn min(self) -> EdgeEndpointWrapper<MinExpr<TemporalPropExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.min(), endpoint)
    }
    #[inline]
    pub fn max(self) -> EdgeEndpointWrapper<MaxExpr<TemporalPropExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.max(), endpoint)
    }
    #[inline]
    pub fn first(self) -> EdgeEndpointWrapper<FirstExpr<TemporalPropExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.first(), endpoint)
    }
    #[inline]
    pub fn last(self) -> EdgeEndpointWrapper<LastExpr<TemporalPropExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.last(), endpoint)
    }
    #[inline]
    pub fn len(self) -> EdgeEndpointWrapper<LenExpr<TemporalPropExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.len(), endpoint)
    }
    #[inline]
    pub fn any(self) -> EdgeEndpointWrapper<AnyExpr<TemporalPropExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(AnyExpr(self.inner), endpoint)
    }
    #[inline]
    pub fn all(self) -> EdgeEndpointWrapper<AllExpr<TemporalPropExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(AllExpr(self.inner), endpoint)
    }
}

impl<T: EntityExpr> EntityExpr for EdgeEndpointWrapper<T> {
    type Marker = EdgeFilter;
    fn entity(&self) -> Self::Marker {
        EdgeFilter
    }
}

impl<T: CreateOp> CreateOp for EdgeEndpointWrapper<T> {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let node_op = self.inner.create_node_op(graph)?;
        Ok(Arc::new(EdgeEndpointNodeOp {
            node_op,
            endpoint: self.endpoint,
        }))
    }
}

impl<M> Wrap for EdgeEndpointWrapper<M> {
    type Wrapped<T> = EdgeEndpointWrapper<T>;

    fn wrap<T>(&self, inner: T) -> Self::Wrapped<T> {
        EdgeEndpointWrapper {
            inner,
            endpoint: self.endpoint,
        }
    }
}

impl<T> ComposableFilter for EdgeEndpointWrapper<T> where T: Clone {}

impl<T: InternalNodeFilterBuilder> InternalNodeFilterBuilder for EdgeEndpointWrapper<T> {
    type FilterType = T::FilterType;
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: CreateFilter + Clone + 'static> CreateFilter for EdgeEndpointWrapper<T> {
    type EntityFiltered<'graph, G>
        = EdgeNodeFilteredGraph<G, T::NodeFilter<'graph, G>>
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
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let filter = self.inner.create_node_filter(graph.clone())?;
        Ok(EdgeNodeFilteredGraph::new(graph, self.endpoint, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeFilterFactory impls
// ─────────────────────────────────────────────────────────────────────────────

impl EdgeFilterFactory for EdgeFilter {}
impl EdgeFilterFactory for ExplodedEdgeFilter {}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for Windowed<T> {}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for Latest<T> {}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for Layered<T> {}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for SnapshotAt<T> {}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for SnapshotLatest<T> {}
