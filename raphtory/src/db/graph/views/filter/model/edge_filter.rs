use crate::{
    db::{
        api::{
            state::ops::{Id, Name, NotANodeFilter, Type},
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            edge_node_filtered_graph::EdgeNodeFilteredGraph,
            model::{
                edge_expr::{ops::EdgeEndpointNodeOp, EdgeExpr, EdgeOp},
                exploded_edge_filter::{CompositeExplodedEdgeFilter, ExplodedEdgeFilter},
                is_active_edge_filter::IsActiveEdge,
                is_deleted_filter::IsDeletedEdge,
                is_self_loop_filter::IsSelfLoopEdge,
                is_valid_filter::IsValidEdge,
                latest_filter::Latest,
                layered_filter::Layered,
                node_expr::{EntityExpr, NodeExpr},
                node_filter::{
                    builders::InternalNodeFilterBuilder, CompositeNodeFilter, NodeFilter,
                },
                property_filter::PropertyFilter,
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                AllExpr, AndFilter, AnyExpr, AvgExpr, CombinedFilter, ComposableFilter,
                CreateView, EdgeFilterFactory, EdgeViewFilterOps, EntityAggOps,
                EntityExprFilterOps, EntityMarker, FirstExpr, InternalViewWrapOps,
                LastExpr, LenExpr, MaxExpr, MetadataExpr, MinExpr, NotFilter, OrFilter,
                PropertyExpr, PropertyFilterFactory, SumExpr, TemporalExpr,
                TryAsCompositeFilter, Wrap,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::EventTime,
};
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

impl EdgeViewFilterOps for EdgeFilter {
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
    pub fn property(&self, name: impl Into<String>) -> EdgeEndpointWrapper<PropertyExpr<NodeFilter>> {
        EdgeEndpointWrapper::new(NodeFilter.property(name), self.endpoint)
    }

    #[inline]
    pub fn metadata(&self, name: impl Into<String>) -> EdgeEndpointWrapper<MetadataExpr<NodeFilter>> {
        EdgeEndpointWrapper::new(NodeFilter.metadata(name), self.endpoint)
    }
}

impl<E: CreateView + Clone + Send + Sync + 'static> EdgeEndpointWrapper<PropertyExpr<E>> {
    #[inline]
    pub fn temporal(&self) -> EdgeEndpointWrapper<TemporalExpr<E>> {
        EdgeEndpointWrapper::new(self.inner.temporal(), self.endpoint)
    }
}

impl<E: CreateView + EntityExpr + Clone + Send + Sync + 'static> EdgeEndpointWrapper<TemporalExpr<E>> {
    #[inline]
    pub fn sum(self) -> EdgeEndpointWrapper<SumExpr<TemporalExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.sum(), endpoint)
    }
    #[inline]
    pub fn avg(self) -> EdgeEndpointWrapper<AvgExpr<TemporalExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.avg(), endpoint)
    }
    #[inline]
    pub fn min(self) -> EdgeEndpointWrapper<MinExpr<TemporalExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.min(), endpoint)
    }
    #[inline]
    pub fn max(self) -> EdgeEndpointWrapper<MaxExpr<TemporalExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.max(), endpoint)
    }
    #[inline]
    pub fn first(self) -> EdgeEndpointWrapper<FirstExpr<TemporalExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.first(), endpoint)
    }
    #[inline]
    pub fn last(self) -> EdgeEndpointWrapper<LastExpr<TemporalExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.last(), endpoint)
    }
    #[inline]
    pub fn len(self) -> EdgeEndpointWrapper<LenExpr<TemporalExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(self.inner.len(), endpoint)
    }
    #[inline]
    pub fn any(self) -> EdgeEndpointWrapper<AnyExpr<TemporalExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(AnyExpr(self.inner), endpoint)
    }
    #[inline]
    pub fn all(self) -> EdgeEndpointWrapper<AllExpr<TemporalExpr<E>>> {
        let endpoint = self.endpoint;
        EdgeEndpointWrapper::new(AllExpr(self.inner), endpoint)
    }
}

impl<T: EntityExpr> EntityExpr for EdgeEndpointWrapper<T> {
    type Marker = EdgeFilter;
}

impl<T: NodeExpr> EdgeExpr for EdgeEndpointWrapper<T> {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let node_op = self.inner.create_node_op(graph)?;
        Ok(Arc::new(EdgeEndpointNodeOp { node_op, endpoint: self.endpoint }))
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

impl<T> ComposableFilter for EdgeEndpointWrapper<T> where T: TryAsCompositeFilter + Clone {}

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

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

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

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for EdgeEndpointWrapper<T> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        let filter = self.inner.try_as_composite_node_filter()?;
        let filter = match self.endpoint {
            Endpoint::Src => CompositeEdgeFilter::Src(filter),
            Endpoint::Dst => CompositeEdgeFilter::Dst(filter),
        };
        Ok(filter)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        let filter = self.inner.try_as_composite_node_filter()?;
        let filter = match self.endpoint {
            Endpoint::Src => CompositeExplodedEdgeFilter::Src(filter),
            Endpoint::Dst => CompositeExplodedEdgeFilter::Dst(filter),
        };
        Ok(filter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeEdgeFilter {
    Src(CompositeNodeFilter),
    Dst(CompositeNodeFilter),
    Property(PropertyFilter<EdgeFilter>),
    Windowed(Box<Windowed<CompositeEdgeFilter>>),
    Latest(Box<Latest<CompositeEdgeFilter>>),
    SnapshotAt(Box<SnapshotAt<CompositeEdgeFilter>>),
    SnapshotLatest(Box<SnapshotLatest<CompositeEdgeFilter>>),
    IsActiveEdge(IsActiveEdge),
    IsValidEdge(IsValidEdge),
    IsDeletedEdge(IsDeletedEdge),
    IsSelfLoopEdge(IsSelfLoopEdge),
    Layered(Box<Layered<CompositeEdgeFilter>>),
    And(Box<CompositeEdgeFilter>, Box<CompositeEdgeFilter>),
    Or(Box<CompositeEdgeFilter>, Box<CompositeEdgeFilter>),
    Not(Box<CompositeEdgeFilter>),
}

impl Display for CompositeEdgeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeEdgeFilter::Src(filter) => write!(f, "SRC({})", filter),
            CompositeEdgeFilter::Dst(filter) => write!(f, "DST({})", filter),
            CompositeEdgeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::Windowed(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::Latest(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::SnapshotAt(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::SnapshotLatest(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::IsActiveEdge(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::IsValidEdge(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::IsDeletedEdge(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::IsSelfLoopEdge(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::Layered(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeEdgeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeEdgeFilter::Not(filter) => write!(f, "(NOT {})", filter),
        }
    }
}

impl CreateFilter for CompositeEdgeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    type NodeFilter<'graph, G>
        = NotANodeFilter
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type FilteredGraph<'graph, G>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        match self {
            CompositeEdgeFilter::Src(filter) => {
                let wrapped = EdgeEndpointWrapper::new(filter, Endpoint::Src);
                let filtered_graph = wrapped.create_filter(graph)?;
                Ok(Arc::new(filtered_graph))
            }
            CompositeEdgeFilter::Dst(filter) => {
                let wrapped = EdgeEndpointWrapper::new(filter, Endpoint::Dst);
                let filtered_graph = wrapped.create_filter(graph)?;
                Ok(Arc::new(filtered_graph))
            }
            CompositeEdgeFilter::Property(i) => Ok(Arc::new(i.create_filter(graph)?)),
            CompositeEdgeFilter::Windowed(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_filter(dyn_graph)
            }
            CompositeEdgeFilter::Latest(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_filter(dyn_graph)
            }
            CompositeEdgeFilter::SnapshotAt(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_filter(dyn_graph)
            }
            CompositeEdgeFilter::SnapshotLatest(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_filter(dyn_graph)
            }
            CompositeEdgeFilter::IsActiveEdge(i) => Ok(Arc::new(i.create_filter(graph)?)),
            CompositeEdgeFilter::IsValidEdge(i) => Ok(Arc::new(i.create_filter(graph)?)),
            CompositeEdgeFilter::IsDeletedEdge(i) => Ok(Arc::new(i.create_filter(graph)?)),
            CompositeEdgeFilter::IsSelfLoopEdge(i) => Ok(Arc::new(i.create_filter(graph)?)),
            CompositeEdgeFilter::Layered(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_filter(dyn_graph)
            }
            CompositeEdgeFilter::And(l, r) => {
                let (l, r) = (*l, *r);
                Ok(Arc::new(
                    AndFilter { left: l, right: r }.create_filter(graph)?,
                ))
            }
            CompositeEdgeFilter::Or(l, r) => {
                let (l, r) = (*l, *r);
                Ok(Arc::new(
                    OrFilter { left: l, right: r }.create_filter(graph)?,
                ))
            }
            CompositeEdgeFilter::Not(f) => {
                let base = *f;
                Ok(Arc::new(NotFilter(base).create_filter(graph)?))
            }
        }
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

impl TryAsCompositeFilter for CompositeEdgeFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(self.clone())
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
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
