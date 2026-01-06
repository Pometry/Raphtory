use crate::{
    db::{
        api::{
            state::ops::NotANodeFilter,
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            edge_node_filtered_graph::EdgeNodeFilteredGraph,
            model::{
                exploded_edge_filter::CompositeExplodedEdgeFilter,
                latest_filter::Latest,
                layered_filter::Layered,
                node_filter::{
                    builders::{
                        InternalNodeFilterBuilder, InternalNodeIdFilterBuilder,
                        NodeIdFilterBuilder, NodeNameFilterBuilder, NodeTypeFilterBuilder,
                    },
                    CompositeNodeFilter, NodeFilter,
                },
                property_filter::{
                    builders::{MetadataFilterBuilder, PropertyExprBuilder, PropertyFilterBuilder},
                    Op, PropertyFilter, PropertyRef,
                },
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                AndFilter, ComposableFilter, InternalPropertyFilterBuilder,
                InternalPropertyFilterFactory, NotFilter, OrFilter, TemporalPropertyFilterFactory,
                TryAsCompositeFilter, Wrap,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::{entities::Layer, storage::timeindex::TimeIndexEntry};
use raphtory_core::utils::time::IntoTime;
use std::{fmt, fmt::Display, sync::Arc};

// User facing entry for building edge filters.
#[derive(Clone, Debug, Copy, Default, PartialEq, Eq)]
pub struct EdgeFilter;

impl EdgeFilter {
    #[inline]
    pub fn src() -> EdgeEndpointWrapper<NodeFilter> {
        EdgeEndpointWrapper::new(NodeFilter, Endpoint::Src)
    }

    #[inline]
    pub fn dst() -> EdgeEndpointWrapper<NodeFilter> {
        EdgeEndpointWrapper::new(NodeFilter, Endpoint::Dst)
    }

    #[inline]
    pub fn window<S: IntoTime, E: IntoTime>(start: S, end: E) -> Windowed<EdgeFilter> {
        Windowed::from_times(start, end, EdgeFilter)
    }

    #[inline]
    pub fn at<T: IntoTime>(time: T) -> Windowed<EdgeFilter> {
        let t = time.into_time();
        Windowed::from_times(t, t.saturating_add(1), EdgeFilter)
    }

    #[inline]
    pub fn after<T: IntoTime>(time: T) -> Windowed<EdgeFilter> {
        let start = time.into_time().saturating_add(1);
        Windowed::new(
            TimeIndexEntry::start(start),
            TimeIndexEntry::end(i64::MAX),
            EdgeFilter,
        )
    }

    #[inline]
    pub fn before<T: IntoTime>(time: T) -> Windowed<EdgeFilter> {
        Windowed::new(
            TimeIndexEntry::start(i64::MIN),
            TimeIndexEntry::end(time.into_time()),
            EdgeFilter,
        )
    }

    #[inline]
    pub fn latest() -> Latest<EdgeFilter> {
        Latest::new(EdgeFilter)
    }

    #[inline]
    pub fn snapshot_at<T: IntoTime>(time: T) -> SnapshotAt<EdgeFilter> {
        SnapshotAt::new(time, EdgeFilter)
    }

    #[inline]
    pub fn snapshot_latest() -> SnapshotLatest<EdgeFilter> {
        SnapshotLatest::new(EdgeFilter)
    }

    #[inline]
    pub fn layer<L: Into<Layer>>(layer: L) -> Layered<EdgeFilter> {
        Layered::from_layers(layer, EdgeFilter)
    }
}

impl Wrap for EdgeFilter {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalPropertyFilterFactory for EdgeFilter {
    type Entity = EdgeFilter;
    type PropertyBuilder = PropertyFilterBuilder<Self::Entity>;
    type MetadataBuilder = MetadataFilterBuilder<Self::Entity>;

    fn entity(&self) -> Self::Entity {
        EdgeFilter
    }

    fn property_builder(
        &self,
        builder: PropertyFilterBuilder<Self::Entity>,
    ) -> Self::PropertyBuilder {
        builder
    }

    fn metadata_builder(
        &self,
        builder: MetadataFilterBuilder<Self::Entity>,
    ) -> Self::MetadataBuilder {
        builder
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
    pub fn id(&self) -> EdgeEndpointWrapper<NodeIdFilterBuilder> {
        EdgeEndpointWrapper::new(NodeFilter::id(), self.endpoint)
    }

    #[inline]
    pub fn name(&self) -> EdgeEndpointWrapper<NodeNameFilterBuilder> {
        EdgeEndpointWrapper::new(NodeFilter::name(), self.endpoint)
    }

    #[inline]
    pub fn node_type(&self) -> EdgeEndpointWrapper<NodeTypeFilterBuilder> {
        EdgeEndpointWrapper::new(NodeFilter::node_type(), self.endpoint)
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

impl<T: InternalNodeIdFilterBuilder> InternalNodeIdFilterBuilder for EdgeEndpointWrapper<T> {
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalNodeFilterBuilder> InternalNodeFilterBuilder for EdgeEndpointWrapper<T> {
    type FilterType = T::FilterType;
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalPropertyFilterBuilder> InternalPropertyFilterBuilder for EdgeEndpointWrapper<T> {
    type Filter = EdgeEndpointWrapper<T::Filter>;
    type ExprBuilder = EdgeEndpointWrapper<T::ExprBuilder>;
    type Marker = T::Marker;

    #[inline]
    fn property_ref(&self) -> PropertyRef {
        self.inner.property_ref()
    }

    #[inline]
    fn ops(&self) -> &[Op] {
        self.inner.ops()
    }

    #[inline]
    fn entity(&self) -> Self::Marker {
        self.inner.entity()
    }

    fn filter(&self, filter: PropertyFilter<Self::Marker>) -> Self::Filter {
        self.wrap(self.inner.filter(filter))
    }

    fn into_expr_builder(&self, builder: PropertyExprBuilder<Self::Marker>) -> Self::ExprBuilder {
        self.wrap(self.inner.into_expr_builder(builder))
    }
}

impl<T: InternalPropertyFilterFactory> InternalPropertyFilterFactory for EdgeEndpointWrapper<T> {
    type Entity = T::Entity;
    type PropertyBuilder = EdgeEndpointWrapper<T::PropertyBuilder>;
    type MetadataBuilder = EdgeEndpointWrapper<T::MetadataBuilder>;

    fn entity(&self) -> Self::Entity {
        self.inner.entity()
    }

    fn property_builder(
        &self,
        builder: PropertyFilterBuilder<Self::Entity>,
    ) -> Self::PropertyBuilder {
        self.wrap(self.inner.property_builder(builder))
    }

    fn metadata_builder(
        &self,
        builder: MetadataFilterBuilder<Self::Entity>,
    ) -> Self::MetadataBuilder {
        self.wrap(self.inner.metadata_builder(builder))
    }
}

impl<T: TemporalPropertyFilterFactory> TemporalPropertyFilterFactory for EdgeEndpointWrapper<T> {}

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
