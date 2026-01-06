use crate::{
    db::{
        api::{
            state::ops::NotANodeFilter,
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            exploded_edge_node_filtered_graph::ExplodedEdgeNodeFilteredGraph,
            model::{
                edge_filter::{CompositeEdgeFilter, Endpoint},
                latest_filter::Latest,
                layered_filter::Layered,
                node_filter::{
                    builders::{InternalNodeFilterBuilder, InternalNodeIdFilterBuilder},
                    CompositeNodeFilter, NodeFilter,
                },
                property_filter::{
                    builders::{MetadataFilterBuilder, PropertyExprBuilder, PropertyFilterBuilder},
                    Op, PropertyFilter, PropertyRef,
                },
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                AndFilter, InternalPropertyFilterBuilder, InternalPropertyFilterFactory, NotFilter,
                OrFilter, TemporalPropertyFilterFactory, TryAsCompositeFilter, Wrap,
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

#[derive(Clone, Debug, Copy, Default, PartialEq, Eq)]
pub struct ExplodedEdgeFilter;

impl ExplodedEdgeFilter {
    #[inline]
    pub fn src() -> ExplodedEdgeEndpointWrapper<NodeFilter> {
        ExplodedEdgeEndpointWrapper::new(NodeFilter, Endpoint::Src)
    }

    #[inline]
    pub fn dst() -> ExplodedEdgeEndpointWrapper<NodeFilter> {
        ExplodedEdgeEndpointWrapper::new(NodeFilter, Endpoint::Dst)
    }

    #[inline]
    pub fn window<S: IntoTime, E: IntoTime>(start: S, end: E) -> Windowed<ExplodedEdgeFilter> {
        Windowed::from_times(start, end, ExplodedEdgeFilter)
    }

    #[inline]
    pub fn at<T: IntoTime>(time: T) -> Windowed<ExplodedEdgeFilter> {
        let t = time.into_time();
        Windowed::from_times(t, t.saturating_add(1), ExplodedEdgeFilter)
    }

    #[inline]
    pub fn after<T: IntoTime>(time: T) -> Windowed<ExplodedEdgeFilter> {
        let start = time.into_time().saturating_add(1);
        Windowed::new(
            TimeIndexEntry::start(start),
            TimeIndexEntry::end(i64::MAX),
            ExplodedEdgeFilter,
        )
    }

    #[inline]
    pub fn before<T: IntoTime>(time: T) -> Windowed<ExplodedEdgeFilter> {
        Windowed::new(
            TimeIndexEntry::start(i64::MIN),
            TimeIndexEntry::end(time.into_time()),
            ExplodedEdgeFilter,
        )
    }

    #[inline]
    pub fn latest() -> Latest<ExplodedEdgeFilter> {
        Latest::new(ExplodedEdgeFilter)
    }

    #[inline]
    pub fn snapshot_at<T: IntoTime>(time: T) -> SnapshotAt<ExplodedEdgeFilter> {
        SnapshotAt::new(time, ExplodedEdgeFilter)
    }

    #[inline]
    pub fn snapshot_latest() -> SnapshotLatest<ExplodedEdgeFilter> {
        SnapshotLatest::new(ExplodedEdgeFilter)
    }

    #[inline]
    pub fn layer<L: Into<Layer>>(layer: L) -> Layered<ExplodedEdgeFilter> {
        Layered::from_layers(layer, ExplodedEdgeFilter)
    }
}

impl Wrap for ExplodedEdgeFilter {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalPropertyFilterFactory for ExplodedEdgeFilter {
    type Entity = ExplodedEdgeFilter;
    type PropertyBuilder = PropertyFilterBuilder<Self::Entity>;
    type MetadataBuilder = MetadataFilterBuilder<Self::Entity>;

    fn entity(&self) -> Self::Entity {
        ExplodedEdgeFilter
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

impl<T: InternalNodeIdFilterBuilder> InternalNodeIdFilterBuilder
    for ExplodedEdgeEndpointWrapper<T>
{
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalNodeFilterBuilder> InternalNodeFilterBuilder for ExplodedEdgeEndpointWrapper<T> {
    type FilterType = T::FilterType;

    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalPropertyFilterBuilder> InternalPropertyFilterBuilder
    for ExplodedEdgeEndpointWrapper<T>
{
    type Filter = ExplodedEdgeEndpointWrapper<T::Filter>;
    type ExprBuilder = ExplodedEdgeEndpointWrapper<T::ExprBuilder>;
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

impl<T: InternalPropertyFilterFactory> InternalPropertyFilterFactory
    for ExplodedEdgeEndpointWrapper<T>
{
    type Entity = T::Entity;
    type PropertyBuilder = ExplodedEdgeEndpointWrapper<T::PropertyBuilder>;
    type MetadataBuilder = ExplodedEdgeEndpointWrapper<T::MetadataBuilder>;

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

impl<T: TemporalPropertyFilterFactory> TemporalPropertyFilterFactory
    for ExplodedEdgeEndpointWrapper<T>
{
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

impl<T> TryAsCompositeFilter for ExplodedEdgeEndpointWrapper<T>
where
    T: TryAsCompositeFilter + Clone,
{
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        let nf = self.inner.try_as_composite_node_filter()?;
        Ok(match self.endpoint {
            Endpoint::Src => CompositeExplodedEdgeFilter::Src(nf),
            Endpoint::Dst => CompositeExplodedEdgeFilter::Dst(nf),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeExplodedEdgeFilter {
    Src(CompositeNodeFilter),
    Dst(CompositeNodeFilter),
    Property(PropertyFilter<ExplodedEdgeFilter>),
    Windowed(Box<Windowed<CompositeExplodedEdgeFilter>>),
    Layered(Box<Layered<CompositeExplodedEdgeFilter>>),
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
            CompositeExplodedEdgeFilter::Layered(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeExplodedEdgeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeExplodedEdgeFilter::Not(filter) => write!(f, "(NOT {})", filter),
        }
    }
}

impl CreateFilter for CompositeExplodedEdgeFilter {
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
            Self::Src(filter) => {
                let wrapped = ExplodedEdgeEndpointWrapper::new(filter, Endpoint::Src);
                let filtered_graph = wrapped.create_filter(graph)?;
                Ok(Arc::new(filtered_graph))
            }
            Self::Dst(filter) => {
                let wrapped = ExplodedEdgeEndpointWrapper::new(filter, Endpoint::Dst);
                let filtered_graph = wrapped.create_filter(graph)?;
                Ok(Arc::new(filtered_graph))
            }
            Self::Property(p) => Ok(Arc::new(p.create_filter(graph)?)),
            Self::Windowed(pw) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                pw.create_filter(dyn_graph)
            }
            Self::Layered(pw) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                pw.create_filter(dyn_graph)
            }
            Self::And(l, r) => {
                let (l, r) = (*l, *r); // move out, no clone
                Ok(Arc::new(
                    AndFilter { left: l, right: r }.create_filter(graph)?,
                ))
            }
            Self::Or(l, r) => {
                let (l, r) = (*l, *r);
                Ok(Arc::new(
                    OrFilter { left: l, right: r }.create_filter(graph)?,
                ))
            }
            Self::Not(f) => {
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

impl TryAsCompositeFilter for CompositeExplodedEdgeFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(self.clone())
    }
}
