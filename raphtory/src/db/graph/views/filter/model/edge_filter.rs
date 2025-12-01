use crate::{
    db::{
        api::{
            state::ops::NotANodeFilter,
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            edge_node_filtered_graph::EdgeNodeFilteredGraph,
            internal::CreateFilter,
            model::{
                exploded_edge_filter::CompositeExplodedEdgeFilter,
                node_filter::{
                    builders::{
                        InternalNodeFilterBuilderOps, InternalNodeIdFilterBuilderOps,
                        NodeIdFilterBuilder, NodeNameFilterBuilder, NodeTypeFilterBuilder,
                    },
                    CompositeNodeFilter, NodeFilter,
                },
                property_filter::{
                    builders::{
                        InternalPropertyFilterBuilderOps, MetadataFilterBuilder, OpChainBuilder,
                        PropertyFilterBuilder,
                    },
                    Op, PropertyFilter, PropertyRef,
                },
                windowed_filter::Windowed,
                AndFilter, ComposableFilter, EntityMarker, InternalPropertyFilterFactory,
                NotFilter, OrFilter, TemporalPropertyFilterFactory, TryAsCompositeFilter, Wrap,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_core::utils::time::IntoTime;
use std::{fmt, fmt::Display, sync::Arc};

// User facing entry for building edge filters.
#[derive(Clone, Debug, Copy, Default, PartialEq, Eq)]
pub struct EdgeFilter;

impl EdgeFilter {
    #[inline]
    pub fn src() -> EndpointWrapper<NodeFilter> {
        EndpointWrapper::new(NodeFilter, Endpoint::Src)
    }

    #[inline]
    pub fn dst() -> EndpointWrapper<NodeFilter> {
        EndpointWrapper::new(NodeFilter, Endpoint::Dst)
    }

    #[inline]
    pub fn window<S: IntoTime, E: IntoTime>(start: S, end: E) -> Windowed<EdgeFilter> {
        Windowed::from_times(start, end, EdgeFilter)
    }
}

impl Wrap for EdgeFilter {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl EntityMarker for EdgeFilter {}

impl InternalPropertyFilterFactory for EdgeFilter {
    type Entity = EdgeFilter;
    type PropertyBuilder = PropertyFilterBuilder<EdgeFilter>;
    type MetadataBuilder = MetadataFilterBuilder<EdgeFilter>;

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
pub struct EndpointWrapper<T> {
    pub(crate) inner: T,
    endpoint: Endpoint,
}

impl<T: Display> Display for EndpointWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> EndpointWrapper<T> {
    #[inline]
    pub fn new(inner: T, endpoint: Endpoint) -> Self {
        Self { inner, endpoint }
    }

    #[inline]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> EndpointWrapper<U> {
        EndpointWrapper {
            inner: f(self.inner),
            endpoint: self.endpoint,
        }
    }
}

impl EndpointWrapper<NodeFilter> {
    #[inline]
    pub fn id(&self) -> EndpointWrapper<NodeIdFilterBuilder> {
        EndpointWrapper::new(NodeFilter::id(), self.endpoint)
    }

    #[inline]
    pub fn name(&self) -> EndpointWrapper<NodeNameFilterBuilder> {
        EndpointWrapper::new(NodeFilter::name(), self.endpoint)
    }

    #[inline]
    pub fn node_type(&self) -> EndpointWrapper<NodeTypeFilterBuilder> {
        EndpointWrapper::new(NodeFilter::node_type(), self.endpoint)
    }
}

impl<M> Wrap for EndpointWrapper<M> {
    type Wrapped<T> = EndpointWrapper<T>;

    fn wrap<T>(&self, inner: T) -> Self::Wrapped<T> {
        EndpointWrapper {
            inner,
            endpoint: self.endpoint,
        }
    }
}

impl<M> EntityMarker for EndpointWrapper<M> where M: EntityMarker + Send + Sync + Clone + 'static {}

impl<T> ComposableFilter for EndpointWrapper<T> where T: TryAsCompositeFilter + Clone {}

impl<T: InternalNodeIdFilterBuilderOps> InternalNodeIdFilterBuilderOps for EndpointWrapper<T> {
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalNodeFilterBuilderOps> InternalNodeFilterBuilderOps for EndpointWrapper<T> {
    type FilterType = T::FilterType;
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalPropertyFilterBuilderOps> InternalPropertyFilterBuilderOps for EndpointWrapper<T> {
    type Filter = EndpointWrapper<T::Filter>;
    type Chained = EndpointWrapper<T::Chained>;
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

    fn chained(&self, builder: OpChainBuilder<Self::Marker>) -> Self::Chained {
        self.wrap(self.inner.chained(builder))
    }
}

impl<T: InternalPropertyFilterFactory> InternalPropertyFilterFactory for EndpointWrapper<T> {
    type Entity = T::Entity;
    type PropertyBuilder = EndpointWrapper<T::PropertyBuilder>;
    type MetadataBuilder = EndpointWrapper<T::MetadataBuilder>;

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

impl<T: TemporalPropertyFilterFactory> TemporalPropertyFilterFactory for EndpointWrapper<T> {}

impl<T: CreateFilter + Clone + 'static> CreateFilter for EndpointWrapper<T> {
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

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for EndpointWrapper<T> {
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
                let wrapped = EndpointWrapper::new(filter, Endpoint::Src);
                let filtered_graph = wrapped.create_filter(graph)?;
                Ok(Arc::new(filtered_graph))
            }
            CompositeEdgeFilter::Dst(filter) => {
                let wrapped = EndpointWrapper::new(filter, Endpoint::Dst);
                let filtered_graph = wrapped.create_filter(graph)?;
                Ok(Arc::new(filtered_graph))
            }
            CompositeEdgeFilter::Property(i) => Ok(Arc::new(i.create_filter(graph)?)),
            CompositeEdgeFilter::Windowed(i) => {
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
