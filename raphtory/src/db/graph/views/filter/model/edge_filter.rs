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
                is_active_edge_filter::IsActiveEdge,
                is_deleted_filter::IsDeletedEdge,
                is_self_loop_filter::IsSelfLoopEdge,
                is_valid_filter::IsValidEdge,
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
                    builders::{
                        MetadataFilterBuilder, PropertyExprBuilderInput, PropertyFilterBuilder,
                    },
                    Op, PropertyFilter, PropertyFilterInput, PropertyRef,
                },
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                AndFilter, CombinedFilter, ComposableFilter, EdgeViewFilterOps, EntityMarker,
                InternalPropertyFilterBuilder, InternalPropertyFilterFactory, InternalViewWrapOps,
                NotFilter, OrFilter, TemporalPropertyFilterFactory, TryAsCompositeFilter, Wrap,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::storage::timeindex::EventTime;
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

impl InternalPropertyFilterFactory for EdgeFilter {
    type Entity = EdgeFilter;
    type PropertyBuilder = PropertyFilterBuilder<Self::Entity>;
    type MetadataBuilder = MetadataFilterBuilder<Self::Entity>;

    fn entity(&self) -> Self::Entity {
        EdgeFilter
    }

    fn property_builder(&self, property: String) -> Self::PropertyBuilder {
        PropertyFilterBuilder(property, self.entity())
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        MetadataFilterBuilder(property, self.entity())
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

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        self.wrap(self.inner.filter(filter))
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        self.wrap(self.inner.with_expr_builder(builder))
    }
}

impl<T: InternalPropertyFilterFactory> InternalPropertyFilterFactory for EdgeEndpointWrapper<T> {
    type Entity = T::Entity;
    type PropertyBuilder = EdgeEndpointWrapper<T::PropertyBuilder>;
    type MetadataBuilder = EdgeEndpointWrapper<T::MetadataBuilder>;

    fn entity(&self) -> Self::Entity {
        self.inner.entity()
    }

    fn property_builder(&self, property: String) -> Self::PropertyBuilder {
        self.wrap(self.inner.property_builder(property))
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        self.wrap(self.inner.metadata_builder(property))
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

    type FilteredGraph<'graph, G>
        = T::FilteredGraph<'graph, G>
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

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        self.inner.filter_graph_view(graph)
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
    IsActiveEdge(Box<IsActiveEdge>),
    IsValidEdge(Box<IsValidEdge>),
    IsDeletedEdge(Box<IsDeletedEdge>),
    IsSelfLoopEdge(Box<IsSelfLoopEdge>),
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
            CompositeEdgeFilter::IsActiveEdge(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_filter(dyn_graph)
            }
            CompositeEdgeFilter::IsValidEdge(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_filter(dyn_graph)
            }
            CompositeEdgeFilter::IsDeletedEdge(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_filter(dyn_graph)
            }
            CompositeEdgeFilter::IsSelfLoopEdge(i) => {
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

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        match self.clone() {
            CompositeEdgeFilter::Src(filter) => {
                let wrapped = EdgeEndpointWrapper::new(filter, Endpoint::Src);
                let filtered_graph = wrapped.filter_graph_view(graph)?;
                Ok(Arc::new(filtered_graph))
            }
            CompositeEdgeFilter::Dst(filter) => {
                let wrapped = EdgeEndpointWrapper::new(filter, Endpoint::Dst);
                let filtered_graph = wrapped.filter_graph_view(graph)?;
                Ok(Arc::new(filtered_graph))
            }
            CompositeEdgeFilter::Property(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeEdgeFilter::Windowed(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeEdgeFilter::Latest(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeEdgeFilter::SnapshotAt(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeEdgeFilter::SnapshotLatest(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeEdgeFilter::IsActiveEdge(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeEdgeFilter::IsValidEdge(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeEdgeFilter::IsDeletedEdge(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeEdgeFilter::IsSelfLoopEdge(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeEdgeFilter::Layered(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeEdgeFilter::And(l, r) => {
                let (l, r) = (*l, *r);
                Ok(Arc::new(
                    AndFilter { left: l, right: r }.filter_graph_view(graph)?,
                ))
            }
            CompositeEdgeFilter::Or(l, r) => {
                let (l, r) = (*l, *r);
                Ok(Arc::new(
                    OrFilter { left: l, right: r }.filter_graph_view(graph)?,
                ))
            }
            CompositeEdgeFilter::Not(f) => {
                let base = *f;
                Ok(Arc::new(NotFilter(base).filter_graph_view(graph)?))
            }
        }
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
