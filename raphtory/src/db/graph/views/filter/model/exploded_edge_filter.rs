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
                edge_filter::{CompositeEdgeFilter, Endpoint},
                node_filter::{
                    CompositeNodeFilter, InternalNodeFilterBuilderOps,
                    InternalNodeIdFilterBuilderOps, NodeFilter,
                },
                property_filter::{
                    InternalPropertyFilterBuilderOps, MetadataFilterBuilder, Op, PropertyFilter,
                    PropertyFilterBuilder, PropertyRef,
                },
                AndFilter, EntityMarker, NotFilter, OrFilter, TryAsCompositeFilter, Windowed, Wrap,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_core::utils::time::IntoTime;
use std::{fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeExplodedEdgeFilter {
    Src(CompositeNodeFilter),
    Dst(CompositeNodeFilter),
    Property(PropertyFilter<ExplodedEdgeFilter>),
    Windowed(Box<Windowed<CompositeExplodedEdgeFilter>>),

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
                let filter = filter.create_node_filter(graph.clone())?;
                Ok(Arc::new(EdgeNodeFilteredGraph::new(
                    graph,
                    Endpoint::Src,
                    filter,
                )))
            }
            Self::Dst(filter) => {
                let filter = filter.create_node_filter(graph.clone())?;
                Ok(Arc::new(EdgeNodeFilteredGraph::new(
                    graph,
                    Endpoint::Dst,
                    filter,
                )))
            }
            Self::Property(p) => Ok(Arc::new(p.create_filter(graph)?)),
            Self::Windowed(pw) => {
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

#[derive(Debug, Clone)]
pub struct ExplodedEndpointWrapper<T> {
    pub(crate) inner: T,
    endpoint: Endpoint,
}

impl<T> ExplodedEndpointWrapper<T> {
    #[inline]
    pub fn new(inner: T, endpoint: Endpoint) -> Self {
        Self { inner, endpoint }
    }

    #[inline]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> ExplodedEndpointWrapper<U> {
        ExplodedEndpointWrapper {
            inner: f(self.inner),
            endpoint: self.endpoint,
        }
    }
}

impl<T: Display> Display for ExplodedEndpointWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> TryAsCompositeFilter for ExplodedEndpointWrapper<T>
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

impl<T> CreateFilter for ExplodedEndpointWrapper<T>
where
    T: TryAsCompositeFilter + Clone + 'static,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        T: 'graph;
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
        self.try_as_composite_exploded_edge_filter()?
            .create_filter(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

impl<M> ExplodedEndpointWrapper<Windowed<M>>
where
    M: EntityMarker + Send + Sync + Clone + 'static,
{
    #[inline]
    pub fn property(
        &self,
        name: impl Into<String>,
    ) -> PropertyFilterBuilder<ExplodedEndpointWrapper<Windowed<M>>> {
        PropertyFilterBuilder::new(
            name.into(),
            ExplodedEndpointWrapper::new(self.inner.clone(), self.endpoint),
        )
    }

    #[inline]
    pub fn metadata(
        &self,
        name: impl Into<String>,
    ) -> MetadataFilterBuilder<ExplodedEndpointWrapper<Windowed<M>>> {
        MetadataFilterBuilder::new(
            name.into(),
            ExplodedEndpointWrapper::new(self.inner.clone(), self.endpoint),
        )
    }
}

impl<T: InternalPropertyFilterBuilderOps> InternalPropertyFilterBuilderOps
    for ExplodedEndpointWrapper<T>
{
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
}

#[derive(Clone, Debug, Copy, Default, PartialEq, Eq)]
pub struct ExplodedEdgeFilter;

impl ExplodedEdgeFilter {
    #[inline]
    pub fn src() -> ExplodedEndpointWrapper<NodeFilter> {
        ExplodedEndpointWrapper::new(NodeFilter, Endpoint::Src)
    }

    #[inline]
    pub fn dst() -> ExplodedEndpointWrapper<NodeFilter> {
        ExplodedEndpointWrapper::new(NodeFilter, Endpoint::Dst)
    }

    #[inline]
    pub fn window<S: IntoTime, E: IntoTime>(start: S, end: E) -> Windowed<ExplodedEdgeFilter> {
        Windowed::from_times(start, end, ExplodedEdgeFilter)
    }
}

impl<T: InternalNodeFilterBuilderOps> InternalNodeFilterBuilderOps for ExplodedEndpointWrapper<T> {
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalNodeIdFilterBuilderOps> InternalNodeIdFilterBuilderOps
    for ExplodedEndpointWrapper<T>
{
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<M> EntityMarker for ExplodedEndpointWrapper<M> where
    M: EntityMarker + Send + Sync + Clone + 'static
{
}

impl Wrap for ExplodedEdgeFilter {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl<M> Wrap for ExplodedEndpointWrapper<M> {
    type Wrapped<T> = ExplodedEndpointWrapper<T>;

    fn wrap<T>(&self, inner: T) -> Self::Wrapped<T> {
        ExplodedEndpointWrapper {
            inner,
            endpoint: self.endpoint,
        }
    }
}
