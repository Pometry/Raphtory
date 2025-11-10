use crate::{
    db::{
        api::view::BoxableGraphView,
        graph::views::filter::{
            edge_node_filtered_graph::EdgeNodeFilteredGraph,
            internal::CreateFilter,
            model::{
                edge_filter::{CompositeEdgeFilter, Endpoint},
                node_filter::{
                    CompositeNodeFilter, NodeFilter, NodeFilterBuilderOps, NodeIdFilter,
                    NodeIdFilterBuilder, NodeIdFilterBuilderOps, NodeNameFilter,
                    NodeNameFilterBuilder, NodeTypeFilter, NodeTypeFilterBuilder,
                },
                property_filter::{
                    InternalPropertyFilterOps, MetadataFilterBuilder, Op, OpChainBuilder,
                    PropertyFilter, PropertyFilterBuilder, PropertyFilterOps, PropertyRef,
                },
                AndFilter, EntityMarker, NotFilter, OrFilter, TryAsCompositeFilter, Windowed,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::{properties::prop::Prop, GID};
use raphtory_core::utils::time::IntoTime;
use std::{fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeExplodedEdgeFilter {
    Src(CompositeNodeFilter),
    Dst(CompositeNodeFilter),
    Property(PropertyFilter<ExplodedEdgeFilter>),
    PropertyWindowed(PropertyFilter<Windowed<ExplodedEdgeFilter>>),

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
            CompositeExplodedEdgeFilter::PropertyWindowed(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeExplodedEdgeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeExplodedEdgeFilter::Not(filter) => write!(f, "(NOT {})", filter),
        }
    }
}

impl CreateFilter for CompositeExplodedEdgeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        match self {
            Self::Src(filter) => {
                let filtered_graph = filter.create_filter(graph.clone())?;
                Ok(Arc::new(EdgeNodeFilteredGraph::new(
                    graph,
                    Endpoint::Src,
                    filtered_graph,
                )))
            }
            Self::Dst(filter) => {
                let filtered_graph = filter.create_filter(graph.clone())?;
                Ok(Arc::new(EdgeNodeFilteredGraph::new(
                    graph,
                    Endpoint::Dst,
                    filtered_graph,
                )))
            }
            Self::Property(p) => Ok(Arc::new(p.create_filter(graph)?)),
            Self::PropertyWindowed(pw) => Ok(Arc::new(pw.create_filter(graph)?)),
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

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct ExplodedEdgeEndpoint(Endpoint);

impl ExplodedEdgeEndpoint {
    #[inline]
    pub fn src() -> Self {
        Self(Endpoint::Src)
    }

    #[inline]
    pub fn dst() -> Self {
        Self(Endpoint::Dst)
    }

    #[inline]
    pub fn id(&self) -> ExplodedEndpointWrapper<NodeIdFilterBuilder> {
        ExplodedEndpointWrapper::new(NodeFilter::id(), self.0)
    }

    #[inline]
    pub fn name(&self) -> ExplodedEndpointWrapper<NodeNameFilterBuilder> {
        ExplodedEndpointWrapper::new(NodeFilter::name(), self.0)
    }

    #[inline]
    pub fn node_type(&self) -> ExplodedEndpointWrapper<NodeTypeFilterBuilder> {
        ExplodedEndpointWrapper::new(NodeFilter::node_type(), self.0)
    }

    #[inline]
    pub fn property(
        &self,
        name: impl Into<String>,
    ) -> PropertyFilterBuilder<ExplodedEndpointWrapper<NodeFilter>> {
        PropertyFilterBuilder::new(
            name.into(),
            ExplodedEndpointWrapper::new(NodeFilter, self.0),
        )
    }

    #[inline]
    pub fn metadata(
        &self,
        name: impl Into<String>,
    ) -> MetadataFilterBuilder<ExplodedEndpointWrapper<NodeFilter>> {
        MetadataFilterBuilder::new(
            name.into(),
            ExplodedEndpointWrapper::new(NodeFilter, self.0),
        )
    }

    #[inline]
    pub fn window<S: IntoTime, E: IntoTime>(
        &self,
        start: S,
        end: E,
    ) -> ExplodedEndpointWrapper<Windowed<NodeFilter>> {
        ExplodedEndpointWrapper::new(NodeFilter::window(start, end), self.0)
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

    #[inline]
    pub fn with<U>(&self, inner: U) -> ExplodedEndpointWrapper<U> {
        ExplodedEndpointWrapper {
            inner,
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
}

impl<T: InternalPropertyFilterOps> InternalPropertyFilterOps for ExplodedEndpointWrapper<T> {
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

impl<T: InternalPropertyFilterOps> ExplodedEndpointWrapper<T> {
    #[inline]
    pub fn eq(&self, v: impl Into<Prop>) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.eq(v))
    }

    #[inline]
    pub fn ne(&self, v: impl Into<Prop>) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.ne(v))
    }

    #[inline]
    pub fn le(&self, v: impl Into<Prop>) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.le(v))
    }

    #[inline]
    pub fn ge(&self, v: impl Into<Prop>) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.ge(v))
    }

    #[inline]
    pub fn lt(&self, v: impl Into<Prop>) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.lt(v))
    }

    #[inline]
    pub fn gt(&self, v: impl Into<Prop>) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.gt(v))
    }

    #[inline]
    pub fn is_in(
        &self,
        vals: impl IntoIterator<Item = Prop>,
    ) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.is_in(vals))
    }

    #[inline]
    pub fn is_not_in(
        &self,
        vals: impl IntoIterator<Item = Prop>,
    ) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.is_not_in(vals))
    }

    #[inline]
    pub fn is_none(&self) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.is_none())
    }

    #[inline]
    pub fn is_some(&self) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.is_some())
    }

    #[inline]
    pub fn starts_with(
        &self,
        v: impl Into<Prop>,
    ) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.starts_with(v))
    }

    #[inline]
    pub fn ends_with(
        &self,
        v: impl Into<Prop>,
    ) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.ends_with(v))
    }

    #[inline]
    pub fn contains(
        &self,
        v: impl Into<Prop>,
    ) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.contains(v))
    }

    #[inline]
    pub fn not_contains(
        &self,
        v: impl Into<Prop>,
    ) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.not_contains(v))
    }

    #[inline]
    pub fn fuzzy_search(
        &self,
        s: impl Into<String>,
        d: usize,
        p: bool,
    ) -> ExplodedEndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.fuzzy_search(s, d, p))
    }
}

impl ExplodedEndpointWrapper<OpChainBuilder<NodeFilter>> {
    #[inline]
    pub fn any(self) -> Self {
        self.map(|b| b.any())
    }

    #[inline]
    pub fn all(self) -> Self {
        self.map(|b| b.all())
    }

    #[inline]
    pub fn len(self) -> Self {
        self.map(|b| b.len())
    }

    #[inline]
    pub fn sum(self) -> Self {
        self.map(|b| b.sum())
    }

    #[inline]
    pub fn avg(self) -> Self {
        self.map(|b| b.avg())
    }

    #[inline]
    pub fn min(self) -> Self {
        self.map(|b| b.min())
    }

    #[inline]
    pub fn max(self) -> Self {
        self.map(|b| b.max())
    }

    #[inline]
    pub fn first(self) -> Self {
        self.map(|b| b.first())
    }

    #[inline]
    pub fn last(self) -> Self {
        self.map(|b| b.last())
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

impl ExplodedEndpointWrapper<OpChainBuilder<Windowed<NodeFilter>>> {
    #[inline]
    pub fn any(self) -> Self {
        self.map(|b| b.any())
    }

    #[inline]
    pub fn all(self) -> Self {
        self.map(|b| b.all())
    }

    #[inline]
    pub fn len(self) -> Self {
        self.map(|b| b.len())
    }

    #[inline]
    pub fn sum(self) -> Self {
        self.map(|b| b.sum())
    }

    #[inline]
    pub fn avg(self) -> Self {
        self.map(|b| b.avg())
    }

    #[inline]
    pub fn min(self) -> Self {
        self.map(|b| b.min())
    }

    #[inline]
    pub fn max(self) -> Self {
        self.map(|b| b.max())
    }

    #[inline]
    pub fn first(self) -> Self {
        self.map(|b| b.first())
    }

    #[inline]
    pub fn last(self) -> Self {
        self.map(|b| b.last())
    }
}

impl ExplodedEndpointWrapper<NodeIdFilterBuilder> {
    #[inline]
    pub fn eq<V: Into<GID>>(&self, v: V) -> ExplodedEndpointWrapper<NodeIdFilter> {
        self.with(self.inner.eq(v))
    }

    #[inline]
    pub fn ne<V: Into<GID>>(&self, v: V) -> ExplodedEndpointWrapper<NodeIdFilter> {
        self.with(self.inner.ne(v))
    }

    #[inline]
    pub fn is_in<I, V>(&self, vals: I) -> ExplodedEndpointWrapper<NodeIdFilter>
    where
        I: IntoIterator<Item = V>,
        V: Into<GID>,
    {
        self.with(self.inner.is_in(vals))
    }

    #[inline]
    pub fn is_not_in<I, V>(&self, vals: I) -> ExplodedEndpointWrapper<NodeIdFilter>
    where
        I: IntoIterator<Item = V>,
        V: Into<GID>,
    {
        self.with(self.inner.is_not_in(vals))
    }

    #[inline]
    pub fn lt<V: Into<GID>>(&self, v: V) -> ExplodedEndpointWrapper<NodeIdFilter> {
        self.with(self.inner.lt(v))
    }

    #[inline]
    pub fn le<V: Into<GID>>(&self, v: V) -> ExplodedEndpointWrapper<NodeIdFilter> {
        self.with(self.inner.le(v))
    }

    #[inline]
    pub fn gt<V: Into<GID>>(&self, v: V) -> ExplodedEndpointWrapper<NodeIdFilter> {
        self.with(self.inner.gt(v))
    }

    #[inline]
    pub fn ge<V: Into<GID>>(&self, v: V) -> ExplodedEndpointWrapper<NodeIdFilter> {
        self.with(self.inner.ge(v))
    }

    // string-y id ops (if allowed by your NodeIdFilter validation)
    #[inline]
    pub fn starts_with<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeIdFilter> {
        self.with(self.inner.starts_with(s))
    }

    #[inline]
    pub fn ends_with<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeIdFilter> {
        self.with(self.inner.ends_with(s))
    }

    #[inline]
    pub fn contains<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeIdFilter> {
        self.with(self.inner.contains(s))
    }

    #[inline]
    pub fn not_contains<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeIdFilter> {
        self.with(self.inner.not_contains(s))
    }

    #[inline]
    pub fn fuzzy_search<S: Into<String>>(
        &self,
        s: S,
        d: usize,
        p: bool,
    ) -> ExplodedEndpointWrapper<NodeIdFilter> {
        self.with(self.inner.fuzzy_search(s, d, p))
    }
}

impl ExplodedEndpointWrapper<NodeNameFilterBuilder> {
    #[inline]
    pub fn eq<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeNameFilter> {
        self.with(self.inner.eq(s.into()))
    }

    #[inline]
    pub fn ne<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeNameFilter> {
        self.with(self.inner.ne(s.into()))
    }

    #[inline]
    pub fn is_in<I>(&self, vals: I) -> ExplodedEndpointWrapper<NodeNameFilter>
    where
        I: IntoIterator<Item = String>,
    {
        self.with(self.inner.is_in(vals))
    }

    #[inline]
    pub fn is_not_in<I>(&self, vals: I) -> ExplodedEndpointWrapper<NodeNameFilter>
    where
        I: IntoIterator<Item = String>,
    {
        self.with(self.inner.is_not_in(vals))
    }

    #[inline]
    pub fn starts_with<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeNameFilter> {
        self.with(self.inner.starts_with(s.into()))
    }

    #[inline]
    pub fn ends_with<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeNameFilter> {
        self.with(self.inner.ends_with(s.into()))
    }

    #[inline]
    pub fn contains<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeNameFilter> {
        self.with(self.inner.contains(s.into()))
    }

    #[inline]
    pub fn not_contains<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeNameFilter> {
        self.with(self.inner.not_contains(s.into()))
    }

    #[inline]
    pub fn fuzzy_search<S: Into<String>>(
        &self,
        s: S,
        d: usize,
        p: bool,
    ) -> ExplodedEndpointWrapper<NodeNameFilter> {
        self.with(self.inner.fuzzy_search(s.into(), d, p))
    }
}

impl ExplodedEndpointWrapper<NodeTypeFilterBuilder> {
    #[inline]
    pub fn eq<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.eq(s.into()))
    }

    #[inline]
    pub fn ne<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.ne(s.into()))
    }

    #[inline]
    pub fn is_in<I>(&self, vals: I) -> ExplodedEndpointWrapper<NodeTypeFilter>
    where
        I: IntoIterator<Item = String>,
    {
        self.with(self.inner.is_in(vals))
    }

    #[inline]
    pub fn is_not_in<I>(&self, vals: I) -> ExplodedEndpointWrapper<NodeTypeFilter>
    where
        I: IntoIterator<Item = String>,
    {
        self.with(self.inner.is_not_in(vals))
    }

    #[inline]
    pub fn starts_with<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.starts_with(s.into()))
    }

    #[inline]
    pub fn ends_with<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.ends_with(s.into()))
    }

    #[inline]
    pub fn contains<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.contains(s.into()))
    }

    #[inline]
    pub fn not_contains<S: Into<String>>(&self, s: S) -> ExplodedEndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.not_contains(s.into()))
    }

    #[inline]
    pub fn fuzzy_search<S: Into<String>>(
        &self,
        s: S,
        d: usize,
        p: bool,
    ) -> ExplodedEndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.fuzzy_search(s.into(), d, p))
    }
}

#[derive(Clone, Debug, Copy, Default, PartialEq, Eq)]
pub struct ExplodedEdgeFilter;

impl ExplodedEdgeFilter {
    #[inline]
    pub fn src() -> ExplodedEdgeEndpoint {
        ExplodedEdgeEndpoint::src()
    }

    #[inline]
    pub fn dst() -> ExplodedEdgeEndpoint {
        ExplodedEdgeEndpoint::dst()
    }

    #[inline]
    pub fn window<S: IntoTime, E: IntoTime>(start: S, end: E) -> Windowed<ExplodedEdgeFilter> {
        Windowed::from_times(start, end)
    }
}

impl TryAsCompositeFilter for PropertyFilter<ExplodedEndpointWrapper<NodeFilter>> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        let node_prop = PropertyFilter::<NodeFilter> {
            prop_ref: self.prop_ref.clone(),
            prop_value: self.prop_value.clone(),
            operator: self.operator.clone(),
            ops: self.ops.clone(),
            entity: NodeFilter,
        };
        let node_cf = node_prop.try_as_composite_node_filter()?;
        Ok(match self.entity.endpoint {
            Endpoint::Src => CompositeExplodedEdgeFilter::Src(node_cf),
            Endpoint::Dst => CompositeExplodedEdgeFilter::Dst(node_cf),
        })
    }
}

impl CreateFilter for PropertyFilter<ExplodedEndpointWrapper<NodeFilter>> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        self.try_as_composite_exploded_edge_filter()?
            .create_filter(graph)
    }
}

impl TryAsCompositeFilter for PropertyFilter<ExplodedEndpointWrapper<Windowed<NodeFilter>>> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        let node_prop = PropertyFilter::<Windowed<NodeFilter>> {
            prop_ref: self.prop_ref.clone(),
            prop_value: self.prop_value.clone(),
            operator: self.operator.clone(),
            ops: self.ops.clone(),
            entity: self.entity.inner.clone(),
        };
        let node_cf = node_prop.try_as_composite_node_filter()?;
        Ok(match self.entity.endpoint {
            Endpoint::Src => CompositeExplodedEdgeFilter::Src(node_cf),
            Endpoint::Dst => CompositeExplodedEdgeFilter::Dst(node_cf),
        })
    }
}

impl CreateFilter for PropertyFilter<ExplodedEndpointWrapper<Windowed<NodeFilter>>> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        self.try_as_composite_exploded_edge_filter()?
            .create_filter(graph)
    }
}

impl<M> EntityMarker for ExplodedEndpointWrapper<M> where
    M: EntityMarker + Send + Sync + Clone + 'static
{
}
