use crate::{
    db::{
        api::view::BoxableGraphView,
        graph::views::filter::{
            edge_node_filtered_graph::EdgeNodeFilteredGraph,
            internal::CreateFilter,
            model::{
                exploded_edge_filter::CompositeExplodedEdgeFilter,
                node_filter::{
                    CompositeNodeFilter, InternalNodeFilterBuilderOps,
                    InternalNodeIdFilterBuilderOps, NodeFilter, NodeFilterBuilderOps, NodeIdFilter,
                    NodeIdFilterBuilder, NodeIdFilterBuilderOps, NodeNameFilter,
                    NodeNameFilterBuilder, NodeTypeFilter, NodeTypeFilterBuilder,
                },
                property_filter::{
                    ElemQualifierOps, InternalPropertyFilterOps, ListAggOps, MetadataFilterBuilder,
                    Op, OpChainBuilder, PropertyFilter, PropertyFilterBuilder, PropertyFilterOps,
                    WindowedPropertyRef,
                },
                AndFilter, NotFilter, OrFilter, PropertyFilterFactory, TryAsCompositeFilter,
                Windowed,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::TimeIndexEntry,
};
use raphtory_core::utils::time::IntoTime;
use std::{fmt, fmt::Display, ops::Deref, sync::Arc};

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum Endpoint {
    Src,
    Dst,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeEdgeFilter {
    Src(CompositeNodeFilter),
    Dst(CompositeNodeFilter),
    Property(PropertyFilter<EdgeFilter>),
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
            CompositeEdgeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeEdgeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeEdgeFilter::Not(filter) => write!(f, "(NOT {})", filter),
        }
    }
}

impl CreateFilter for CompositeEdgeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        match self {
            CompositeEdgeFilter::Src(filter) => {
                let filtered_graph = filter.clone().create_filter(graph.clone())?;
                Ok(Arc::new(EdgeNodeFilteredGraph::new(
                    graph,
                    Endpoint::Src,
                    filtered_graph,
                )))
            }
            CompositeEdgeFilter::Dst(filter) => {
                let filtered_graph = filter.clone().create_filter(graph.clone())?;
                Ok(Arc::new(EdgeNodeFilteredGraph::new(
                    graph,
                    Endpoint::Dst,
                    filtered_graph,
                )))
            }
            CompositeEdgeFilter::Property(i) => Ok(Arc::new(i.create_filter(graph)?)),
            CompositeEdgeFilter::And(l, r) => Ok(Arc::new(
                AndFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_filter(graph)?,
            )),
            CompositeEdgeFilter::Or(l, r) => Ok(Arc::new(
                OrFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_filter(graph)?,
            )),
            CompositeEdgeFilter::Not(filter) => {
                let base = filter.deref().clone();
                Ok(Arc::new(NotFilter(base).create_filter(graph)?))
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

// User facing entry for building edge filters.
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct EdgeFilter;

impl PropertyFilterFactory<EdgeFilter> for EdgeFilter {}

impl EdgeFilter {
    #[inline]
    pub fn src() -> EdgeEndpoint {
        EdgeEndpoint::src()
    }

    #[inline]
    pub fn dst() -> EdgeEndpoint {
        EdgeEndpoint::dst()
    }

    #[inline]
    pub fn window<S: IntoTime, E: IntoTime>(start: S, end: E) -> Windowed<EdgeFilter> {
        Windowed::from_times(start, end)
    }
}

// Endpoint selector that exposes **node** filter builders for src/dst.
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct EdgeEndpoint(Endpoint);

impl EdgeEndpoint {
    #[inline]
    pub fn src() -> Self {
        Self(Endpoint::Src)
    }

    #[inline]
    pub fn dst() -> Self {
        Self(Endpoint::Dst)
    }
}

impl EdgeEndpoint {
    #[inline]
    pub fn id(&self) -> EndpointWrapper<NodeIdFilterBuilder> {
        EndpointWrapper::new(NodeFilter::id(), self.0)
    }

    #[inline]
    pub fn name(&self) -> EndpointWrapper<NodeNameFilterBuilder> {
        EndpointWrapper::new(NodeFilter::name(), self.0)
    }

    #[inline]
    pub fn node_type(&self) -> EndpointWrapper<NodeTypeFilterBuilder> {
        EndpointWrapper::new(NodeFilter::node_type(), self.0)
    }

    #[inline]
    pub fn property(
        &self,
        name: impl Into<String>,
    ) -> EndpointWrapper<PropertyFilterBuilder<NodeFilter>> {
        EndpointWrapper::new(PropertyFilterBuilder::<NodeFilter>::new(name), self.0)
    }

    #[inline]
    pub fn metadata(
        &self,
        name: impl Into<String>,
    ) -> EndpointWrapper<MetadataFilterBuilder<NodeFilter>> {
        EndpointWrapper::new(MetadataFilterBuilder::<NodeFilter>::new(name), self.0)
    }

    #[inline]
    pub fn window<S: IntoTime, E: IntoTime>(
        &self,
        start: S,
        end: E,
    ) -> EndpointWrapper<Windowed<NodeFilter>> {
        EndpointWrapper::new(NodeFilter::window(start, end), self.0)
    }
}

// Generic wrapper that pairs node-side builders with a concrete endpoint.
// The objective is to carry the endpoint through builder chain without having to change node builders
// and at the end convert into a composite node filter via TryAsCompositeFilter
#[derive(Debug, Clone)]
pub struct EndpointWrapper<T> {
    pub(crate) inner: T,
    endpoint: Endpoint,
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

    #[inline]
    pub fn with<U>(&self, inner: U) -> EndpointWrapper<U> {
        EndpointWrapper {
            inner,
            endpoint: self.endpoint,
        }
    }
}

impl<T: Display> Display for EndpointWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> TryAsCompositeFilter for EndpointWrapper<T>
where
    T: TryAsCompositeFilter + Clone,
{
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        let filter = self.inner.try_as_composite_node_filter()?;
        Ok(match self.endpoint {
            Endpoint::Src => CompositeEdgeFilter::Src(filter),
            Endpoint::Dst => CompositeEdgeFilter::Dst(filter),
        })
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

impl<T> CreateFilter for EndpointWrapper<T>
where
    T: TryAsCompositeFilter + Clone,
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
        let filter = self.try_as_composite_edge_filter()?;
        filter.create_filter(graph)
    }
}

impl<T: InternalPropertyFilterOps> InternalPropertyFilterOps for EndpointWrapper<T> {
    type Marker = T::Marker;
    #[inline]
    fn property_ref(&self) -> crate::db::graph::views::filter::model::property_filter::PropertyRef {
        self.inner.property_ref()
    }

    #[inline]
    fn ops(&self) -> &[Op] {
        self.inner.ops()
    }

    #[inline]
    fn window(&self) -> Option<(TimeIndexEntry, TimeIndexEntry)> {
        self.inner.window()
    }
}

impl<T: InternalPropertyFilterOps> EndpointWrapper<T> {
    #[inline]
    pub fn eq(&self, v: impl Into<Prop>) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.eq(v))
    }

    #[inline]
    pub fn ne(&self, v: impl Into<Prop>) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.ne(v))
    }

    #[inline]
    pub fn le(&self, v: impl Into<Prop>) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.le(v))
    }

    #[inline]
    pub fn ge(&self, v: impl Into<Prop>) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.ge(v))
    }

    #[inline]
    pub fn lt(&self, v: impl Into<Prop>) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.lt(v))
    }

    #[inline]
    pub fn gt(&self, v: impl Into<Prop>) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.gt(v))
    }

    #[inline]
    pub fn is_in(
        &self,
        vals: impl IntoIterator<Item = Prop>,
    ) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.is_in(vals))
    }

    #[inline]
    pub fn is_not_in(
        &self,
        vals: impl IntoIterator<Item = Prop>,
    ) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.is_not_in(vals))
    }

    #[inline]
    pub fn is_none(&self) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.is_none())
    }

    #[inline]
    pub fn is_some(&self) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.is_some())
    }

    #[inline]
    pub fn starts_with(&self, v: impl Into<Prop>) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.starts_with(v))
    }

    #[inline]
    pub fn ends_with(&self, v: impl Into<Prop>) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.ends_with(v))
    }

    #[inline]
    pub fn contains(&self, v: impl Into<Prop>) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.contains(v))
    }

    #[inline]
    pub fn not_contains(&self, v: impl Into<Prop>) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.not_contains(v))
    }

    #[inline]
    pub fn fuzzy_search(
        &self,
        s: impl Into<String>,
        d: usize,
        p: bool,
    ) -> EndpointWrapper<PropertyFilter<T::Marker>> {
        self.with(self.inner.fuzzy_search(s, d, p))
    }
}

impl EndpointWrapper<OpChainBuilder<NodeFilter>> {
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

impl EndpointWrapper<Windowed<NodeFilter>> {
    #[inline]
    pub fn property(
        &self,
        name: impl Into<String>,
    ) -> EndpointWrapper<WindowedPropertyRef<NodeFilter>> {
        self.with(self.inner.property(name))
    }

    #[inline]
    pub fn metadata(
        &self,
        name: impl Into<String>,
    ) -> EndpointWrapper<WindowedPropertyRef<NodeFilter>> {
        self.with(self.inner.metadata(name))
    }
}

impl EndpointWrapper<WindowedPropertyRef<NodeFilter>> {
    #[inline]
    pub fn temporal(self) -> Self {
        self.map(|w| w.temporal())
    }

    #[inline]
    pub fn any(self) -> Self {
        self.map(|w| w.any())
    }

    #[inline]
    pub fn all(self) -> Self {
        self.map(|w| w.all())
    }

    #[inline]
    pub fn len(self) -> Self {
        self.map(|w| w.len())
    }

    #[inline]
    pub fn sum(self) -> Self {
        self.map(|w| w.sum())
    }

    #[inline]
    pub fn avg(self) -> Self {
        self.map(|w| w.avg())
    }

    #[inline]
    pub fn min(self) -> Self {
        self.map(|w| w.min())
    }

    #[inline]
    pub fn max(self) -> Self {
        self.map(|w| w.max())
    }

    #[inline]
    pub fn first(self) -> Self {
        self.map(|w| w.first())
    }

    #[inline]
    pub fn last(self) -> Self {
        self.map(|w| w.last())
    }
}

impl EndpointWrapper<NodeIdFilterBuilder> {
    #[inline]
    pub fn eq<V: Into<GID>>(&self, v: V) -> EndpointWrapper<NodeIdFilter> {
        self.with(self.inner.eq(v))
    }

    #[inline]
    pub fn ne<V: Into<GID>>(&self, v: V) -> EndpointWrapper<NodeIdFilter> {
        self.with(self.inner.ne(v))
    }

    #[inline]
    pub fn is_in<I, V>(&self, vals: I) -> EndpointWrapper<NodeIdFilter>
    where
        I: IntoIterator<Item = V>,
        V: Into<GID>,
    {
        self.with(self.inner.is_in(vals))
    }

    #[inline]
    pub fn is_not_in<I, V>(&self, vals: I) -> EndpointWrapper<NodeIdFilter>
    where
        I: IntoIterator<Item = V>,
        V: Into<GID>,
    {
        self.with(self.inner.is_not_in(vals))
    }

    #[inline]
    pub fn lt<V: Into<GID>>(&self, v: V) -> EndpointWrapper<NodeIdFilter> {
        self.with(self.inner.lt(v))
    }

    #[inline]
    pub fn le<V: Into<GID>>(&self, v: V) -> EndpointWrapper<NodeIdFilter> {
        self.with(self.inner.le(v))
    }

    #[inline]
    pub fn gt<V: Into<GID>>(&self, v: V) -> EndpointWrapper<NodeIdFilter> {
        self.with(self.inner.gt(v))
    }

    #[inline]
    pub fn ge<V: Into<GID>>(&self, v: V) -> EndpointWrapper<NodeIdFilter> {
        self.with(self.inner.ge(v))
    }

    // string-y id ops (if allowed by your NodeIdFilter validation)
    #[inline]
    pub fn starts_with<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeIdFilter> {
        self.with(self.inner.starts_with(s))
    }

    #[inline]
    pub fn ends_with<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeIdFilter> {
        self.with(self.inner.ends_with(s))
    }

    #[inline]
    pub fn contains<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeIdFilter> {
        self.with(self.inner.contains(s))
    }

    #[inline]
    pub fn not_contains<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeIdFilter> {
        self.with(self.inner.not_contains(s))
    }

    #[inline]
    pub fn fuzzy_search<S: Into<String>>(
        &self,
        s: S,
        d: usize,
        p: bool,
    ) -> EndpointWrapper<NodeIdFilter> {
        self.with(self.inner.fuzzy_search(s, d, p))
    }
}

impl EndpointWrapper<NodeNameFilterBuilder> {
    #[inline]
    pub fn eq<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeNameFilter> {
        self.with(self.inner.eq(s.into()))
    }

    #[inline]
    pub fn ne<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeNameFilter> {
        self.with(self.inner.ne(s.into()))
    }

    #[inline]
    pub fn is_in<I>(&self, vals: I) -> EndpointWrapper<NodeNameFilter>
    where
        I: IntoIterator<Item = String>,
    {
        self.with(self.inner.is_in(vals))
    }

    #[inline]
    pub fn is_not_in<I>(&self, vals: I) -> EndpointWrapper<NodeNameFilter>
    where
        I: IntoIterator<Item = String>,
    {
        self.with(self.inner.is_not_in(vals))
    }

    #[inline]
    pub fn starts_with<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeNameFilter> {
        self.with(self.inner.starts_with(s.into()))
    }

    #[inline]
    pub fn ends_with<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeNameFilter> {
        self.with(self.inner.ends_with(s.into()))
    }

    #[inline]
    pub fn contains<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeNameFilter> {
        self.with(self.inner.contains(s.into()))
    }

    #[inline]
    pub fn not_contains<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeNameFilter> {
        self.with(self.inner.not_contains(s.into()))
    }

    #[inline]
    pub fn fuzzy_search<S: Into<String>>(
        &self,
        s: S,
        d: usize,
        p: bool,
    ) -> EndpointWrapper<NodeNameFilter> {
        self.with(self.inner.fuzzy_search(s.into(), d, p))
    }
}

impl EndpointWrapper<NodeTypeFilterBuilder> {
    #[inline]
    pub fn eq<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.eq(s.into()))
    }

    #[inline]
    pub fn ne<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.ne(s.into()))
    }

    #[inline]
    pub fn is_in<I>(&self, vals: I) -> EndpointWrapper<NodeTypeFilter>
    where
        I: IntoIterator<Item = String>,
    {
        self.with(self.inner.is_in(vals))
    }

    #[inline]
    pub fn is_not_in<I>(&self, vals: I) -> EndpointWrapper<NodeTypeFilter>
    where
        I: IntoIterator<Item = String>,
    {
        self.with(self.inner.is_not_in(vals))
    }

    #[inline]
    pub fn starts_with<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.starts_with(s.into()))
    }

    #[inline]
    pub fn ends_with<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.ends_with(s.into()))
    }

    #[inline]
    pub fn contains<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.contains(s.into()))
    }

    #[inline]
    pub fn not_contains<S: Into<String>>(&self, s: S) -> EndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.not_contains(s.into()))
    }

    #[inline]
    pub fn fuzzy_search<S: Into<String>>(
        &self,
        s: S,
        d: usize,
        p: bool,
    ) -> EndpointWrapper<NodeTypeFilter> {
        self.with(self.inner.fuzzy_search(s.into(), d, p))
    }
}

impl EndpointWrapper<PropertyFilterBuilder<NodeFilter>> {
    #[inline]
    pub fn temporal(self) -> EndpointWrapper<OpChainBuilder<NodeFilter>> {
        self.clone().with(self.inner.temporal())
    }
}
