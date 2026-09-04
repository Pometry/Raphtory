use crate::{
    db::{
        api::{
            state::ops::{NodeOp, NotANodeFilter},
            view::internal::GraphView,
        },
        graph::views::filter::{
            edge_expr_filtered_graph::EdgeExprFilteredGraph,
            edge_node_filtered_graph::EdgeNodeFilteredGraph,
            model::{
                edge_expr::{ops::EdgeEndpointNodeOp, EdgeOp},
                is_active_edge_filter::IsActiveEdge,
                is_deleted_filter::IsDeletedEdge,
                is_self_loop_filter::IsSelfLoopEdge,
                is_valid_filter::IsValidEdge,
                latest_filter::Latest,
                layered_filter::Layered,
                node_expr::{CreateOp, EntityExpr, EntityExprBuilder},
                node_filter::{CompositeNodeFilter, NodeFilter},
                property_filter::PropertyFilter,
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                CombinedFilter, ComposableFilter, DynFilter, EdgeViewFilterOps, EntityMarker,
                InternalViewWrapOps, Wrap,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, properties::prop::Prop},
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::graph::GraphStorage;
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

// Generic wrapper that pairs a node-side expression with a concrete endpoint,
// carrying the endpoint through the chain so the compiled node op can be
// applied to the edge's src or dst node.
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
    /// Endpoint fields and properties are expressions: they compose with the
    /// comparison, string, set and temporal operators.
    #[inline]
    pub fn id(&self) -> EdgeEndpointWrapper<Id> {
        self.wrap(Id)
    }

    #[inline]
    pub fn name(&self) -> EdgeEndpointWrapper<Name> {
        self.wrap(Name)
    }

    #[inline]
    pub fn node_type(&self) -> EdgeEndpointWrapper<Type> {
        self.wrap(Type)
    }

    #[inline]
    pub fn property(
        &self,
        name: impl Into<String>,
    ) -> EdgeEndpointWrapper<PropertyExpr<NodeFilter>> {
        self.wrap(PropertyExprFactory::property(&self.inner, name))
    }

    #[inline]
    pub fn metadata(
        &self,
        name: impl Into<String>,
    ) -> EdgeEndpointWrapper<MetadataExpr<NodeFilter>> {
        self.wrap(PropertyExprFactory::metadata(&self.inner, name))
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

impl<T> ComposableFilter for EdgeEndpointWrapper<T> where T: Clone + Send + Sync {}

impl<T: CreateFilter + Clone + 'static> CreateFilter for EdgeEndpointWrapper<T> {
    type EntityFiltered<'graph, G, F>
        = EdgeNodeFilteredGraph<G, T::NodeFilter<'graph, G, F>>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type NodeFilter<'graph, G, F>
        = NotANodeFilter
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type FilteredGraph<'graph, G>
        = T::FilteredGraph<'graph, G>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let filter = self.inner.create_node_filter(graph.clone(), filtered)?;
        Ok(EdgeNodeFilteredGraph::new(graph, self.endpoint, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        self.inner.filter_graph_view(graph)
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

// ── expr layer: a full node filter evaluated on an edge endpoint ──

/// Evaluates an erased node filter against the src or dst node of each edge.
///
/// Carries whatever the nested filter is (combinators, views, property
/// conditions) by compiling it to a boolean node op and applying that to the
/// endpoint's VID at evaluation time.
#[derive(Clone)]
pub struct EdgeEndpointNodeFilter {
    pub endpoint: Endpoint,
    pub inner: DynFilter,
}

#[derive(Clone)]
struct EndpointNodeBoolOp<'g> {
    endpoint: Endpoint,
    node_op: Arc<dyn NodeOp<Output = bool> + 'g>,
}

impl<'g> EdgeOp for EndpointNodeBoolOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        let vid = match self.endpoint {
            Endpoint::Src => edge.src(),
            Endpoint::Dst => edge.dst(),
        };
        self.node_op.apply(storage, vid)
    }
}

impl CreateFilter for EdgeEndpointNodeFilter {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NotANodeFilter;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let node_op = self
            .inner
            .create_dyn_node_filter(Arc::new(graph.clone()), Arc::new(filtered))?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(EndpointNodeBoolOp {
            endpoint: self.endpoint,
            node_op,
        });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl ComposableFilter for EdgeEndpointNodeFilter {}

// ── expr layer: endpoint expressions bridge node ops into edge ops ──

impl<T: EntityExprBuilder> EntityExprBuilder for EdgeEndpointWrapper<T> {}

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

// ── expr layer: which types serve as edge-filter factories ──

use crate::db::{
    api::state::ops::node::{Id, Name, Type},
    graph::views::filter::model::{
        exploded_edge_filter::ExplodedEdgeFilter, CreateView, EdgeFilterFactory, MetadataExpr,
        PropertyExpr, PropertyExprFactory,
    },
};

impl EdgeFilterFactory for EdgeFilter {}
impl EdgeFilterFactory for ExplodedEdgeFilter {}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for Windowed<T> {}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for Latest<T> {}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for Layered<T> {}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for SnapshotAt<T> {}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for SnapshotLatest<T> {}

// ── expr layer: temporal chains on endpoint properties ──

use crate::db::graph::views::filter::model::node_expr::{
    AllExpr, AnyExpr, AvgExpr, EntityAggOps, FirstExpr, LastExpr, LenExpr, MaxExpr, MinExpr,
    SumExpr, TemporalPropExpr,
};

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
