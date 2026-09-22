use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::filter::model::{
            edge_expr::{ops::EdgeEndpointNodeOp, EdgeOp},
            is_active_edge_filter::IsActiveEdge,
            is_deleted_filter::IsDeletedEdge,
            is_self_loop_filter::IsSelfLoopEdge,
            is_valid_filter::IsValidEdge,
            latest_filter::Latest,
            layered_filter::Layered,
            node_expr::{CreateOp, EntityExpr, PredicateLhs},
            node_filter::NodeFilter,
            snapshot_filter::{SnapshotAt, SnapshotLatest},
            windowed_filter::Windowed,
            CombinedFilter, ComposableFilter, EdgeViewFilterOps, EntityMarker, InternalViewWrapOps,
            Wrap,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
use serde::{Deserialize, Serialize};
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

#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint
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

// ── expr layer: endpoint expressions bridge node ops into edge ops ──

impl<T: PredicateLhs> PredicateLhs for EdgeEndpointWrapper<T> {}

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
