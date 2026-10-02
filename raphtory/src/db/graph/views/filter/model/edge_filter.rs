use crate::{
    db::{
        api::view::internal::{GraphView, Static},
        graph::views::filter::model::{
            edge_expr::{ops::EdgeEndpointNodeOp, EdgeOp},
            expr::DynCreateHistory,
            latest_filter::Latest,
            layered_filter::Layered,
            node_expr::{CreateOp, EntityExpr, PredicateLhs},
            node_filter::NodeFilter,
            snapshot_filter::{SnapshotAt, SnapshotLatest},
            windowed_filter::Windowed,
            EntityMarker, InternalViewWrapOps, Wrap,
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

impl Static for EdgeFilter {}

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

impl InternalViewWrapOps for EdgeFilter {
    type Window = Windowed<EdgeFilter>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
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

    /// A history read on the node at this end of the edge.
    fn history(&self) -> Option<Arc<dyn DynCreateHistory>> {
        let inner = self.inner.history()?;
        Some(Arc::new(EdgeEndpointWrapper::new(inner, self.endpoint)))
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

impl EdgeFilterFactory for EdgeFilter {
    type EdgeWindow = Windowed<EdgeFilter>;
}
impl EdgeFilterFactory for ExplodedEdgeFilter {
    type EdgeWindow = Windowed<ExplodedEdgeFilter>;
}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for Windowed<T> {
    type EdgeWindow = T::EdgeWindow;
}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for Latest<T> {
    type EdgeWindow = Windowed<Latest<T>>;
}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for Layered<T> {
    type EdgeWindow = Layered<T::EdgeWindow>;
}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for SnapshotAt<T> {
    type EdgeWindow = Windowed<SnapshotAt<T>>;
}
impl<T: EdgeFilterFactory + CreateView> EdgeFilterFactory for SnapshotLatest<T> {
    type EdgeWindow = Windowed<SnapshotLatest<T>>;
}

// ── expr layer: temporal and aggregated terms on endpoint properties ──

use crate::db::graph::views::filter::model::node_expr::{EntityAggOps, TemporalPropExpr};

impl<E: CreateView + Clone + Send + Sync + 'static> EdgeEndpointWrapper<PropertyExpr<E>> {
    #[inline]
    pub fn temporal(&self) -> EdgeEndpointWrapper<TemporalPropExpr<E>> {
        EdgeEndpointWrapper::new(self.inner.temporal(), self.endpoint)
    }
}

/// Aggregations on an endpoint term come from the same trait as on a node term,
/// so they apply to any list-valued property, temporal or not.
impl<T: EntityExpr> EntityAggOps for EdgeEndpointWrapper<T> {}
