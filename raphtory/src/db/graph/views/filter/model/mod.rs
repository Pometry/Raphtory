pub(crate) use crate::db::graph::views::filter::model::and_filter::AndFilter;
pub use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::{
            filter::{
                model::{
                    edge_filter::{EdgeEndpointWrapper, EdgeFilter},
                    exploded_edge_filter::{
                        CompositeExplodedEdgeFilter, ExplodedEdgeEndpointWrapper,
                        ExplodedEdgeFilter,
                    },
                    filter_operator::{
                        BinaryOp, Comparable, FilterOperator, SetOp, StringComparable, StringOp,
                        UnaryOp,
                    },
                    node_expr::{
                        AllExpr, AnyExpr, AvgExpr, BinaryCmpExpr,
                        EntityAggOps, EntityExprFilterOps, FirstExpr,
                        LastExpr, LenExpr, MaxExpr, MinExpr,
                        PropValueSetExpr, StringExpr, SumExpr,
                        TemporalExpr, TemporalProp, TemporalPropOps,
                        UnaryExpr,
                    },
                    node_filter::{NodeFilter, NodeFilterFactory},
                    not_filter::NotFilter,
                    or_filter::OrFilter,
                },
                CreateFilter,
            },
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, TimeOps},
};
use crate::{
    db::{
        api::{
            properties::TemporalPropertyView,
            state::{
                ops::{filter::NO_FILTER, Const},
                NodeOp,
            },
            view::BoxableGraphView,
        },
        graph::views::{
            filter::model::{
                edge_filter::CompositeEdgeFilter,
                is_active_edge_filter::IsActiveEdge,
                is_active_node_filter::IsActiveNode,
                is_deleted_filter::IsDeletedEdge,
                is_self_loop_filter::IsSelfLoopEdge,
                is_valid_filter::IsValidEdge,
                latest_filter::Latest,
                layered_filter::Layered,
                node_expr::{NodeMetaOp, NodePropOp},
                property_filter::{
                    builders::{
                        InternalPropertyFilterBuilder, PropertyExprBuilder,
                        PropertyExprBuilderInput,
                    },
                    Op, PropertyFilterInput, PropertyRef,
                },
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
            },
            layer_graph::LayeredGraph,
        },
    },
    prelude::LayerOps,
};
pub use node_filter::CompositeNodeFilter;
use raphtory_api::core::{
    entities::{properties::prop::Prop, Layer},
    storage::{arc_str::ArcStr, timeindex::{AsTime, EventTime}},
    utils::time::IntoTime,
};
use std::{marker::PhantomData, ops::Deref, sync::Arc};

pub mod and_filter;
pub mod degree_filter;
pub mod edge_expr;
pub mod edge_filter;
pub mod exploded_edge_filter;
pub mod filter;
pub mod filter_operator;
pub mod filter_value;
pub mod graph_filter;
pub mod is_active_edge_filter;
pub mod is_active_node_filter;
pub mod is_deleted_filter;
pub mod is_self_loop_filter;
pub mod is_valid_filter;
pub mod latest_filter;
pub mod layered_filter;
pub mod node_expr;
pub mod node_filter;
pub mod node_state_filter;
pub mod not_filter;
pub mod or_filter;
pub mod property_filter;
pub mod snapshot_filter;
pub mod windowed_filter;

#[derive(Debug, Copy, Clone)]
pub struct NoFilter;

impl CreateFilter for NoFilter {
    type EntityFiltered<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;
    type NodeFilter<'graph, G>
        = Const<bool>
    where
        Self: 'graph,
        G: GraphView + 'graph;
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NO_FILTER)
    }
}

impl TryAsCompositeFilter for NoFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

pub trait Wrap {
    type Wrapped<T>;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T>;
}

impl<S: Wrap> Wrap for Arc<S> {
    type Wrapped<T> = S::Wrapped<T>;
    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        self.deref().wrap(value)
    }
}

pub trait ComposableFilter: Sized {
    fn and<F>(self, other: F) -> AndFilter<Self, F> {
        AndFilter {
            left: self,
            right: other,
        }
    }

    fn or<F>(self, other: F) -> OrFilter<Self, F> {
        OrFilter {
            left: self,
            right: other,
        }
    }
}

pub trait DynCreateFilter: TryAsCompositeFilter + Send + Sync + 'static {
    fn create_dyn_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;

    fn create_dyn_node_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError>;
}

impl<T> DynCreateFilter for T
where
    T: CombinedFilter,
{
    fn create_dyn_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_filter(graph)?))
    }

    fn create_dyn_node_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_node_filter(graph)?))
    }
}

impl<T: DynCreateFilter + ?Sized + 'static> CreateFilter for Arc<T> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph> = Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        self.deref().create_dyn_filter(Arc::new(graph))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        self.deref().create_dyn_node_filter(Arc::new(graph))
    }
}

#[derive(Copy, Clone)]
pub enum EntityMarker {
    Node,
    Edge,
    ExplodedEdge,
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared property name expressions
//
// These structs carry only a property name. They implement both NodeExpr and
// EdgeExpr in their respective modules (node_expr/exprs.rs, edge_expr/exprs.rs),
// reading from node_meta() or edge_meta() depending on the context.
// ─────────────────────────────────────────────────────────────────────────────

/// Latest temporal property value — implements both `NodeExpr` and `EdgeExpr`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Property {
    pub name: String,
}

impl Property {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

/// Static metadata field — implements both `NodeExpr` and `EdgeExpr`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub name: String,
}

impl Metadata {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[derive(Clone)]
pub struct PropertyExpr<E> {
    view_expr: E,
    name: String,
}

impl<E: EntityExpr> EntityExpr for PropertyExpr<E> {
    type Marker = E::Marker;
}

impl<E: EntityExpr + CreateView + NodeFilterFactory + Clone + Send + Sync + 'static> NodeExpr
    for PropertyExpr<E>
{

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let prop_id = graph
            .node_meta()
            .get_prop_id(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(NodePropOp { graph, prop_id }))
    }
}

#[derive(Clone)]
pub struct MetadataExpr<E> {
    view_expr: E,
    name: String,
}

impl<E: EntityExpr> EntityExpr for MetadataExpr<E> {
    type Marker = E::Marker;
}

impl<E: EntityExpr + CreateView + NodeFilterFactory + Clone + Send + Sync + 'static> NodeExpr
    for MetadataExpr<E>
{
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let prop_id = graph
            .node_meta()
            .get_prop_id(&self.name, true)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(NodeMetaOp { graph, prop_id }))
    }
}

pub trait PropertyFilterFactory: Sized {
    fn property(&self, name: impl Into<String>) -> PropertyExpr<Self>;

    fn metadata(&self, name: impl Into<String>) -> MetadataExpr<Self>;
}

impl<T: CreateView + Clone> PropertyFilterFactory for T {
    fn property(&self, name: impl Into<String>) -> PropertyExpr<Self> {
        PropertyExpr {
            view_expr: self.clone(),
            name: name.into(),
        }
    }

    fn metadata(&self, name: impl Into<String>) -> MetadataExpr<Self> {
        MetadataExpr {
            view_expr: self.clone(),
            name: name.into(),
        }
    }
}

pub trait DynPropertyFilterFactory {
    fn property(&self, name: String) -> PropertyExpr<Arc<dyn DynCreateView>>;
}

impl<T: CreateView> DynPropertyFilterFactory for T {
    fn property(&self, name: String) -> PropertyExpr<Arc<dyn DynCreateView>> {
        PropertyExpr {
            view_expr: Arc::new(self.clone()) as Arc<dyn DynCreateView>,
            name,
        }
    }
}

impl<E> InternalPropertyFilterBuilder for PropertyExpr<E>
where
    E: Into<EntityMarker> + Send + Sync + Clone + 'static,
    crate::prelude::PropertyFilter<E>: CombinedFilter,
    PropertyExprBuilder<E>: InternalPropertyFilterBuilder,
{
    type Filter = crate::prelude::PropertyFilter<E>;
    type ExprBuilder = PropertyExprBuilder<E>;
    type Marker = E;

    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Property(self.name.clone())
    }

    fn ops(&self) -> &[Op] {
        &[]
    }

    fn entity(&self) -> Self::Marker {
        self.view_expr.clone()
    }

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        filter.with_entity(self.entity())
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        builder.with_entity(self.entity())
    }
}

impl<E> InternalPropertyFilterBuilder for MetadataExpr<E>
where
    E: Into<EntityMarker> + Send + Sync + Clone + 'static,
    crate::prelude::PropertyFilter<E>: CombinedFilter,
    PropertyExprBuilder<E>: InternalPropertyFilterBuilder,
{
    type Filter = crate::prelude::PropertyFilter<E>;
    type ExprBuilder = PropertyExprBuilder<E>;
    type Marker = E;

    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Metadata(self.name.clone())
    }

    fn ops(&self) -> &[Op] {
        &[]
    }

    fn entity(&self) -> Self::Marker {
        self.view_expr.clone()
    }

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        filter.with_entity(self.entity())
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        builder.with_entity(self.entity())
    }
}

impl<E: CreateView + Clone + Send + Sync + 'static> PropertyExpr<E> {
    pub fn temporal(&self) -> TemporalProp<E> {
        TemporalProp::new(self.view_expr.clone(), self.name.clone())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeFilterFactory — marker for edge-side filter builder types
// ─────────────────────────────────────────────────────────────────────────────

/// Marker trait for edge filter builder types (`EdgeFilter`, `Windowed<EdgeFilter>`, etc.).
///
/// Disjoint from `NodeFilterFactory`: no type implements both, so `PropertyExpr<E>`
/// can have two separate sets of comparison methods gated on each.
pub trait EdgeFilterFactory: PropertyFilterFactory + Clone {}

// ─────────────────────────────────────────────────────────────────────────────
// PropertyExpr<E> / MetadataExpr<E> — EdgeExpr impls
// ─────────────────────────────────────────────────────────────────────────────

use edge_expr::{
    EdgeExpr, EdgeOp
};
use crate::db::graph::views::filter::model::edge_expr::ops::{EdgeMetaOp, EdgePropOp};
use crate::db::graph::views::filter::model::node_expr::{EntityExpr, NodeExpr};

impl<E: EntityExpr + CreateView + EdgeFilterFactory + Clone + Send + Sync + 'static> EdgeExpr
    for PropertyExpr<E>
{
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let prop_id = graph
            .edge_meta()
            .get_prop_id(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(EdgePropOp { graph, prop_id }))
    }
}

impl<E: EntityExpr + CreateView + EdgeFilterFactory + Clone + Send + Sync + 'static> EdgeExpr
    for MetadataExpr<E>
{
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let prop_id = graph
            .edge_meta()
            .get_prop_id(&self.name, true)
            .ok_or_else(|| GraphError::MetadataMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(EdgeMetaOp { graph, prop_id }))
    }
}

pub trait TryAsCompositeFilter: Send + Sync {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError>;

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError>;

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError>;
}

impl<T: TryAsCompositeFilter + ?Sized> TryAsCompositeFilter for Arc<T> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        self.deref().try_as_composite_node_filter()
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        self.deref().try_as_composite_edge_filter()
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        self.deref().try_as_composite_exploded_edge_filter()
    }
}

pub trait CombinedFilter: CreateFilter + TryAsCompositeFilter + Clone + 'static {}

impl<T: CreateFilter + TryAsCompositeFilter + Clone + 'static> CombinedFilter for T {}

// This is implemented to avoid infinite recursive windowing.
pub trait InternalViewWrapOps: Send + Sync + Clone + 'static {
    type Window: InternalViewWrapOps;

    fn bounds(&self) -> (EventTime, EventTime) {
        (EventTime::MIN, EventTime::MAX)
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window;
}

pub trait DynInternalViewWrapOps: Send + Sync + 'static {
    fn dyn_bounds(&self) -> (EventTime, EventTime);

    fn dyn_build_window(&self, start: EventTime, end: EventTime)
        -> Arc<dyn DynInternalViewWrapOps>;
}

impl<T: InternalViewWrapOps> DynInternalViewWrapOps for T {
    fn dyn_bounds(&self) -> (EventTime, EventTime) {
        self.bounds()
    }

    fn dyn_build_window(
        &self,
        start: EventTime,
        end: EventTime,
    ) -> Arc<dyn DynInternalViewWrapOps> {
        Arc::new(self.clone().build_window(start, end))
    }
}

impl InternalViewWrapOps for Arc<dyn DynInternalViewWrapOps> {
    type Window = Arc<dyn DynInternalViewWrapOps>;

    fn bounds(&self) -> (EventTime, EventTime) {
        self.deref().dyn_bounds()
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        self.deref().dyn_build_window(start, end)
    }
}

pub trait ViewWrapOps: InternalViewWrapOps + Sized {
    #[inline]
    fn window<S: IntoTime, E: IntoTime>(self, start: S, end: E) -> Self::Window {
        let (old_start, old_end) = self.bounds();
        let end = end.into_time().min(old_end);
        let start = start.into_time().max(old_start).min(end);
        self.build_window(start, end)
    }

    #[inline]
    fn at<T: IntoTime>(self, time: T) -> Self::Window {
        let t = time.into_time();
        self.window(t, t.t().saturating_add(1))
    }

    #[inline]
    fn after<T: IntoTime>(self, time: T) -> Self::Window {
        let start = time.into_time().t().saturating_add(1);
        self.window(EventTime::start(start), EventTime::end(i64::MAX))
    }

    #[inline]
    fn before<T: IntoTime>(self, time: T) -> Self::Window {
        self.window(
            EventTime::start(i64::MIN),
            EventTime::end(time.into_time().t()),
        )
    }

    #[inline]
    fn latest(self) -> Latest<Self> {
        Latest::new(self)
    }

    #[inline]
    fn snapshot_at<T: IntoTime>(self, time: T) -> SnapshotAt<Self> {
        SnapshotAt::new(time, self)
    }

    #[inline]
    fn snapshot_latest(self) -> SnapshotLatest<Self> {
        SnapshotLatest::new(self)
    }

    #[inline]
    fn layer<L: Into<Layer>>(self, layer: L) -> Layered<Self> {
        Layered::from_layers(layer, self)
    }
}

impl<T: InternalViewWrapOps + Sized> ViewWrapOps for T {}

pub trait CreateView: Clone + Send + Sync + 'static {
    type View<'graph, G: GraphView + 'graph>: GraphView + 'graph;
    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError>;
}

pub trait DynCreateView: Send + Sync {
    fn dyn_create_view<'graph>(
        &self,
        view: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;
}

impl<T: CreateView> DynCreateView for T {
    fn dyn_create_view<'graph>(
        &self,
        view: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.create_view(view)?))
    }
}

impl CreateView for Arc<dyn DynCreateView> {
    type View<'graph, G: GraphView + 'graph> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        self.deref().dyn_create_view(Arc::new(view))
    }
}

impl CreateView for NodeFilter {
    type View<'graph, G: GraphView + 'graph> = G;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        Ok(view)
    }
}

impl EntityExpr for NodeFilter {
    type Marker = NodeFilter;
}

impl CreateView for EdgeFilter {
    type View<'graph, G: GraphView + 'graph> = G;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        Ok(view)
    }
}

impl EntityExpr for EdgeFilter {
    type Marker = EdgeFilter;
}

impl CreateView for ExplodedEdgeFilter {
    type View<'graph, G: GraphView + 'graph> = G;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        Ok(view)
    }
}

impl EntityExpr for ExplodedEdgeFilter {
    type Marker = EdgeFilter;
}

impl<T: EntityExpr> EntityExpr for Windowed<T> {
    type Marker = T::Marker;
}

impl<T: EntityExpr> EntityExpr for Layered<T> {
    type Marker = T::Marker;
}

impl<T: EntityExpr> EntityExpr for Latest<T> {
    type Marker = T::Marker;
}

impl<T: EntityExpr> EntityExpr for SnapshotAt<T> {
    type Marker = T::Marker;
}

impl<T: EntityExpr> EntityExpr for SnapshotLatest<T> {
    type Marker = T::Marker;
}

impl<T: CreateView> CreateView for Layered<T> {
    type View<'graph, G: GraphView + 'graph> = LayeredGraph<T::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<LayeredGraph<T::View<'graph, G>>, GraphError> {
        let inner = self.inner.create_view(view)?;
        inner.layers(self.layer.clone())
    }
}

pub trait ViewWrapPropOps: InternalViewWrapOps + PropertyFilterFactory + Sized {}

impl<T> ViewWrapPropOps for T where T: InternalViewWrapOps + PropertyFilterFactory + Sized {}

pub trait DynInternalViewWrapPropOps:
    DynInternalViewWrapOps + DynPropertyFilterFactory + DynCreateView
{
}

impl<T> DynInternalViewWrapPropOps for T where
    T: DynInternalViewWrapOps + DynPropertyFilterFactory + DynCreateView
{
}

impl InternalViewWrapOps for Arc<dyn DynInternalViewWrapPropOps> {
    type Window = Arc<dyn DynInternalViewWrapPropOps>;

    fn bounds(&self) -> (EventTime, EventTime) {
        self.deref().dyn_bounds()
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Arc::new(Windowed::new(start, end, self))
    }
}

impl CreateView for Arc<dyn DynInternalViewWrapPropOps> {
    type View<'graph, G: GraphView + 'graph> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        self.deref().dyn_create_view(Arc::new(view))
    }
}

pub trait DynViewFilter: DynInternalViewWrapOps + DynCreateFilter + Send + Sync + 'static {}
impl<T> DynViewFilter for T where T: DynInternalViewWrapOps + DynCreateFilter + Send + Sync + 'static
{}

pub type DynView = Arc<dyn DynViewFilter>;

pub type DynFilter = Arc<dyn DynCreateFilter>;

impl ComposableFilter for DynFilter {}
impl ComposableFilter for DynView {}

impl InternalViewWrapOps for DynView {
    type Window = DynView;

    fn bounds(&self) -> (EventTime, EventTime) {
        self.deref().dyn_bounds()
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Arc::new(Windowed::new(start, end, self))
    }
}

pub trait NodeViewFilterOps: ViewWrapOps {
    type Output<T: CombinedFilter>: CombinedFilter;

    fn is_active(&self) -> Self::Output<IsActiveNode<NodeFilter>>;
}

pub trait DynNodeViewFilterOps: DynInternalViewWrapPropOps + TryAsCompositeFilter {
    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter>;
}

impl<T: NodeViewFilterOps + DynInternalViewWrapPropOps + TryAsCompositeFilter> DynNodeViewFilterOps
    for T
{
    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter> {
        Arc::new(self.is_active())
    }
}

pub trait EdgeViewFilterOps: ViewWrapOps {
    type Output<T: CombinedFilter>: CombinedFilter;

    fn is_active(&self) -> Self::Output<IsActiveEdge>;

    fn is_valid(&self) -> Self::Output<IsValidEdge>;

    fn is_deleted(&self) -> Self::Output<IsDeletedEdge>;

    fn is_self_loop(&self) -> Self::Output<IsSelfLoopEdge>;
}

pub trait DynEdgeViewFilterOps: DynInternalViewWrapPropOps + TryAsCompositeFilter {
    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter>;

    fn dyn_is_valid(&self) -> Arc<dyn DynCreateFilter>;

    fn dyn_is_deleted(&self) -> Arc<dyn DynCreateFilter>;

    fn dyn_is_self_loop(&self) -> Arc<dyn DynCreateFilter>;
}

impl<T: EdgeViewFilterOps + DynInternalViewWrapPropOps + TryAsCompositeFilter> DynEdgeViewFilterOps
    for T
{
    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter> {
        Arc::new(self.is_active())
    }

    fn dyn_is_valid(&self) -> Arc<dyn DynCreateFilter> {
        Arc::new(self.is_valid())
    }

    fn dyn_is_deleted(&self) -> Arc<dyn DynCreateFilter> {
        Arc::new(self.is_deleted())
    }

    fn dyn_is_self_loop(&self) -> Arc<dyn DynCreateFilter> {
        Arc::new(self.is_self_loop())
    }
}

pub type DynNodeViewProps = Arc<dyn DynNodeViewFilterOps>;

impl CreateView for DynNodeViewProps {
    type View<'graph, G: GraphView + 'graph> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        self.deref().dyn_create_view(Arc::new(view))
    }
}

impl InternalViewWrapOps for DynNodeViewProps {
    type Window = DynNodeViewProps;

    fn bounds(&self) -> (EventTime, EventTime) {
        self.deref().dyn_bounds()
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Arc::new(Windowed::new(start, end, self))
    }
}

impl NodeViewFilterOps for DynNodeViewProps {
    type Output<T: CombinedFilter> = Arc<dyn DynCreateFilter>;

    fn is_active(&self) -> Self::Output<IsActiveNode<NodeFilter>> {
        self.deref().dyn_is_active()
    }
}

impl DynNodeViewFilterOps for Windowed<DynNodeViewProps> {
    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter> {
        Arc::new(IsActiveNode {
            view_expr: self.clone(),
        })
    }
}

pub type DynEdgeViewProps = Arc<dyn DynEdgeViewFilterOps>;

impl CreateView for DynEdgeViewProps {
    type View<'graph, G: GraphView + 'graph> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        self.deref().dyn_create_view(Arc::new(view))
    }
}

impl InternalViewWrapOps for DynEdgeViewProps {
    type Window = DynEdgeViewProps;

    fn bounds(&self) -> (EventTime, EventTime) {
        self.deref().dyn_bounds()
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Arc::new(Windowed::new(start, end, self))
    }
}

impl EdgeViewFilterOps for DynEdgeViewProps {
    type Output<T: CombinedFilter> = Arc<dyn DynCreateFilter>;

    fn is_active(&self) -> Self::Output<IsActiveEdge> {
        self.deref().dyn_is_active()
    }

    fn is_valid(&self) -> Self::Output<IsValidEdge> {
        self.deref().dyn_is_valid()
    }

    fn is_deleted(&self) -> Self::Output<IsDeletedEdge> {
        self.deref().dyn_is_deleted()
    }

    fn is_self_loop(&self) -> Self::Output<IsSelfLoopEdge> {
        self.deref().dyn_is_self_loop()
    }
}

