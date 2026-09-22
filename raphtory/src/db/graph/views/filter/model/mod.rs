pub(crate) use crate::db::graph::views::filter::model::and_filter::AndFilter;
pub use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::{
            filter::{
                model::{
                    edge_filter::{EdgeEndpointWrapper, EdgeFilter},
                    exploded_edge_filter::{ExplodedEdgeEndpointWrapper, ExplodedEdgeFilter},
                    filter_operator::{
                        BinaryOp, Comparable, FilterOperator, SetOp, StringComparable, StringOp,
                        UnaryOp,
                    },
                    node_expr::{
                        AllExpr, AnyExpr, AvgExpr, BinaryCmpExpr, EntityAggOps, FirstExpr,
                        LastExpr, LenExpr, MaxExpr, MinExpr, PropValueSetExpr, StringExpr, SumExpr,
                        TemporalPropExpr, UnaryExpr,
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
            state::{
                ops::{filter::NO_FILTER, Const},
                NodeOp,
            },
            view::{internal::DynGraphArc, BoxableGraphView},
        },
        graph::views::{
            filter::model::{
                is_active_edge_filter::IsActiveEdge,
                is_active_node_filter::IsActiveNode,
                is_deleted_filter::IsDeletedEdge,
                is_self_loop_filter::IsSelfLoopEdge,
                is_valid_filter::IsValidEdge,
                latest_filter::Latest,
                layered_filter::Layered,
                node_expr::{NodeMetaOp, NodePropOp},
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
            },
            layer_graph::LayeredGraph,
        },
    },
    prelude::LayerOps,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, Layer},
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::{ops::Deref, sync::Arc};

pub mod and_filter;
pub mod dyn_factory;
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
pub mod tree;
pub mod windowed_filter;

#[derive(Debug, Copy, Clone)]
pub struct Unfiltered;

impl CreateFilter for Unfiltered {
    type EntityFiltered<'graph, G, F>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;
    type NodeFilter<'graph, G, F>
        = Const<bool>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        _filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        Ok(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Ok(NO_FILTER)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
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

    fn not(self) -> NotFilter<Self> {
        NotFilter(self)
    }
}

pub trait DynCreateFilter: Send + Sync + 'static {
    fn create_dyn_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
        filtered: DynGraphArc<'graph>,
    ) -> Result<DynGraphArc<'graph>, GraphError>;

    fn create_dyn_node_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
        filtered: DynGraphArc<'graph>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError>;

    fn dyn_filter_graph_view<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynGraphArc<'graph>, GraphError>;
}

impl<T> DynCreateFilter for T
where
    T: CombinedFilter,
{
    fn create_dyn_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
        filtered: DynGraphArc<'graph>,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_filter(graph, filtered)?))
    }

    fn create_dyn_node_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
        filtered: DynGraphArc<'graph>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_node_filter(graph, filtered)?))
    }

    fn dyn_filter_graph_view<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        Ok(Arc::new(self.clone().filter_graph_view(graph)?))
    }
}

impl<T: DynCreateFilter + ?Sized + 'static> CreateFilter for Arc<T> {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = DynGraphArc<'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        self.deref()
            .create_dyn_filter(Arc::new(graph), Arc::new(filtered))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        self.deref()
            .create_dyn_node_filter(Arc::new(graph), Arc::new(filtered))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        self.deref().dyn_filter_graph_view(Arc::new(graph))
    }
}

#[derive(Copy, Clone)]
pub enum EntityMarker {
    Node,
    Edge,
    ExplodedEdge,
    Const,
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
    pub(crate) view_expr: E,
    pub(crate) name: String,
}

impl<E: EntityExpr> EntityExpr for PropertyExpr<E> {
    type Marker = E::Marker;

    fn entity(&self) -> Self::Marker {
        self.view_expr.entity()
    }
}

#[derive(Clone)]
pub struct MetadataExpr<E> {
    view_expr: E,
    name: String,
}

impl<E: EntityExpr> EntityExpr for MetadataExpr<E> {
    type Marker = E::Marker;
    fn entity(&self) -> Self::Marker {
        self.view_expr.entity()
    }
}

impl<E: EntityExpr + CreateView + Clone + Send + Sync + 'static> CreateOp for PropertyExpr<E> {
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

impl<E: EntityExpr + CreateView + Clone + Send + Sync + 'static> CreateOp for MetadataExpr<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let prop_id = graph
            .node_meta()
            .get_prop_id(&self.name, true)
            .ok_or_else(|| GraphError::MetadataMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(NodeMetaOp { graph, prop_id }))
    }

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

/// Entry point of the expr API: selects a property or metadata column on any view expression.
pub trait PropertyExprFactory: CreateView + EntityExpr + Sized {
    fn property(&self, name: impl Into<String>) -> PropertyExpr<Self>;

    fn metadata(&self, name: impl Into<String>) -> MetadataExpr<Self>;
}

impl<T: CreateView + EntityExpr + Clone> PropertyExprFactory for T {
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

pub trait DynPropertyExprFactory {
    fn dyn_property(&self, name: String) -> Arc<dyn DynTemporal>;
}

impl<T: PropertyExprFactory> DynPropertyExprFactory for T {
    fn dyn_property(&self, name: String) -> Arc<dyn DynTemporal> {
        Arc::new(self.property(name))
    }
}

impl<E: CreateView + Clone + Send + Sync + 'static> PropertyExpr<E> {
    pub fn temporal(&self) -> TemporalPropExpr<E> {
        TemporalPropExpr {
            view_expr: self.view_expr.clone(),
            name: self.name.clone(),
        }
    }
}

/// Aggregators apply to the latest value of a property when it is list-valued;
/// scalar values are rejected at filter-build time (`require_aggregable`).
impl<E: EntityExpr> EntityAggOps for PropertyExpr<E> {
    fn sum(self) -> SumExpr<Self> {
        SumExpr(self)
    }
    fn avg(self) -> AvgExpr<Self> {
        AvgExpr(self)
    }
    fn min(self) -> MinExpr<Self> {
        MinExpr(self)
    }
    fn max(self) -> MaxExpr<Self> {
        MaxExpr(self)
    }
    fn first(self) -> FirstExpr<Self> {
        FirstExpr(self)
    }
    fn last(self) -> LastExpr<Self> {
        LastExpr(self)
    }
    fn len(self) -> LenExpr<Self> {
        LenExpr(self)
    }
}

/// As for [`PropertyExpr`]: aggregation over a list-valued metadata field.
impl<E: EntityExpr> EntityAggOps for MetadataExpr<E> {
    fn sum(self) -> SumExpr<Self> {
        SumExpr(self)
    }
    fn avg(self) -> AvgExpr<Self> {
        AvgExpr(self)
    }
    fn min(self) -> MinExpr<Self> {
        MinExpr(self)
    }
    fn max(self) -> MaxExpr<Self> {
        MaxExpr(self)
    }
    fn first(self) -> FirstExpr<Self> {
        FirstExpr(self)
    }
    fn last(self) -> LastExpr<Self> {
        LastExpr(self)
    }
    fn len(self) -> LenExpr<Self> {
        LenExpr(self)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeFilterFactory — marker for edge-side filter factory types
// ─────────────────────────────────────────────────────────────────────────────

/// Marker trait for edge filter factory types (`EdgeFilter`, `Windowed<EdgeFilter>`, etc.).
///
/// Disjoint from `NodeFilterFactory`: no type implements both, so `PropertyExpr<E>`
/// can have two separate sets of comparison methods gated on each.
pub trait EdgeFilterFactory: PropertyExprFactory + Clone {}

// ─────────────────────────────────────────────────────────────────────────────
// PropertyExpr<E> / MetadataExpr<E> — EdgeExpr impls
// ─────────────────────────────────────────────────────────────────────────────

use crate::db::graph::views::filter::model::{
    edge_expr::ops::{EdgeMetaOp, EdgePropOp},
    graph_filter::GraphFilterOps,
    node_expr::{CreateOp, DynTemporal, EntityExpr, PredicateLhs},
};
use edge_expr::EdgeOp;
use raphtory_api::core::entities::properties::prop::PropType;

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

impl<T: DynInternalViewWrapOps + ?Sized> InternalViewWrapOps for Arc<T> {
    type Window = Arc<dyn DynInternalViewWrapOps>;

    fn bounds(&self) -> (EventTime, EventTime) {
        self.deref().dyn_bounds()
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        self.deref().dyn_build_window(start, end)
    }
}

/// The window `at(t)` means: every event at the timestamp `t`, whatever its
/// position within that timestamp.
pub(crate) fn at_bounds(t: EventTime) -> (EventTime, EventTime) {
    (
        EventTime::start(t.t()),
        EventTime::start(t.t().saturating_add(1)),
    )
}

/// The window `after(t)` means: everything strictly after `t`.
pub(crate) fn after_bounds(t: EventTime) -> (EventTime, EventTime) {
    (
        EventTime::start(t.t().saturating_add(1)),
        EventTime::end(i64::MAX),
    )
}

/// The window `before(t)` means: everything strictly before `t`. Events at
/// the timestamp `t` itself are excluded, matching `GraphViewOps::before`.
pub(crate) fn before_bounds(t: EventTime) -> (EventTime, EventTime) {
    (EventTime::start(i64::MIN), EventTime::start(t.t()))
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
        let (start, end) = at_bounds(time.into_time());
        self.window(start, end)
    }

    #[inline]
    fn after<T: IntoTime>(self, time: T) -> Self::Window {
        let (start, end) = after_bounds(time.into_time());
        self.window(start, end)
    }

    #[inline]
    fn before<T: IntoTime>(self, time: T) -> Self::Window {
        let (start, end) = before_bounds(time.into_time());
        self.window(start, end)
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

pub trait DynCreateView: Send + Sync + 'static {
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

impl<T: DynCreateView + ?Sized> CreateView for Arc<T> {
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
    fn entity(&self) -> Self::Marker {
        NodeFilter
    }
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
    fn entity(&self) -> Self::Marker {
        EdgeFilter
    }
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
    type Marker = ExplodedEdgeFilter;
    fn entity(&self) -> Self::Marker {
        ExplodedEdgeFilter
    }
}

impl<T: EntityExpr> EntityExpr for Windowed<T> {
    type Marker = T::Marker;
    fn entity(&self) -> Self::Marker {
        self.inner.entity()
    }
}

impl<T: EntityExpr> EntityExpr for Layered<T> {
    type Marker = T::Marker;
    fn entity(&self) -> Self::Marker {
        self.inner.entity()
    }
}

impl<T: EntityExpr> EntityExpr for Latest<T> {
    type Marker = T::Marker;
    fn entity(&self) -> Self::Marker {
        self.inner.entity()
    }
}

impl<T: EntityExpr> EntityExpr for SnapshotAt<T> {
    type Marker = T::Marker;
    fn entity(&self) -> Self::Marker {
        self.inner.entity()
    }
}

impl<T: EntityExpr> EntityExpr for SnapshotLatest<T> {
    type Marker = T::Marker;
    fn entity(&self) -> Self::Marker {
        self.inner.entity()
    }
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

pub trait DynViewFilter: DynCreateFilter + Send + Sync + 'static {
    fn dyn_bounds(&self) -> (EventTime, EventTime);

    fn dyn_build_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynViewFilter>;
}
impl<T> DynViewFilter for T
where
    T: GraphFilterOps,
{
    fn dyn_bounds(&self) -> (EventTime, EventTime) {
        self.bounds()
    }

    fn dyn_build_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynViewFilter> {
        Arc::new(self.clone().build_window(start, end))
    }
}

impl InternalViewWrapOps for Arc<dyn DynViewFilter> {
    type Window = Self;

    fn bounds(&self) -> (EventTime, EventTime) {
        self.deref().dyn_bounds()
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        self.deref().dyn_build_window(start, end)
    }
}

impl GraphFilterOps for DynView {
    type GraphWindow = Self;
}

pub type DynView = Arc<dyn DynViewFilter>;

pub type DynFilter = Arc<dyn DynCreateFilter>;

impl ComposableFilter for DynFilter {}
impl ComposableFilter for DynView {}

pub trait EdgeViewFilterOps: ViewWrapOps {
    type Output<T: CombinedFilter>: CombinedFilter;

    fn is_active(&self) -> Self::Output<IsActiveEdge>;

    fn is_valid(&self) -> Self::Output<IsValidEdge>;

    fn is_deleted(&self) -> Self::Output<IsDeletedEdge>;

    fn is_self_loop(&self) -> Self::Output<IsSelfLoopEdge>;
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityExprFilterOps — comparison and set operators on any EntityExpr
// ─────────────────────────────────────────────────────────────────────────────

/// Comparison, string, set, and presence operators on any [`CreateOp`].
///
/// `.any()` / `.all()` are qualifiers on a list-valued expression: the comparison that follows
/// is applied to each element and the results are reduced, so `.any().gt(10i64)` holds when any
/// element is greater than ten.
///
/// ```rust,ignore
/// NodeFilter.degree().gt(2usize)
/// NodeFilter.degree().sum() // TODO: Throw an error
/// NodeFilter.out_degree().gt(NodeFilter.in_degree())
/// NodeFilter.property("age").gt(30i64)
/// NodeFilter.property("score").temporal().gt(10i64).any()
/// ```
pub trait EntityExprFilterOps: EntityExpr + Sized {
    fn gt<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Gt, rhs, entity)
    }

    fn ge<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Ge, rhs, entity)
    }

    fn lt<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Lt, rhs, entity)
    }

    fn le<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Le, rhs, entity)
    }

    fn eq<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Eq, rhs, entity)
    }

    fn ne<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Ne, rhs, entity)
    }

    fn starts_with<R: EntityExpr>(self, rhs: R) -> StringExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        StringExpr::new(self, StringOp::StartsWith, rhs, entity)
    }

    fn ends_with<R: EntityExpr>(self, rhs: R) -> StringExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        StringExpr::new(self, StringOp::EndsWith, rhs, entity)
    }

    fn contains<R: EntityExpr>(self, rhs: R) -> StringExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        StringExpr::new(self, StringOp::Contains, rhs, entity)
    }

    fn not_contains<R: EntityExpr>(self, rhs: R) -> StringExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        StringExpr::new(self, StringOp::NotContains, rhs, entity)
    }

    fn fuzzy_search<R: EntityExpr>(
        self,
        rhs: R,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> StringExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        StringExpr::new(
            self,
            StringOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            rhs,
            entity,
        )
    }

    fn is_some(self) -> UnaryExpr<Self, Self::Marker> {
        let entity = self.entity();
        UnaryExpr {
            expr: self,
            op: UnaryOp::IsSome,
            entity,
        }
    }

    fn is_none(self) -> UnaryExpr<Self, Self::Marker> {
        let entity = self.entity();
        UnaryExpr {
            expr: self,
            op: UnaryOp::IsNone,
            entity,
        }
    }

    fn is_in<V: Into<Prop>>(
        self,
        values: impl IntoIterator<Item = V>,
    ) -> PropValueSetExpr<Self, Self::Marker> {
        let entity = self.entity();
        PropValueSetExpr {
            expr: self,
            values: values.into_iter().map(Into::into).collect(),
            op: SetOp::IsIn,
            entity,
        }
    }

    fn is_not_in<V: Into<Prop>>(
        self,
        values: impl IntoIterator<Item = V>,
    ) -> PropValueSetExpr<Self, Self::Marker> {
        let entity = self.entity();
        PropValueSetExpr {
            expr: self,
            values: values.into_iter().map(Into::into).collect(),
            op: SetOp::IsNotIn,
            entity,
        }
    }

    fn any(self) -> AnyExpr<Self> {
        AnyExpr(self)
    }

    fn all(self) -> AllExpr<Self> {
        AllExpr(self)
    }
}

impl<E: PredicateLhs> EntityExprFilterOps for E {}

// Concrete LHS markers
impl PredicateLhs for NodeFilter {}
impl PredicateLhs for EdgeFilter {}
impl PredicateLhs for ExplodedEdgeFilter {}

// Property / metadata accessors
impl<E: EntityExpr> PredicateLhs for PropertyExpr<E> {}
impl<E: EntityExpr> PredicateLhs for MetadataExpr<E> {}

// A view wrapper stands on the left-hand side whenever its inner expression does
impl<T: PredicateLhs> PredicateLhs for Windowed<T> {}
impl<T: PredicateLhs> PredicateLhs for Layered<T> {}
impl<T: PredicateLhs> PredicateLhs for Latest<T> {}
impl<T: PredicateLhs> PredicateLhs for SnapshotAt<T> {}
impl<T: PredicateLhs> PredicateLhs for SnapshotLatest<T> {}

/// Reject ordering operators on a type that has no ordering.
///
/// An unresolved type (`PropType::Empty`) passes; the check runs again once
/// the type is known.
pub fn validate_binary_op(op: &BinaryOp, prop_type: &PropType) -> Result<(), GraphError> {
    let ordering = matches!(
        op,
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    );
    if ordering && !prop_type.has_cmp() {
        let kind = match prop_type {
            PropType::List(_) => "list".to_string(),
            PropType::Map(_) => "map".to_string(),
            other => other.to_string(),
        };
        return Err(GraphError::InvalidFilter(format!(
            "operator {:?} is not valid for {} properties",
            op, kind
        )));
    }
    Ok(())
}

/// Reject string operators on non-string properties.
///
/// Only fires when the type is known (`!= PropType::Empty`).
pub fn validate_string_op(prop_type: &PropType) -> Result<(), GraphError> {
    if !(prop_type.is_unknown() || prop_type.is_str()) {
        return Err(GraphError::InvalidFilter(format!(
            "string operator requires a Str property, but the property type is {}",
            prop_type
        )));
    }
    Ok(())
}

/// Pick the more specific of the two known prop types.
///
/// Compiled `NodeOp`s and `EntityExpr`s may both have a known prop type, but
/// expression-level info (e.g. `DegreeExpr::prop_type()` → U64) is not always
/// propagated through generic wrappers like `Map<Op, V>`. Prefer whichever side
/// has a concrete type so validation can fire early.
pub fn resolved_prop_type(expr_pt: PropType, op_pt: PropType) -> PropType {
    if expr_pt != PropType::Empty {
        expr_pt
    } else {
        op_pt
    }
}

/// Reject a constant compared against an expression whose type it can never
/// equal.
///
/// Constants are never converted: a numeric constant compares by value with
/// any numeric expression (`degree() > 2.5` keeps the `.5`), a string constant
/// never compares with a number, and so on. A missing constant (`None`) and an
/// unresolved expression type both pass.
pub fn validate_const_comparable(
    lhs_pt: &PropType,
    value: Option<&Prop>,
) -> Result<(), GraphError> {
    match value {
        Some(v) if !lhs_pt.is_comparable_with(&v.dtype()) => {
            Err(GraphError::InvalidFilter(format!(
                "value {:?} of type {} cannot be compared with {}",
                v,
                v.dtype(),
                lhs_pt
            )))
        }
        _ => Ok(()),
    }
}

/// Reject a comparison between two expressions whose types can never be
/// equal. Either side being unresolved defers to runtime.
pub fn validate_types_comparable(lhs_pt: &PropType, rhs_pt: &PropType) -> Result<(), GraphError> {
    if lhs_pt.is_comparable_with(rhs_pt) {
        Ok(())
    } else {
        Err(GraphError::InvalidFilter(format!(
            "type mismatch: lhs is {}, rhs is {}",
            lhs_pt, rhs_pt
        )))
    }
}

/// Reject aggregators called on a declared scalar expression.
///
/// Lists and unresolved (`PropType::Empty`) types pass through — unresolved
/// is the case where a property name hasn't been looked up yet at expression-
/// build time, so we defer to filter-build / runtime to catch scalar/list
/// mismatches there. Anything declaring a scalar type up front (e.g.
/// `IsActiveNode` → `Bool`, `DegreeExpr` → `U64`) is rejected.
/// The element type a leading `any()`/`all()` chain compares against: one
/// list level is stripped per qualifier. Unknown types stay unknown; a
/// qualifier over a known scalar is an error.
pub fn elem_prop_type(pt: &PropType, levels: usize) -> Result<PropType, GraphError> {
    let mut pt = pt.clone();
    for _ in 0..levels {
        pt = match pt {
            PropType::List(inner) => *inner,
            PropType::Empty => PropType::Empty,
            other => {
                return Err(GraphError::InvalidFilter(format!(
                    "any()/all() require list or temporal values, found {other}"
                )))
            }
        };
    }
    Ok(pt)
}

pub fn require_aggregable(pt: &PropType, op: &str) -> Result<(), GraphError> {
    match pt {
        PropType::List(_) | PropType::Empty => Ok(()),
        _ => Err(GraphError::InvalidFilter(format!(
            "{} is not valid on a scalar expression of type {}",
            op, pt
        ))),
    }
}

/// Narrow an `is_in`/`is_not_in` set to the members that could equal the LHS.
///
/// Set membership asks whether a value is present, so a member of a type the
/// LHS can never equal simply is not present: it is dropped rather than
/// rejected, leaving `is_in` answering "no" where a comparison would refuse
/// the question. The members that remain are kept exactly as written; the
/// runtime comparison handles mixed numeric widths by value. An unresolved
/// LHS type keeps every member.
pub fn comparable_set_values(lhs_pt: &PropType, values: Vec<Prop>) -> Vec<Prop> {
    values
        .into_iter()
        .filter(|v| lhs_pt.is_comparable_with(&v.dtype()))
        .collect()
}

pub trait CombinedFilter: CreateFilter + Clone + Send + Sync + 'static {}

pub trait NodeViewFilterOps: ViewWrapOps {
    type Output<T: CombinedFilter>: CombinedFilter;

    fn is_active(&self) -> Self::Output<IsActiveNode>;
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CombinedFilter for T {}
