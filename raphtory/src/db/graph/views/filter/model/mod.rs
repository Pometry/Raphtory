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
                    builders::{PropertyExprBuilder, PropertyExprBuilderInput},
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
use raphtory_api::core::{
    entities::{properties::prop::Prop, Layer},
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::{ops::Deref, sync::Arc};

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
pub use exploded_edge_filter::CompositeExplodedEdgeFilter;
pub use node_filter::CompositeNodeFilter;
pub mod node_state_filter;
pub mod not_filter;
pub mod or_filter;
pub mod property_filter;
pub mod snapshot_filter;
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

impl TryAsCompositeFilter for Unfiltered {
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

    fn not(self) -> NotFilter<Self> {
        NotFilter(self)
    }
}

pub trait DynCreateFilter: TryAsCompositeFilter + Send + Sync + 'static {
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

pub trait PropertyFilterFactory: InternalPropertyFilterFactory {
    fn property(&self, name: impl Into<String>) -> Self::PropertyBuilder {
        self.property_builder(name.into())
    }

    fn metadata(&self, name: impl Into<String>) -> Self::MetadataBuilder {
        self.metadata_builder(name.into())
    }
}

impl<T: InternalPropertyFilterFactory> PropertyFilterFactory for T {}

pub trait TemporalPropertyFilterFactory: InternalPropertyFilterBuilder {
    fn temporal(&self) -> Self::ExprBuilder {
        let builder = PropertyExprBuilderInput {
            prop_ref: PropertyRef::TemporalProperty(self.property_ref().name().to_string()),
            ops: vec![],
        };
        self.with_expr_builder(builder)
    }
}

pub trait DynTemporalPropertyFilterBuilder: DynPropertyFilterBuilder {
    fn dyn_temporal(&self) -> Arc<dyn DynPropertyFilterBuilder>;
}

impl<T: TemporalPropertyFilterFactory + 'static> DynTemporalPropertyFilterBuilder for T {
    fn dyn_temporal(&self) -> Arc<dyn DynPropertyFilterBuilder> {
        Arc::new(self.temporal())
    }
}

impl TemporalPropertyFilterFactory for Arc<dyn DynTemporalPropertyFilterBuilder> {}

/// One graph-level view restriction, as data. `at`/`before`/`after` lower to
/// `Window` at construction time (see `ViewWrapOps`), so they need no
/// variants here.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphViewOp {
    Window { start: EventTime, end: EventTime },
    Latest,
    SnapshotAt(EventTime),
    SnapshotLatest,
    Layers(Layer),
}

/// Kind-tagged, owned export of a filter tree — the transportable form of a
/// composed filter, referencing no in-process state. `View` is an
/// outermost-first chain of graph-level restrictions. Produced by
/// [`TryAsCompositeFilter::try_as_filter_tree`]; filters that inherently
/// reference in-process state (e.g. node-state columns) cannot be exported
/// and return an error instead.
#[derive(Clone, Debug)]
pub enum FilterTree {
    Node(CompositeNodeFilter),
    Edge(CompositeEdgeFilter),
    ExplodedEdge(CompositeExplodedEdgeFilter),
    View(Vec<GraphViewOp>),
    And(Vec<FilterTree>),
    Or(Vec<FilterTree>),
    Not(Box<FilterTree>),
}

impl FilterTree {
    /// Whether any part of this expression tests edges.
    ///
    /// An edge test says nothing about which nodes belong in a node
    /// collection, so a node-collection subscript refuses such an expression.
    /// Lives here next to the enum so a new variant has to answer the question
    /// rather than silently defaulting somewhere else.
    pub fn tests_edges(&self) -> bool {
        match self {
            FilterTree::Edge(_) | FilterTree::ExplodedEdge(_) => true,
            FilterTree::Node(_) | FilterTree::View(_) => false,
            FilterTree::And(items) | FilterTree::Or(items) => {
                items.iter().any(FilterTree::tests_edges)
            }
            FilterTree::Not(inner) => inner.tests_edges(),
        }
    }
}

pub trait TryAsCompositeFilter: Send + Sync {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError>;

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError>;

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError>;

    /// Export this filter as a kind-tagged [`FilterTree`]. The default covers
    /// every single-kind filter via the composite exports; combinators and
    /// graph-view filters override it to preserve structure the single-kind
    /// exports cannot represent (mixed-kind trees, view chains). The kinds are
    /// tried node → edge → exploded-edge, so a filter that exports as more
    /// than one kind (e.g. the edge validity predicates) keeps its
    /// plain-edge export.
    fn try_as_filter_tree(&self) -> Result<FilterTree, GraphError> {
        if let Ok(f) = self.try_as_composite_node_filter() {
            return Ok(FilterTree::Node(f));
        }
        if let Ok(f) = self.try_as_composite_edge_filter() {
            return Ok(FilterTree::Edge(f));
        }
        Ok(FilterTree::ExplodedEdge(
            self.try_as_composite_exploded_edge_filter()?,
        ))
    }
}

impl<T: TryAsCompositeFilter + ?Sized> TryAsCompositeFilter for Arc<T> {
    fn try_as_filter_tree(&self) -> Result<FilterTree, GraphError> {
        self.deref().try_as_filter_tree()
    }

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
/// Distinct from [`PropertyFilterFactory`], the builder-path factory it will eventually replace.
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

pub trait DynPropertyFilterFactory: Send + Sync + 'static {
    fn dyn_entity(&self) -> EntityMarker;

    fn dyn_property_builder(&self, property: String) -> Arc<dyn DynTemporalPropertyFilterBuilder>;

    fn dyn_metadata_builder(&self, property: String) -> Arc<dyn DynPropertyFilterBuilder>;
}

pub trait DynPropertyExprFactory {
    fn dyn_property(&self, name: String) -> Arc<dyn DynTemporal>;
}

impl<T: PropertyExprFactory> DynPropertyExprFactory for T {
    fn dyn_property(&self, name: String) -> Arc<dyn DynTemporal> {
        Arc::new(self.property(name))
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
    pub fn temporal(&self) -> TemporalPropExpr<E> {
        TemporalPropExpr {
            view_expr: self.view_expr.clone(),
            name: self.name.clone(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeFilterFactory — marker for edge-side filter builder types
// ─────────────────────────────────────────────────────────────────────────────

/// Marker trait for edge filter builder types (`EdgeFilter`, `Windowed<EdgeFilter>`, etc.).
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
    node_expr::{CreateOp, DynTemporal, EntityExpr, EntityExprBuilder},
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

pub trait ViewWrapPropOps: InternalViewWrapOps + PropertyFilterFactory + Sized {}

impl<T> ViewWrapPropOps for T where T: InternalViewWrapOps + PropertyFilterFactory + Sized {}

pub trait DynInternalViewWrapPropOps: DynInternalViewWrapOps + DynPropertyFilterFactory {}

impl<T> DynInternalViewWrapPropOps for T where T: DynInternalViewWrapOps + DynPropertyFilterFactory {}

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

pub trait DynEdgeViewFilterOps: DynInternalViewWrapPropOps {
    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter>;

    fn dyn_is_valid(&self) -> Arc<dyn DynCreateFilter>;

    fn dyn_is_deleted(&self) -> Arc<dyn DynCreateFilter>;

    fn dyn_is_self_loop(&self) -> Arc<dyn DynCreateFilter>;
}

impl<T: EdgeViewFilterOps + DynInternalViewWrapPropOps> DynEdgeViewFilterOps for T {
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

pub type DynEdgeViewProps = Arc<dyn DynEdgeViewFilterOps>;

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

// ─────────────────────────────────────────────────────────────────────────────
// EntityExprFilterOps — comparison and set operators on any EntityExpr
// ─────────────────────────────────────────────────────────────────────────────

/// Comparison, string, set, and presence operators on any [`CreateOp`].
///
/// `.any()` / `.all()` are terminal: they wrap `self` in `AnyExpr`/`AllExpr` and compare the
/// result to `Bool(true)`. For element-wise comparison before reduction, chain in order:
/// `.gt(10i64).any()` not `.any().gt(10i64)`.
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

    fn is_true(self) -> BinaryCmpExpr<Self, Prop, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Eq, Prop::Bool(true), entity)
    }

    fn is_false(self) -> BinaryCmpExpr<Self, Prop, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Eq, Prop::Bool(false), entity)
    }

    fn not(self) -> BinaryCmpExpr<Self, Prop, Self::Marker> {
        self.eq(Prop::Bool(false))
    }

    fn any(self) -> AnyExpr<Self> {
        AnyExpr(self)
    }

    fn all(self) -> AllExpr<Self> {
        AllExpr(self)
    }
}

impl<E: EntityExprBuilder> EntityExprFilterOps for E {}

// Concrete LHS markers
impl EntityExprBuilder for NodeFilter {}
impl EntityExprBuilder for EdgeFilter {}
impl EntityExprBuilder for ExplodedEdgeFilter {}

// Property / metadata accessors
impl<E: EntityExpr> EntityExprBuilder for PropertyExpr<E> {}
impl<E: EntityExpr> EntityExprBuilder for MetadataExpr<E> {}

// View modifiers preserve builder-ness
impl<T: EntityExprBuilder> EntityExprBuilder for Windowed<T> {}
impl<T: EntityExprBuilder> EntityExprBuilder for Layered<T> {}
impl<T: EntityExprBuilder> EntityExprBuilder for Latest<T> {}
impl<T: EntityExprBuilder> EntityExprBuilder for SnapshotAt<T> {}
impl<T: EntityExprBuilder> EntityExprBuilder for SnapshotLatest<T> {}

/// Reject ordering operators on boolean properties.
//. TODO: Also check if both the types are comparable.
pub fn validate_binary_op(op: &BinaryOp, prop_type: &PropType) -> Result<(), GraphError> {
    if matches!(
        op,
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    ) {
        if *prop_type == PropType::Bool {
            return Err(GraphError::InvalidFilter(format!(
                "operator {:?} is not valid for boolean properties",
                op
            )));
        }
        if matches!(prop_type, PropType::Map(_)) {
            return Err(GraphError::InvalidFilter(format!(
                "operator {:?} is not valid for map properties",
                op
            )));
        }
        if matches!(prop_type, PropType::List(_)) {
            return Err(GraphError::InvalidFilter(format!(
                "operator {:?} is not valid for list properties",
                op
            )));
        }
    }
    Ok(())
}

/// Reject string operators on non-string properties.
///
/// Only fires when the type is known (`!= PropType::Empty`).
pub fn validate_string_op(prop_type: &PropType) -> Result<(), GraphError> {
    if *prop_type != PropType::Empty && *prop_type != PropType::Str {
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

/// Reject a constant RHS value whose type cannot be coerced to the LHS type.
///
/// Only fires when both sides are known and the RHS is a literal/const. Defers
/// to runtime when the LHS type is unknown (`PropType::Empty`) or the RHS isn't
/// a const value.
pub fn validate_const_castable(
    lhs_pt: &PropType,
    rhs_const: Option<&Prop>,
) -> Result<(), GraphError> {
    if *lhs_pt == PropType::Empty {
        return Ok(());
    }
    if let Some(rhs) = rhs_const {
        // Map values carry partial schemas against a union-schema declared
        // type and compare structurally at runtime; a non-map constant can
        // never match a map property.
        if matches!(lhs_pt, PropType::Map(_)) {
            return if matches!(rhs, Prop::Map(_)) {
                Ok(())
            } else {
                Err(GraphError::InvalidFilter(format!(
                    "value {:?} of type {} cannot be coerced to {}",
                    rhs,
                    rhs.dtype(),
                    lhs_pt
                )))
            };
        }
        if rhs.dtype() != *lhs_pt && rhs.clone().try_cast(lhs_pt.clone()).is_err() {
            return Err(GraphError::InvalidFilter(format!(
                "value {:?} of type {} cannot be coerced to {}",
                rhs,
                rhs.dtype(),
                lhs_pt
            )));
        }
    }
    Ok(())
}

/// A representative value for a given `PropType` — used to check type-level
/// compatibility via the value-based `Prop::try_cast` matrix. Returns `None`
/// for composite types (List, Map) where no canonical scalar default exists.
fn representative_prop(pt: &PropType) -> Option<Prop> {
    Some(match pt {
        PropType::Str => Prop::Str("".into()),
        PropType::U8 => Prop::U8(0),
        PropType::U16 => Prop::U16(0),
        PropType::U32 => Prop::U32(0),
        PropType::U64 => Prop::U64(0),
        PropType::I32 => Prop::I32(0),
        PropType::I64 => Prop::I64(0),
        PropType::F32 => Prop::F32(0.0),
        PropType::F64 => Prop::F64(0.0),
        PropType::Bool => Prop::Bool(false),
        PropType::Empty
        | PropType::List(_)
        | PropType::Map(_)
        | PropType::NDTime
        | PropType::DTime
        | PropType::Decimal { .. } => return None,
    })
}

/// Reject a binary comparison where LHS and RHS types are known but incompatible.
///
/// Complements `validate_const_castable` (which only checks const RHS) by also
/// catching mismatches when the RHS is another expression with a declared
/// `prop_type`. Uses the same `Prop::try_cast` matrix for coercion checks via
/// a representative value, so the numeric family (U/I/F) is considered
/// compatible while cross-domain (Bool vs U64, Str vs I64) is rejected.
///
/// Both sides being `Empty` defers to runtime (no-op).
pub fn validate_types_compatible(lhs_pt: &PropType, rhs_pt: &PropType) -> Result<(), GraphError> {
    if *lhs_pt == PropType::Empty || *rhs_pt == PropType::Empty || lhs_pt == rhs_pt {
        return Ok(());
    }
    let castable = representative_prop(rhs_pt)
        .and_then(|v| v.try_cast(lhs_pt.clone()).ok())
        .is_some();
    if !castable {
        return Err(GraphError::InvalidFilter(format!(
            "type mismatch: lhs is {}, rhs is {}",
            lhs_pt, rhs_pt
        )));
    }
    Ok(())
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

/// Cast every value in an `is_in`/`is_not_in` set to the LHS type.
///
/// If the LHS type is unknown (`PropType::Empty`), the values are returned
/// unchanged and coercion is deferred to runtime. Otherwise, any value whose
/// type cannot be coerced produces `Err(InvalidFilter)`. Successful casts are
/// substituted so the runtime set comparison sees same-typed values.
pub fn coerce_set_values(lhs_pt: &PropType, values: Vec<Prop>) -> Result<Vec<Prop>, GraphError> {
    if *lhs_pt == PropType::Empty {
        return Ok(values);
    }
    // Map values carry partial schemas and compare structurally; the declared
    // map type is the union schema, so per-value coercion would reject
    // legitimate members.
    if matches!(lhs_pt, PropType::Map(_)) {
        return Ok(values);
    }
    values
        .into_iter()
        .map(|v| {
            if v.dtype() == *lhs_pt {
                Ok(v)
            } else {
                let original_dtype = v.dtype();
                v.clone().try_cast(lhs_pt.clone()).map_err(|v| {
                    GraphError::InvalidFilter(format!(
                        "value {:?} of type {} cannot be coerced to {}",
                        v, original_dtype, lhs_pt
                    ))
                })
            }
        })
        .collect()
}

// ── composite-path machinery: still consumed by the GraphQL and permissions lowering ──

pub trait InternalPropertyFilterBuilder: Send + Sync {
    type Filter: CombinedFilter;
    type ExprBuilder: InternalPropertyFilterBuilder;
    type Marker: Into<EntityMarker> + Send + Sync + Clone + 'static;

    fn property_ref(&self) -> PropertyRef;

    fn ops(&self) -> &[Op];

    fn entity(&self) -> Self::Marker;

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter;

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder;
}

pub trait DynPropertyFilterBuilder: Send + Sync + 'static {
    fn dyn_property_ref(&self) -> PropertyRef;

    fn dyn_ops(&self) -> &[Op];

    fn dyn_entity(&self) -> EntityMarker;

    fn dyn_filter(&self, filter: PropertyFilterInput) -> Arc<dyn DynCreateFilter>;

    fn dyn_into_expr_builder(
        &self,
        builder: PropertyExprBuilderInput,
    ) -> Arc<dyn DynPropertyFilterBuilder>;
}

pub trait InternalPropertyFilterFactory {
    type Entity: Clone + Send + Sync + Into<EntityMarker> + 'static;
    type PropertyBuilder: InternalPropertyFilterBuilder + TemporalPropertyFilterFactory;
    type MetadataBuilder: InternalPropertyFilterBuilder;

    fn entity(&self) -> Self::Entity;

    fn property_builder(&self, property: String) -> Self::PropertyBuilder;

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder;
}

pub trait CombinedFilter: CreateFilter + TryAsCompositeFilter + Clone + 'static {}

pub trait NodeViewFilterOps: ViewWrapOps {
    type Output<T: CombinedFilter>: CombinedFilter;

    fn is_active(&self) -> Self::Output<IsActiveNode>;
}

pub trait DynNodeViewFilterOps: DynInternalViewWrapPropOps {
    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter>;
}

impl<T: InternalPropertyFilterBuilder + 'static> DynPropertyFilterBuilder for T {
    fn dyn_property_ref(&self) -> PropertyRef {
        self.property_ref()
    }

    fn dyn_ops(&self) -> &[Op] {
        self.ops()
    }

    fn dyn_entity(&self) -> EntityMarker {
        self.entity().into()
    }

    fn dyn_filter(&self, filter: PropertyFilterInput) -> Arc<dyn DynCreateFilter> {
        Arc::new(self.filter(filter))
    }

    fn dyn_into_expr_builder(
        &self,
        builder: PropertyExprBuilderInput,
    ) -> Arc<dyn DynPropertyFilterBuilder> {
        Arc::new(self.with_expr_builder(builder))
    }
}

impl InternalPropertyFilterBuilder for Arc<dyn DynPropertyFilterBuilder> {
    type Filter = Arc<dyn DynCreateFilter>;
    type ExprBuilder = Arc<dyn DynPropertyFilterBuilder>;
    type Marker = EntityMarker;

    fn property_ref(&self) -> PropertyRef {
        self.deref().dyn_property_ref()
    }

    fn ops(&self) -> &[Op] {
        self.deref().dyn_ops()
    }

    fn entity(&self) -> Self::Marker {
        self.deref().dyn_entity()
    }

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        self.deref().dyn_filter(filter)
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        self.deref().dyn_into_expr_builder(builder)
    }
}

impl InternalPropertyFilterBuilder for Arc<dyn DynTemporalPropertyFilterBuilder> {
    type Filter = Arc<dyn DynCreateFilter>;
    type ExprBuilder = Arc<dyn DynPropertyFilterBuilder>;
    type Marker = EntityMarker;

    fn property_ref(&self) -> PropertyRef {
        self.deref().dyn_property_ref()
    }

    fn ops(&self) -> &[Op] {
        self.deref().dyn_ops()
    }

    fn entity(&self) -> Self::Marker {
        self.deref().dyn_entity()
    }

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        self.deref().dyn_filter(filter)
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        self.deref().dyn_into_expr_builder(builder)
    }
}

impl<T: InternalPropertyFilterBuilder> InternalPropertyFilterBuilder for Arc<T> {
    type Filter = T::Filter;
    type ExprBuilder = T::ExprBuilder;
    type Marker = T::Marker;

    fn property_ref(&self) -> PropertyRef {
        self.deref().property_ref()
    }

    fn ops(&self) -> &[Op] {
        self.deref().ops()
    }

    fn entity(&self) -> Self::Marker {
        self.deref().entity()
    }

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        self.deref().filter(filter)
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        self.deref().with_expr_builder(builder)
    }
}

impl InternalPropertyFilterFactory for Arc<dyn DynPropertyFilterFactory> {
    type Entity = EntityMarker;
    type PropertyBuilder = Arc<dyn DynTemporalPropertyFilterBuilder>;
    type MetadataBuilder = Arc<dyn DynPropertyFilterBuilder>;

    fn entity(&self) -> Self::Entity {
        self.deref().dyn_entity()
    }

    fn property_builder(&self, property: String) -> Self::PropertyBuilder {
        self.deref().dyn_property_builder(property)
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        self.deref().dyn_metadata_builder(property)
    }
}

impl<T: CreateFilter + TryAsCompositeFilter + Clone + 'static> CombinedFilter for T {}

impl InternalPropertyFilterFactory for Arc<dyn DynInternalViewWrapPropOps> {
    type Entity = EntityMarker;
    type PropertyBuilder = Arc<dyn DynTemporalPropertyFilterBuilder>;
    type MetadataBuilder = Arc<dyn DynPropertyFilterBuilder>;

    fn entity(&self) -> Self::Entity {
        self.deref().dyn_entity()
    }

    fn property_builder(&self, property: String) -> Self::PropertyBuilder {
        self.deref().dyn_property_builder(property)
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        self.deref().dyn_metadata_builder(property)
    }
}

impl<T: NodeViewFilterOps + DynInternalViewWrapPropOps> DynNodeViewFilterOps for T {
    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter> {
        Arc::new(self.is_active())
    }
}

pub type DynNodeViewProps = Arc<dyn DynNodeViewFilterOps>;

impl NodeViewFilterOps for DynNodeViewProps {
    type Output<T: CombinedFilter> = Arc<dyn DynCreateFilter>;

    fn is_active(&self) -> Self::Output<IsActiveEdge> {
        self.deref().dyn_is_active()
    }
}

impl InternalPropertyFilterFactory for DynNodeViewProps {
    type Entity = EntityMarker;
    type PropertyBuilder = Arc<dyn DynTemporalPropertyFilterBuilder>;
    type MetadataBuilder = Arc<dyn DynPropertyFilterBuilder>;

    fn entity(&self) -> Self::Entity {
        self.deref().dyn_entity()
    }

    fn property_builder(&self, property: String) -> Self::PropertyBuilder {
        self.deref().dyn_property_builder(property)
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        self.deref().dyn_metadata_builder(property)
    }
}

impl InternalPropertyFilterFactory for DynEdgeViewProps {
    type Entity = EntityMarker;
    type PropertyBuilder = Arc<dyn DynTemporalPropertyFilterBuilder>;
    type MetadataBuilder = Arc<dyn DynPropertyFilterBuilder>;

    fn entity(&self) -> Self::Entity {
        self.deref().dyn_entity()
    }

    fn property_builder(&self, property: String) -> Self::PropertyBuilder {
        self.deref().dyn_property_builder(property)
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        self.deref().dyn_metadata_builder(property)
    }
}
