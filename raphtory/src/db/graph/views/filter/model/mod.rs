pub(crate) use crate::db::graph::views::filter::model::and_filter::AndFilter;
use crate::db::{
    api::{state::NodeOp, view::BoxableGraphView},
    graph::views::filter::model::{
        edge_filter::CompositeEdgeFilter,
        is_active_edge_filter::IsActiveEdge,
        is_active_node_filter::IsActiveNode,
        is_deleted_filter::IsDeletedEdge,
        is_self_loop_filter::IsSelfLoopEdge,
        is_valid_filter::IsValidEdge,
        latest_filter::Latest,
        layered_filter::Layered,
        property_filter::{
            builders::PropertyExprBuilderInput, Op, PropertyFilterInput, PropertyRef,
        },
        snapshot_filter::{SnapshotAt, SnapshotLatest},
        windowed_filter::Windowed,
    },
};
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
                    filter_operator::FilterOperator,
                    node_filter::{NodeFilter, NodeNameFilter, NodeTypeFilter},
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
pub use node_filter::CompositeNodeFilter;
use raphtory_api::core::{
    entities::Layer,
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::{ops::Deref, sync::Arc};

pub mod and_filter;
pub mod edge_filter;
pub mod exploded_edge_filter;
pub mod filter;
pub mod filter_operator;
pub mod graph_filter;
pub mod is_active_edge_filter;
pub mod is_active_node_filter;
pub mod is_deleted_filter;
pub mod is_self_loop_filter;
pub mod is_valid_filter;
pub mod latest_filter;
pub mod layered_filter;
pub mod node_filter;
pub mod node_state_filter;
pub mod not_filter;
pub mod or_filter;
pub mod property_filter;
pub mod snapshot_filter;
pub mod windowed_filter;

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

pub trait DynCreateFilter: TryAsCompositeFilter + Send + Sync + 'static {
    fn create_dyn_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;

    fn create_dyn_node_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError>;

    fn dyn_filter_graph_view<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;
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

    fn dyn_filter_graph_view<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().filter_graph_view(graph)?))
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

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        self.deref().dyn_filter_graph_view(Arc::new(graph))
    }
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

#[derive(Copy, Clone)]
pub enum EntityMarker {
    Node,
    Edge,
    ExplodedEdge,
}

pub trait InternalPropertyFilterFactory {
    type Entity: Clone + Send + Sync + Into<EntityMarker> + 'static;
    type PropertyBuilder: InternalPropertyFilterBuilder + TemporalPropertyFilterFactory;
    type MetadataBuilder: InternalPropertyFilterBuilder;

    fn entity(&self) -> Self::Entity;

    fn property_builder(&self, property: String) -> Self::PropertyBuilder;

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder;
}

pub trait DynPropertyFilterFactory: Send + Sync + 'static {
    fn dyn_entity(&self) -> EntityMarker;

    fn dyn_property_builder(&self, property: String) -> Arc<dyn DynTemporalPropertyFilterBuilder>;

    fn dyn_metadata_builder(&self, property: String) -> Arc<dyn DynPropertyFilterBuilder>;
}

impl<T: InternalPropertyFilterFactory + Send + Sync + 'static> DynPropertyFilterFactory for T {
    fn dyn_entity(&self) -> EntityMarker {
        self.entity().into()
    }

    fn dyn_property_builder(&self, property: String) -> Arc<dyn DynTemporalPropertyFilterBuilder> {
        Arc::new(self.property_builder(property))
    }

    fn dyn_metadata_builder(&self, property: String) -> Arc<dyn DynPropertyFilterBuilder> {
        Arc::new(self.metadata_builder(property))
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

pub trait ViewWrapPropOps: InternalViewWrapOps + InternalPropertyFilterFactory + Sized {}

impl<T> ViewWrapPropOps for T where T: InternalViewWrapOps + InternalPropertyFilterFactory + Sized {}

pub trait DynInternalViewWrapPropOps: DynInternalViewWrapOps + DynPropertyFilterFactory {}

impl<T> DynInternalViewWrapPropOps for T where T: DynInternalViewWrapOps + DynPropertyFilterFactory {}

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

impl InternalViewWrapOps for Arc<dyn DynInternalViewWrapPropOps> {
    type Window = Arc<dyn DynInternalViewWrapPropOps>;

    fn bounds(&self) -> (EventTime, EventTime) {
        self.deref().dyn_bounds()
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Arc::new(Windowed::new(start, end, self))
    }
}

pub trait DynViewFilter: DynInternalViewWrapOps + DynCreateFilter + Send + Sync + 'static {}
impl<T> DynViewFilter for T where T: DynInternalViewWrapOps + DynCreateFilter + Send + Sync + 'static
{}

pub type DynView = Arc<dyn DynViewFilter>;

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

    fn is_active(&self) -> Self::Output<IsActiveNode>;
}

pub trait DynNodeViewFilterOps: DynInternalViewWrapPropOps {
    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter>;
}

impl<T: NodeViewFilterOps + DynInternalViewWrapPropOps> DynNodeViewFilterOps for T {
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

pub type DynNodeViewProps = Arc<dyn DynNodeViewFilterOps>;

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

pub type DynEdgeViewProps = Arc<dyn DynEdgeViewFilterOps>;

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
