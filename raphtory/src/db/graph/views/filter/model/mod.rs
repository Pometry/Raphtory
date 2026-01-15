pub(crate) use crate::db::graph::views::filter::model::and_filter::AndFilter;
use crate::{
    db::graph::views::filter::model::{
        edge_filter::CompositeEdgeFilter,
        latest_filter::Latest,
        layered_filter::Layered,
        property_filter::{
            builders::{MetadataFilterBuilder, PropertyExprBuilder, PropertyFilterBuilder},
            Op, PropertyRef,
        },
        snapshot_filter::{SnapshotAt, SnapshotLatest},
        windowed_filter::Windowed,
    },
    prelude::PropertyFilter,
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
use std::{fmt::Display, ops::Deref, sync::Arc};

pub mod and_filter;
pub mod edge_filter;
pub mod exploded_edge_filter;
pub mod filter;
pub mod filter_operator;
pub mod latest_filter;
pub mod layered_filter;
pub mod node_filter;
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
    type Marker: Send + Sync + Clone + 'static;

    fn property_ref(&self) -> PropertyRef;

    fn ops(&self) -> &[Op];

    fn entity(&self) -> Self::Marker;

    fn filter(&self, filter: PropertyFilter<Self::Marker>) -> Self::Filter;

    fn into_expr_builder(&self, builder: PropertyExprBuilder<Self::Marker>) -> Self::ExprBuilder;
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

    fn filter(&self, filter: PropertyFilter<Self::Marker>) -> Self::Filter {
        self.deref().filter(filter)
    }

    fn into_expr_builder(&self, builder: PropertyExprBuilder<Self::Marker>) -> Self::ExprBuilder {
        self.deref().into_expr_builder(builder)
    }
}

pub trait InternalPropertyFilterFactory {
    type Entity: Clone + Send + Sync + 'static;
    type PropertyBuilder: InternalPropertyFilterBuilder + TemporalPropertyFilterFactory;
    type MetadataBuilder: InternalPropertyFilterBuilder;

    fn entity(&self) -> Self::Entity;

    fn property_builder(
        &self,
        builder: PropertyFilterBuilder<Self::Entity>,
    ) -> Self::PropertyBuilder;

    fn metadata_builder(
        &self,
        builder: MetadataFilterBuilder<Self::Entity>,
    ) -> Self::MetadataBuilder;
}

pub trait PropertyFilterFactory: InternalPropertyFilterFactory {
    fn property(&self, name: impl Into<String>) -> Self::PropertyBuilder {
        let builder = PropertyFilterBuilder::new(name, self.entity());
        self.property_builder(builder)
    }

    fn metadata(&self, name: impl Into<String>) -> Self::MetadataBuilder {
        let builder = MetadataFilterBuilder::new(name, self.entity());
        self.metadata_builder(builder)
    }
}

impl<T: InternalPropertyFilterFactory> PropertyFilterFactory for T {}

pub trait TemporalPropertyFilterFactory: InternalPropertyFilterBuilder {
    fn temporal(&self) -> Self::ExprBuilder {
        let builder = PropertyExprBuilder {
            prop_ref: PropertyRef::TemporalProperty(self.property_ref().name().to_string()),
            ops: vec![],
            entity: self.entity(),
        };
        self.into_expr_builder(builder)
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

pub trait ViewWrapOps: Sized {
    #[inline]
    fn window<S: IntoTime, E: IntoTime>(self, start: S, end: E) -> Windowed<Self> {
        Windowed::from_times(start, end, self)
    }

    #[inline]
    fn at<T: IntoTime>(self, time: T) -> Windowed<Self> {
        let t = time.into_time();
        Windowed::from_times(t, t.t().saturating_add(1), self)
    }

    #[inline]
    fn after<T: IntoTime>(self, time: T) -> Windowed<Self> {
        let start = time.into_time().t().saturating_add(1);
        Windowed::new(EventTime::start(start), EventTime::end(i64::MAX), self)
    }

    #[inline]
    fn before<T: IntoTime>(self, time: T) -> Windowed<Self> {
        Windowed::new(
            EventTime::start(i64::MIN),
            EventTime::end(time.into_time().t()),
            self,
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
