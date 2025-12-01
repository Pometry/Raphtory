pub(crate) use crate::db::graph::views::filter::model::and_filter::AndFilter;
use crate::db::graph::views::filter::model::{
    edge_filter::CompositeEdgeFilter,
    property_filter::builders::{MetadataFilterBuilder, OpChainBuilder, PropertyFilterBuilder},
};
pub use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::{
            filter::{
                internal::CreateFilter,
                model::{
                    edge_filter::{EdgeFilter, EndpointWrapper},
                    exploded_edge_filter::{
                        CompositeExplodedEdgeFilter, ExplodedEdgeFilter, ExplodedEndpointWrapper,
                    },
                    filter_operator::FilterOperator,
                    node_filter::{NodeFilter, NodeNameFilter, NodeTypeFilter},
                    not_filter::NotFilter,
                    or_filter::OrFilter,
                },
            },
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, TimeOps},
};
pub use node_filter::CompositeNodeFilter;
pub use property_filter::{
    builders::InternalPropertyFilterBuilderOps, Op, PropertyFilter, PropertyRef,
};
use raphtory_api::core::storage::timeindex::AsTime;
use raphtory_core::utils::time::IntoTime;
use std::{fmt::Display, ops::Deref, sync::Arc};

pub mod and_filter;
pub mod edge_filter;
pub mod exploded_edge_filter;
pub mod filter;
pub mod filter_operator;
pub mod node_filter;
pub mod not_filter;
pub mod or_filter;
pub mod property_filter;
pub mod windowed_filter;

trait EntityMarker: Clone + Send + Sync {}

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

pub trait InternalPropertyFilterFactory {
    type Entity: Clone + Send + Sync + 'static;
    type PropertyBuilder: InternalPropertyFilterBuilderOps + TemporalPropertyFilterFactory;
    type MetadataBuilder: InternalPropertyFilterBuilderOps;

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

pub trait TemporalPropertyFilterFactory: InternalPropertyFilterBuilderOps {
    fn temporal(&self) -> Self::Chained {
        let builder = OpChainBuilder {
            prop_ref: PropertyRef::TemporalProperty(self.property_ref().name().to_string()),
            ops: vec![],
            entity: self.entity(),
        };
        self.chained(builder)
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
