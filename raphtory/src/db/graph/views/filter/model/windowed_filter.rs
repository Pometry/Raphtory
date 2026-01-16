use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::{
            filter::{
                model::{
                    edge_filter::CompositeEdgeFilter,
                    node_filter::builders::{
                        InternalNodeFilterBuilder, InternalNodeIdFilterBuilder,
                    },
                    property_filter::{builders::PropertyExprBuilderInput, PropertyFilterInput},
                    ComposableFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter,
                    InternalPropertyFilterBuilder, InternalPropertyFilterFactory,
                    InternalViewWrapOps, Op, PropertyRef, TemporalPropertyFilterFactory,
                    TryAsCompositeFilter, Wrap,
                },
                CreateFilter,
            },
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, TimeOps},
};
use raphtory_api::core::{
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Windowed<M> {
    pub start: EventTime,
    pub end: EventTime,
    pub inner: M,
}

impl<M: Display> Display for Windowed<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WINDOW[{}..{}]({})",
            self.start.t(),
            self.end.t(),
            self.inner
        )
    }
}

impl<M> Windowed<M> {
    #[inline]
    pub fn new(start: EventTime, end: EventTime, entity: M) -> Self {
        Self {
            start,
            end,
            inner: entity,
        }
    }

    #[inline]
    pub fn from_times<S: IntoTime, E: IntoTime>(start: S, end: E, entity: M) -> Self {
        let s = start.into_time();
        let e = end.into_time();
        Self::new(s, e, entity)
    }
}

impl<T: InternalViewWrapOps> InternalViewWrapOps for Windowed<T> {
    type Window = T::Window;

    fn bounds(&self) -> (EventTime, EventTime) {
        (self.start, self.end)
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        self.inner.build_window(start, end)
    }
}

impl<T: InternalNodeFilterBuilder> InternalNodeFilterBuilder for Windowed<T> {
    type FilterType = T::FilterType;

    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalNodeIdFilterBuilder> InternalNodeIdFilterBuilder for Windowed<T> {
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalPropertyFilterBuilder> InternalPropertyFilterBuilder for Windowed<T> {
    type Filter = Windowed<T::Filter>;
    type ExprBuilder = Windowed<T::ExprBuilder>;
    type Marker = T::Marker;

    fn property_ref(&self) -> PropertyRef {
        self.inner.property_ref()
    }

    fn ops(&self) -> &[Op] {
        self.inner.ops()
    }

    fn entity(&self) -> Self::Marker {
        self.inner.entity()
    }

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        self.wrap(self.inner.filter(filter))
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        self.wrap(self.inner.with_expr_builder(builder))
    }
}

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for Windowed<T> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        let filter = self.inner.try_as_composite_node_filter()?;
        let filter = CompositeNodeFilter::Windowed(Box::new(self.wrap(filter)));
        Ok(filter)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        let filter = self.inner.try_as_composite_edge_filter()?;
        let filter = CompositeEdgeFilter::Windowed(Box::new(self.wrap(filter)));
        Ok(filter)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        let filter = self.inner.try_as_composite_exploded_edge_filter()?;
        let filter = CompositeExplodedEdgeFilter::Windowed(Box::new(self.wrap(filter)));
        Ok(filter)
    }
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for Windowed<T> {
    type EntityFiltered<'graph, G>
        = T::EntityFiltered<'graph, WindowedGraph<G>>
    where
        G: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>
        = T::NodeFilter<'graph, WindowedGraph<G>>
    where
        G: GraphView + 'graph;

    fn create_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>
    where
        G: GraphViewOps<'graph>,
    {
        self.inner
            .create_filter(graph.window(self.start.t(), self.end.t()))
    }

    fn create_node_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>
    where
        G: GraphView + 'graph,
    {
        self.inner
            .create_node_filter(graph.window(self.start.t(), self.end.t()))
    }
}

impl<T: ComposableFilter> ComposableFilter for Windowed<T> {}

impl<M> Wrap for Windowed<M> {
    type Wrapped<T> = Windowed<T>;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        Windowed::new(self.start, self.end, value)
    }
}

impl<T: InternalPropertyFilterFactory> InternalPropertyFilterFactory for Windowed<T> {
    type Entity = T::Entity;
    type PropertyBuilder = Windowed<T::PropertyBuilder>;
    type MetadataBuilder = Windowed<T::MetadataBuilder>;

    fn entity(&self) -> Self::Entity {
        self.inner.entity()
    }

    fn property_builder(&self, property: String) -> Self::PropertyBuilder {
        self.wrap(self.inner.property_builder(property))
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        self.wrap(self.inner.metadata_builder(property))
    }
}

impl<T: TemporalPropertyFilterFactory> TemporalPropertyFilterFactory for Windowed<T> {}
