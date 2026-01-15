use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::{
            filter::{
                model::{
                    edge_filter::CompositeEdgeFilter,
                    property_filter::builders::{
                        MetadataFilterBuilder, PropertyExprBuilder, PropertyFilterBuilder,
                    },
                    windowed_filter::Windowed,
                    ComposableFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter,
                    InternalPropertyFilterBuilder, InternalPropertyFilterFactory,
                    InternalViewWrapOps, Op, PropertyRef, TemporalPropertyFilterFactory,
                    TryAsCompositeFilter, ViewWrapOps, Wrap,
                },
                CreateFilter,
            },
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, PropertyFilter, TimeOps},
};
use raphtory_api::core::{storage::timeindex::EventTime, utils::time::IntoTime};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotAt<M> {
    pub time: EventTime,
    pub inner: M,
}

impl<M> SnapshotAt<M> {
    #[inline]
    pub fn new<T: IntoTime>(time: T, inner: M) -> Self {
        Self {
            time: time.into_time(),
            inner,
        }
    }
}

impl<M: Display> Display for SnapshotAt<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SNAPSHOT_AT[{}]({})", self.time, self.inner)
    }
}

impl<T: InternalViewWrapOps> InternalViewWrapOps for SnapshotAt<T> {
    type Window = Windowed<SnapshotAt<T>>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

impl<T: InternalPropertyFilterBuilder> InternalPropertyFilterBuilder for SnapshotAt<T> {
    type Filter = SnapshotAt<T::Filter>;
    type ExprBuilder = SnapshotAt<T::ExprBuilder>;
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

    fn filter(&self, filter: PropertyFilter<Self::Marker>) -> Self::Filter {
        self.wrap(self.inner.filter(filter))
    }

    fn into_expr_builder(&self, builder: PropertyExprBuilder<Self::Marker>) -> Self::ExprBuilder {
        self.wrap(self.inner.into_expr_builder(builder))
    }
}

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for SnapshotAt<T> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::SnapshotAt(Box::new(SnapshotAt {
            time: self.time,
            inner: self.inner.try_as_composite_node_filter()?,
        })))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::SnapshotAt(Box::new(SnapshotAt::new(
            self.time,
            self.inner.try_as_composite_edge_filter()?,
        ))))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::SnapshotAt(Box::new(
            SnapshotAt::new(
                self.time,
                self.inner.try_as_composite_exploded_edge_filter()?,
            ),
        )))
    }
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for SnapshotAt<T> {
    type EntityFiltered<'graph, G>
        = T::EntityFiltered<'graph, WindowedGraph<G>>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph> + Clone;

    type NodeFilter<'graph, G>
        = T::NodeFilter<'graph, WindowedGraph<G>>
    where
        G: GraphView + TimeOps<'graph> + Clone + 'graph;

    fn create_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + Clone,
    {
        self.inner.create_filter(graph.snapshot_at(self.time))
    }

    fn create_node_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>
    where
        G: GraphView + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + Clone + 'graph,
    {
        self.inner.create_node_filter(graph.snapshot_at(self.time))
    }
}

impl<T: ComposableFilter> ComposableFilter for SnapshotAt<T> {}

impl<M> Wrap for SnapshotAt<M> {
    type Wrapped<T> = SnapshotAt<T>;
    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        SnapshotAt {
            time: self.time,
            inner: value,
        }
    }
}

impl<T: InternalPropertyFilterFactory> InternalPropertyFilterFactory for SnapshotAt<T> {
    type Entity = T::Entity;
    type PropertyBuilder = SnapshotAt<T::PropertyBuilder>;
    type MetadataBuilder = SnapshotAt<T::MetadataBuilder>;

    fn entity(&self) -> Self::Entity {
        self.inner.entity()
    }

    fn property_builder(
        &self,
        builder: PropertyFilterBuilder<Self::Entity>,
    ) -> Self::PropertyBuilder {
        self.wrap(self.inner.property_builder(builder))
    }

    fn metadata_builder(
        &self,
        builder: MetadataFilterBuilder<Self::Entity>,
    ) -> Self::MetadataBuilder {
        self.wrap(self.inner.metadata_builder(builder))
    }
}

impl<T: TemporalPropertyFilterFactory> TemporalPropertyFilterFactory for SnapshotAt<T> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotLatest<M> {
    pub inner: M,
}

impl<M> SnapshotLatest<M> {
    #[inline]
    pub fn new(inner: M) -> Self {
        Self { inner }
    }
}

impl<M: Display> Display for SnapshotLatest<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SNAPSHOT_LATEST({})", self.inner)
    }
}

impl<T: InternalViewWrapOps> InternalViewWrapOps for SnapshotLatest<T> {
    type Window = Windowed<SnapshotLatest<T>>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

impl<T: InternalPropertyFilterBuilder> InternalPropertyFilterBuilder for SnapshotLatest<T> {
    type Filter = SnapshotLatest<T::Filter>;
    type ExprBuilder = SnapshotLatest<T::ExprBuilder>;
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

    fn filter(&self, filter: PropertyFilter<Self::Marker>) -> Self::Filter {
        self.wrap(self.inner.filter(filter))
    }

    fn into_expr_builder(&self, builder: PropertyExprBuilder<Self::Marker>) -> Self::ExprBuilder {
        self.wrap(self.inner.into_expr_builder(builder))
    }
}

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for SnapshotLatest<T> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::SnapshotLatest(Box::new(
            SnapshotLatest::new(self.inner.try_as_composite_node_filter()?),
        )))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::SnapshotLatest(Box::new(
            SnapshotLatest::new(self.inner.try_as_composite_edge_filter()?),
        )))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::SnapshotLatest(Box::new(
            SnapshotLatest::new(self.inner.try_as_composite_exploded_edge_filter()?),
        )))
    }
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for SnapshotLatest<T> {
    type EntityFiltered<'graph, G>
        = T::EntityFiltered<'graph, WindowedGraph<G>>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph> + Clone;

    type NodeFilter<'graph, G>
        = T::NodeFilter<'graph, WindowedGraph<G>>
    where
        G: GraphView + TimeOps<'graph> + Clone + 'graph;

    fn create_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + Clone,
    {
        self.inner.create_filter(graph.snapshot_latest())
    }

    fn create_node_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>
    where
        G: GraphView + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + Clone + 'graph,
    {
        self.inner.create_node_filter(graph.snapshot_latest())
    }
}

impl<T: ComposableFilter> ComposableFilter for SnapshotLatest<T> {}

impl<M> Wrap for SnapshotLatest<M> {
    type Wrapped<T> = SnapshotLatest<T>;
    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        SnapshotLatest::new(value)
    }
}

impl<T: InternalPropertyFilterFactory> InternalPropertyFilterFactory for SnapshotLatest<T> {
    type Entity = T::Entity;
    type PropertyBuilder = SnapshotLatest<T::PropertyBuilder>;
    type MetadataBuilder = SnapshotLatest<T::MetadataBuilder>;

    fn entity(&self) -> Self::Entity {
        self.inner.entity()
    }

    fn property_builder(
        &self,
        builder: PropertyFilterBuilder<Self::Entity>,
    ) -> Self::PropertyBuilder {
        self.wrap(self.inner.property_builder(builder))
    }

    fn metadata_builder(
        &self,
        builder: MetadataFilterBuilder<Self::Entity>,
    ) -> Self::MetadataBuilder {
        self.wrap(self.inner.metadata_builder(builder))
    }
}

impl<T: TemporalPropertyFilterFactory> TemporalPropertyFilterFactory for SnapshotLatest<T> {}
