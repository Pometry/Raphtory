use crate::{
    db::{
        api::{
            state::NodeOp,
            view::{internal::GraphView, time::TimeOps},
        },
        graph::views::{
            filter::{
                model::{
                    edge_expr::EdgeOp,
                    edge_filter::CompositeEdgeFilter,
                    is_active_edge_filter::IsActiveEdge,
                    is_active_node_filter::IsActiveNode,
                    is_deleted_filter::IsDeletedEdge,
                    is_self_loop_filter::IsSelfLoopEdge,
                    is_valid_filter::IsValidEdge,
                    node_expr::CreateOp,
                    property_filter::{builders::PropertyExprBuilderInput, PropertyFilterInput},
                    windowed_filter::Windowed,
                    CombinedFilter, ComposableFilter, CompositeExplodedEdgeFilter,
                    CompositeNodeFilter, CreateView, EdgeViewFilterOps, FilterTree, GraphViewOp,
                    InternalPropertyFilterBuilder, InternalPropertyFilterFactory,
                    InternalViewWrapOps, NodeViewFilterOps, Op, PropertyRef,
                    TemporalPropertyFilterFactory, TryAsCompositeFilter, Wrap,
                },
                CreateFilter,
            },
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::properties::prop::Prop, storage::timeindex::EventTime, utils::time::IntoTime,
};
use std::{fmt, fmt::Display, sync::Arc};

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

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        self.wrap(self.inner.filter(filter))
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        self.wrap(self.inner.with_expr_builder(builder))
    }
}

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for SnapshotAt<T> {
    fn try_as_filter_tree(&self) -> Result<FilterTree, GraphError> {
        // Single-kind inners keep their composite form (the wrapper becomes a
        // windowed/layered/... composite variant); only graph-level view
        // chains export as `View` ops. Anything else (a view wrapping a
        // mixed-kind tree) has no wire representation yet.
        if let Ok(f) = self.try_as_composite_node_filter() {
            return Ok(FilterTree::Node(f));
        }
        if let Ok(f) = self.try_as_composite_edge_filter() {
            return Ok(FilterTree::Edge(f));
        }
        if let Ok(f) = self.try_as_composite_exploded_edge_filter() {
            return Ok(FilterTree::ExplodedEdge(f));
        }
        let FilterTree::View(ops) = self.inner.try_as_filter_tree()? else {
            return Err(GraphError::NotSupported);
        };
        let mut chain = vec![GraphViewOp::SnapshotAt(self.time)];
        chain.extend(ops);
        Ok(FilterTree::View(chain))
    }

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
    type EntityFiltered<'graph, G, F>
        = T::EntityFiltered<'graph, G, F>
    where
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type NodeFilter<'graph, G, F>
        = T::NodeFilter<'graph, G, F>
    where
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type FilteredGraph<'graph, G>
        = WindowedGraph<T::FilteredGraph<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G, F>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError>
    where
        G: GraphView + 'graph,
        F: GraphView + 'graph,
    {
        self.inner.create_filter(graph, filtered)
    }

    fn create_node_filter<'graph, G, F>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError>
    where
        G: GraphView + 'graph,
        F: GraphView + 'graph,
    {
        self.inner.create_node_filter(graph, filtered)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(self.inner.filter_graph_view(graph)?.snapshot_at(self.time))
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

    fn property_builder(&self, property: String) -> Self::PropertyBuilder {
        self.wrap(self.inner.property_builder(property))
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        self.wrap(self.inner.metadata_builder(property))
    }
}

impl<T: TemporalPropertyFilterFactory> TemporalPropertyFilterFactory for SnapshotAt<T> {}

impl<U: NodeViewFilterOps> NodeViewFilterOps for SnapshotAt<U> {
    type Output<T: CombinedFilter> = SnapshotAt<U::Output<T>>;

    fn is_active(&self) -> Self::Output<IsActiveNode> {
        self.wrap(self.inner.is_active())
    }
}

impl<U: EdgeViewFilterOps> EdgeViewFilterOps for SnapshotAt<U> {
    type Output<T: CombinedFilter> = SnapshotAt<U::Output<T>>;

    fn is_active(&self) -> Self::Output<IsActiveEdge> {
        self.wrap(self.inner.is_active())
    }

    fn is_valid(&self) -> Self::Output<IsValidEdge> {
        self.wrap(self.inner.is_valid())
    }

    fn is_deleted(&self) -> Self::Output<IsDeletedEdge> {
        self.wrap(self.inner.is_deleted())
    }

    fn is_self_loop(&self) -> Self::Output<IsSelfLoopEdge> {
        self.wrap(self.inner.is_self_loop())
    }
}

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

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        self.wrap(self.inner.filter(filter))
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        self.wrap(self.inner.with_expr_builder(builder))
    }
}

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for SnapshotLatest<T> {
    fn try_as_filter_tree(&self) -> Result<FilterTree, GraphError> {
        // Single-kind inners keep their composite form (the wrapper becomes a
        // windowed/layered/... composite variant); only graph-level view
        // chains export as `View` ops. Anything else (a view wrapping a
        // mixed-kind tree) has no wire representation yet.
        if let Ok(f) = self.try_as_composite_node_filter() {
            return Ok(FilterTree::Node(f));
        }
        if let Ok(f) = self.try_as_composite_edge_filter() {
            return Ok(FilterTree::Edge(f));
        }
        if let Ok(f) = self.try_as_composite_exploded_edge_filter() {
            return Ok(FilterTree::ExplodedEdge(f));
        }
        let FilterTree::View(ops) = self.inner.try_as_filter_tree()? else {
            return Err(GraphError::NotSupported);
        };
        let mut chain = vec![GraphViewOp::SnapshotLatest];
        chain.extend(ops);
        Ok(FilterTree::View(chain))
    }

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
    type EntityFiltered<'graph, G, F>
        = T::EntityFiltered<'graph, G, F>
    where
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type NodeFilter<'graph, G, F>
        = T::NodeFilter<'graph, G, F>
    where
        G: GraphView + 'graph,
        F: GraphView + 'graph;
    type FilteredGraph<'graph, G>
        = WindowedGraph<T::FilteredGraph<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G, F>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError>
    where
        G: GraphView + 'graph,
        F: GraphView + 'graph,
    {
        self.inner.create_filter(graph, filtered)
    }

    fn create_node_filter<'graph, G, F>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError>
    where
        G: GraphView + 'graph,
        F: GraphView + 'graph,
    {
        self.inner.create_node_filter(graph, filtered)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(self.inner.filter_graph_view(graph)?.snapshot_latest())
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

    fn property_builder(&self, property: String) -> Self::PropertyBuilder {
        self.wrap(self.inner.property_builder(property))
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        self.wrap(self.inner.metadata_builder(property))
    }
}

impl<T: TemporalPropertyFilterFactory> TemporalPropertyFilterFactory for SnapshotLatest<T> {}

impl<U: NodeViewFilterOps> NodeViewFilterOps for SnapshotLatest<U> {
    type Output<T: CombinedFilter> = SnapshotLatest<U::Output<T>>;

    fn is_active(&self) -> Self::Output<IsActiveNode> {
        self.wrap(self.inner.is_active())
    }
}

impl<U: EdgeViewFilterOps> EdgeViewFilterOps for SnapshotLatest<U> {
    type Output<T: CombinedFilter> = SnapshotLatest<U::Output<T>>;

    fn is_active(&self) -> Self::Output<IsActiveEdge> {
        self.wrap(self.inner.is_active())
    }

    fn is_valid(&self) -> Self::Output<IsValidEdge> {
        self.wrap(self.inner.is_valid())
    }

    fn is_deleted(&self) -> Self::Output<IsDeletedEdge> {
        self.wrap(self.inner.is_deleted())
    }

    fn is_self_loop(&self) -> Self::Output<IsSelfLoopEdge> {
        self.wrap(self.inner.is_self_loop())
    }
}

// ── expr-layer view construction ──

impl<T: CreateView> CreateView for SnapshotAt<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<<T as CreateView>::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.snapshot_at(self.time))
    }
}

impl<T: CreateView> CreateView for SnapshotLatest<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<<T as CreateView>::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.snapshot_latest())
    }
}

// ── expr layer: the snapshot-at view scopes any inner expression (per-expression view) ──
// Nesting order of chained views is pinned by the Phase-3 semantics tests.

impl<T: CreateOp> CreateOp for SnapshotAt<T> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_node_op(graph.snapshot_at(self.time))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_edge_op(graph.snapshot_at(self.time))
    }
}

// ── expr layer: the snapshot-latest view scopes any inner expression (per-expression view) ──
// Nesting order of chained views is pinned by the Phase-3 semantics tests.

impl<T: CreateOp> CreateOp for SnapshotLatest<T> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_node_op(graph.snapshot_latest())
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_edge_op(graph.snapshot_latest())
    }
}
