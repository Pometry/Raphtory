use crate::{
    db::{
        api::{state::NodeOp, view::internal::GraphView},
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
                    node_filter::builders::{
                        InternalNodeFilterBuilder, InternalNodeIdFilterBuilder,
                    },
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
    prelude::TimeOps,
};
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
use std::{fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Latest<M> {
    pub inner: M,
}

impl<M> Latest<M> {
    #[inline]
    pub fn new(inner: M) -> Self {
        Self { inner }
    }
}

impl<M: Display> Display for Latest<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LATEST({})", self.inner)
    }
}

impl<T: InternalViewWrapOps> InternalViewWrapOps for Latest<T> {
    type Window = Windowed<Latest<T>>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

impl<T: InternalNodeFilterBuilder> InternalNodeFilterBuilder for Latest<T> {
    type FilterType = T::FilterType;
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalNodeIdFilterBuilder> InternalNodeIdFilterBuilder for Latest<T> {
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalPropertyFilterBuilder> InternalPropertyFilterBuilder for Latest<T> {
    type Filter = Latest<T::Filter>;
    type ExprBuilder = Latest<T::ExprBuilder>;
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

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for Latest<T> {
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
        let mut chain = vec![GraphViewOp::Latest];
        chain.extend(ops);
        Ok(FilterTree::View(chain))
    }

    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Latest(Box::new(Latest::new(
            self.inner.try_as_composite_node_filter()?,
        ))))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Latest(Box::new(Latest::new(
            self.inner.try_as_composite_edge_filter()?,
        ))))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::Latest(Box::new(Latest::new(
            self.inner.try_as_composite_exploded_edge_filter()?,
        ))))
    }
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for Latest<T> {
    type EntityFiltered<'graph, G, F>
        = T::EntityFiltered<'graph, G, F>
    where
        G: GraphView + TimeOps<'graph> + 'graph,
        F: GraphView + TimeOps<'graph> + 'graph;

    type NodeFilter<'graph, G, F>
        = T::NodeFilter<'graph, G, F>
    where
        G: GraphView + TimeOps<'graph> + 'graph,
        F: GraphView + TimeOps<'graph> + 'graph;

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
        Ok(self.inner.filter_graph_view(graph)?.latest())
    }
}

impl<T: ComposableFilter> ComposableFilter for Latest<T> {}

impl<M> Wrap for Latest<M> {
    type Wrapped<T> = Latest<T>;
    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        Latest::new(value)
    }
}

impl<T: InternalPropertyFilterFactory> InternalPropertyFilterFactory for Latest<T> {
    type Entity = T::Entity;
    type PropertyBuilder = Latest<T::PropertyBuilder>;
    type MetadataBuilder = Latest<T::MetadataBuilder>;

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

impl<T: TemporalPropertyFilterFactory> TemporalPropertyFilterFactory for Latest<T> {}

impl<U: NodeViewFilterOps> NodeViewFilterOps for Latest<U> {
    type Output<T: CombinedFilter> = Latest<U::Output<T>>;

    fn is_active(&self) -> Self::Output<IsActiveNode> {
        self.wrap(self.inner.is_active())
    }
}

impl<U: EdgeViewFilterOps> EdgeViewFilterOps for Latest<U> {
    type Output<T: CombinedFilter> = Latest<U::Output<T>>;

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

// ── expr-layer view construction (June branch) ──

impl<T: CreateView> CreateView for Latest<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<<T as CreateView>::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.latest())
    }
}

// ── expr layer: the latest view scopes any inner expression (per-expression view) ──
// Nesting order of chained views is pinned by the Phase-3 semantics tests.

impl<T: CreateOp> CreateOp for Latest<T> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_node_op(graph.latest())
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_edge_op(graph.latest())
    }
}
