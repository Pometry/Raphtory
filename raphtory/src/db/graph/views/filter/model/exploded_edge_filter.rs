use crate::{
    db::{
        api::{
            state::ops::NotANodeFilter,
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            exploded_edge_node_filtered_graph::ExplodedEdgeNodeFilteredGraph,
            model::{
                edge_filter::{CompositeEdgeFilter, Endpoint},
                is_active_edge_filter::IsActiveEdge,
                is_deleted_filter::IsDeletedEdge,
                is_self_loop_filter::IsSelfLoopEdge,
                is_valid_filter::IsValidEdge,
                latest_filter::Latest,
                layered_filter::Layered,
                node_filter::{
                    builders::{InternalNodeFilterBuilder, InternalNodeIdFilterBuilder},
                    CompositeNodeFilter, NodeFilter,
                },
                property_filter::{
                    builders::{
                        MetadataFilterBuilder, PropertyExprBuilderInput, PropertyFilterBuilder,
                    },
                    Op, PropertyFilter, PropertyFilterInput, PropertyRef,
                },
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                AndFilter, CombinedFilter, EdgeViewFilterOps, EntityMarker,
                InternalPropertyFilterBuilder, InternalPropertyFilterFactory, InternalViewWrapOps,
                NotFilter, OrFilter, TemporalPropertyFilterFactory, TryAsCompositeFilter, Wrap,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::storage::timeindex::EventTime;
use std::{fmt, fmt::Display, sync::Arc};

#[derive(Clone, Debug, Copy, Default, PartialEq, Eq)]
pub struct ExplodedEdgeFilter;

impl From<ExplodedEdgeFilter> for EntityMarker {
    fn from(_value: ExplodedEdgeFilter) -> Self {
        EntityMarker::ExplodedEdge
    }
}

impl ExplodedEdgeFilter {
    #[inline]
    pub fn src() -> ExplodedEdgeEndpointWrapper<NodeFilter> {
        ExplodedEdgeEndpointWrapper::new(NodeFilter, Endpoint::Src)
    }

    #[inline]
    pub fn dst() -> ExplodedEdgeEndpointWrapper<NodeFilter> {
        ExplodedEdgeEndpointWrapper::new(NodeFilter, Endpoint::Dst)
    }
}

impl Wrap for ExplodedEdgeFilter {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalViewWrapOps for ExplodedEdgeFilter {
    type Window = Windowed<ExplodedEdgeFilter>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

impl InternalPropertyFilterFactory for ExplodedEdgeFilter {
    type Entity = ExplodedEdgeFilter;
    type PropertyBuilder = PropertyFilterBuilder<Self::Entity>;
    type MetadataBuilder = MetadataFilterBuilder<Self::Entity>;

    fn entity(&self) -> Self::Entity {
        ExplodedEdgeFilter
    }

    fn property_builder(&self, property: String) -> Self::PropertyBuilder {
        PropertyFilterBuilder(property, self.entity())
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        MetadataFilterBuilder(property, self.entity())
    }
}

impl EdgeViewFilterOps for ExplodedEdgeFilter {
    type Output<T: CombinedFilter> = T;

    fn is_active(&self) -> Self::Output<IsActiveEdge> {
        IsActiveEdge
    }

    fn is_valid(&self) -> Self::Output<IsValidEdge> {
        IsValidEdge
    }

    fn is_deleted(&self) -> Self::Output<IsDeletedEdge> {
        IsDeletedEdge
    }

    fn is_self_loop(&self) -> Self::Output<IsSelfLoopEdge> {
        IsSelfLoopEdge
    }
}

#[derive(Debug, Clone)]
pub struct ExplodedEdgeEndpointWrapper<T> {
    pub(crate) inner: T,
    endpoint: Endpoint,
}

impl<T: Display> Display for ExplodedEdgeEndpointWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> ExplodedEdgeEndpointWrapper<T> {
    #[inline]
    pub fn new(inner: T, endpoint: Endpoint) -> Self {
        Self { inner, endpoint }
    }

    #[inline]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> ExplodedEdgeEndpointWrapper<U> {
        ExplodedEdgeEndpointWrapper {
            inner: f(self.inner),
            endpoint: self.endpoint,
        }
    }
}

impl<M> Wrap for ExplodedEdgeEndpointWrapper<M> {
    type Wrapped<T> = ExplodedEdgeEndpointWrapper<T>;

    fn wrap<T>(&self, inner: T) -> Self::Wrapped<T> {
        ExplodedEdgeEndpointWrapper {
            inner,
            endpoint: self.endpoint,
        }
    }
}

impl<T: InternalNodeIdFilterBuilder> InternalNodeIdFilterBuilder
    for ExplodedEdgeEndpointWrapper<T>
{
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalNodeFilterBuilder> InternalNodeFilterBuilder for ExplodedEdgeEndpointWrapper<T> {
    type FilterType = T::FilterType;

    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalPropertyFilterBuilder> InternalPropertyFilterBuilder
    for ExplodedEdgeEndpointWrapper<T>
{
    type Filter = ExplodedEdgeEndpointWrapper<T::Filter>;
    type ExprBuilder = ExplodedEdgeEndpointWrapper<T::ExprBuilder>;
    type Marker = T::Marker;

    #[inline]
    fn property_ref(&self) -> PropertyRef {
        self.inner.property_ref()
    }

    #[inline]
    fn ops(&self) -> &[Op] {
        self.inner.ops()
    }

    #[inline]
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

impl<T: InternalPropertyFilterFactory> InternalPropertyFilterFactory
    for ExplodedEdgeEndpointWrapper<T>
{
    type Entity = T::Entity;
    type PropertyBuilder = ExplodedEdgeEndpointWrapper<T::PropertyBuilder>;
    type MetadataBuilder = ExplodedEdgeEndpointWrapper<T::MetadataBuilder>;

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

impl<T: TemporalPropertyFilterFactory> TemporalPropertyFilterFactory
    for ExplodedEdgeEndpointWrapper<T>
{
}

impl<T: CreateFilter + Clone + 'static> CreateFilter for ExplodedEdgeEndpointWrapper<T> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = ExplodedEdgeNodeFilteredGraph<G, T::NodeFilter<'graph, G>>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>
        = NotANodeFilter
    where
        Self: 'graph,
        G: GraphView + 'graph;
    type FilteredGraph<'graph, G>
        = T::FilteredGraph<'graph, G>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>
    where
        T: 'graph,
    {
        let filter = self.inner.create_node_filter(graph.clone())?;
        Ok(ExplodedEdgeNodeFilteredGraph::new(
            graph,
            self.endpoint,
            filter,
        ))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        self.inner.filter_graph_view(graph)
    }
}

impl<T> TryAsCompositeFilter for ExplodedEdgeEndpointWrapper<T>
where
    T: TryAsCompositeFilter + Clone,
{
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        let nf = self.inner.try_as_composite_node_filter()?;
        Ok(match self.endpoint {
            Endpoint::Src => CompositeExplodedEdgeFilter::Src(nf),
            Endpoint::Dst => CompositeExplodedEdgeFilter::Dst(nf),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeExplodedEdgeFilter {
    Src(CompositeNodeFilter),
    Dst(CompositeNodeFilter),
    Property(PropertyFilter<ExplodedEdgeFilter>),
    Windowed(Box<Windowed<CompositeExplodedEdgeFilter>>),
    Latest(Box<Latest<CompositeExplodedEdgeFilter>>),
    SnapshotAt(Box<SnapshotAt<CompositeExplodedEdgeFilter>>),
    SnapshotLatest(Box<SnapshotLatest<CompositeExplodedEdgeFilter>>),
    Layered(Box<Layered<CompositeExplodedEdgeFilter>>),
    IsActiveEdge(IsActiveEdge),
    IsValidEdge(IsValidEdge),
    IsDeletedEdge(IsDeletedEdge),
    IsSelfLoopEdge(IsSelfLoopEdge),
    And(
        Box<CompositeExplodedEdgeFilter>,
        Box<CompositeExplodedEdgeFilter>,
    ),
    Or(
        Box<CompositeExplodedEdgeFilter>,
        Box<CompositeExplodedEdgeFilter>,
    ),
    Not(Box<CompositeExplodedEdgeFilter>),
}

impl Display for CompositeExplodedEdgeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeExplodedEdgeFilter::Src(filter) => write!(f, "SRC({})", filter),
            CompositeExplodedEdgeFilter::Dst(filter) => write!(f, "DST({})", filter),
            CompositeExplodedEdgeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::Windowed(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::Latest(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::SnapshotAt(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::SnapshotLatest(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::IsActiveEdge(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::IsValidEdge(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::IsDeletedEdge(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::IsSelfLoopEdge(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::Layered(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeExplodedEdgeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeExplodedEdgeFilter::Not(filter) => write!(f, "(NOT {})", filter),
        }
    }
}

impl CreateFilter for CompositeExplodedEdgeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;
    type NodeFilter<'graph, G>
        = NotANodeFilter
    where
        Self: 'graph,
        G: GraphView + 'graph;
    type FilteredGraph<'graph, G>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        match self {
            Self::Src(filter) => {
                let wrapped = ExplodedEdgeEndpointWrapper::new(filter, Endpoint::Src);
                let filtered_graph = wrapped.create_filter(graph)?;
                Ok(Arc::new(filtered_graph))
            }
            Self::Dst(filter) => {
                let wrapped = ExplodedEdgeEndpointWrapper::new(filter, Endpoint::Dst);
                let filtered_graph = wrapped.create_filter(graph)?;
                Ok(Arc::new(filtered_graph))
            }
            Self::Property(p) => Ok(Arc::new(p.create_filter(graph)?)),
            Self::Windowed(pw) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                pw.create_filter(dyn_graph)
            }
            Self::Latest(pw) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                pw.create_filter(dyn_graph)
            }
            Self::SnapshotAt(pw) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                pw.create_filter(dyn_graph)
            }
            Self::SnapshotLatest(pw) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                pw.create_filter(dyn_graph)
            }
            Self::Layered(pw) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                pw.create_filter(dyn_graph)
            }
            Self::IsActiveEdge(pw) => Ok(Arc::new(pw.create_filter(graph)?)),
            Self::IsValidEdge(pw) => Ok(Arc::new(pw.create_filter(graph)?)),
            Self::IsDeletedEdge(pw) => Ok(Arc::new(pw.create_filter(graph)?)),
            Self::IsSelfLoopEdge(pw) => Ok(Arc::new(pw.create_filter(graph)?)),
            Self::And(l, r) => {
                let (l, r) = (*l, *r); // move out, no clone
                Ok(Arc::new(
                    AndFilter { left: l, right: r }.create_filter(graph)?,
                ))
            }
            Self::Or(l, r) => {
                let (l, r) = (*l, *r);
                Ok(Arc::new(
                    OrFilter { left: l, right: r }.create_filter(graph)?,
                ))
            }
            Self::Not(f) => {
                let base = *f;
                Ok(Arc::new(NotFilter(base).create_filter(graph)?))
            }
        }
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        match self.clone() {
            Self::Src(filter) => {
                let wrapped = ExplodedEdgeEndpointWrapper::new(filter, Endpoint::Src);
                let filtered_graph = wrapped.filter_graph_view(graph)?;
                Ok(Arc::new(filtered_graph))
            }
            Self::Dst(filter) => {
                let wrapped = ExplodedEdgeEndpointWrapper::new(filter, Endpoint::Dst);
                let filtered_graph = wrapped.filter_graph_view(graph)?;
                Ok(Arc::new(filtered_graph))
            }
            Self::Property(p) => Ok(Arc::new(p.filter_graph_view(graph)?)),
            Self::Windowed(pw) => Ok(Arc::new(pw.filter_graph_view(graph)?)),
            Self::Latest(pw) => Ok(Arc::new(pw.filter_graph_view(graph)?)),
            Self::SnapshotAt(pw) => Ok(Arc::new(pw.filter_graph_view(graph)?)),
            Self::SnapshotLatest(pw) => Ok(Arc::new(pw.filter_graph_view(graph)?)),
            Self::Layered(pw) => Ok(Arc::new(pw.filter_graph_view(graph)?)),
            Self::IsActiveEdge(pw) => Ok(Arc::new(pw.filter_graph_view(graph)?)),
            Self::IsValidEdge(pw) => Ok(Arc::new(pw.filter_graph_view(graph)?)),
            Self::IsDeletedEdge(pw) => Ok(Arc::new(pw.filter_graph_view(graph)?)),
            Self::IsSelfLoopEdge(pw) => Ok(Arc::new(pw.filter_graph_view(graph)?)),
            Self::And(l, r) => {
                let (l, r) = (*l, *r); // move out, no clone
                Ok(Arc::new(
                    AndFilter { left: l, right: r }.filter_graph_view(graph)?,
                ))
            }
            Self::Or(l, r) => {
                let (l, r) = (*l, *r);
                Ok(Arc::new(
                    OrFilter { left: l, right: r }.filter_graph_view(graph)?,
                ))
            }
            Self::Not(f) => {
                let base = *f;
                Ok(Arc::new(NotFilter(base).filter_graph_view(graph)?))
            }
        }
    }
}

impl TryAsCompositeFilter for CompositeExplodedEdgeFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(self.clone())
    }
}

#[cfg(test)]
mod filter_tree_export_tests {
    use super::*;
    use crate::db::graph::views::filter::model::{
        node_filter::NodeFilter, property_filter::ops::PropertyFilterOps, ComposableFilter,
        FilterTree, PropertyFilterFactory, ViewWrapOps,
    };

    // An exploded-edge property filter exports as the exploded-edge kind — the
    // transportable form the remote client ships.
    #[test]
    fn exploded_property_exports_as_exploded_edge_tree() {
        let f = ExplodedEdgeFilter.property("w").gt(1i64);
        let tree = f.try_as_filter_tree().unwrap();
        assert!(
            matches!(
                tree,
                FilterTree::ExplodedEdge(CompositeExplodedEdgeFilter::Property(_))
            ),
            "expected ExplodedEdge(Property), got {tree:?}"
        );
    }

    #[test]
    fn exploded_metadata_exports_as_exploded_edge_tree() {
        let f = ExplodedEdgeFilter.metadata("kind").eq("strong");
        let tree = f.try_as_filter_tree().unwrap();
        assert!(
            matches!(
                tree,
                FilterTree::ExplodedEdge(CompositeExplodedEdgeFilter::Property(_))
            ),
            "expected ExplodedEdge(Property), got {tree:?}"
        );
    }

    // A combinator of two exploded filters keeps its composite form — no
    // structural And wrapper.
    #[test]
    fn same_kind_combinators_export_as_a_composite() {
        let a = ExplodedEdgeFilter.property("w").gt(1i64);
        let b = ExplodedEdgeFilter.property("w").lt(9i64);
        let tree = a.clone().and(b.clone()).try_as_filter_tree().unwrap();
        assert!(
            matches!(
                tree,
                FilterTree::ExplodedEdge(CompositeExplodedEdgeFilter::And(_, _))
            ),
            "expected ExplodedEdge(And), got {tree:?}"
        );

        let tree = a.clone().or(b).try_as_filter_tree().unwrap();
        assert!(
            matches!(
                tree,
                FilterTree::ExplodedEdge(CompositeExplodedEdgeFilter::Or(_, _))
            ),
            "expected ExplodedEdge(Or), got {tree:?}"
        );

        let tree = a.not().try_as_filter_tree().unwrap();
        assert!(
            matches!(
                tree,
                FilterTree::ExplodedEdge(CompositeExplodedEdgeFilter::Not(_))
            ),
            "expected ExplodedEdge(Not), got {tree:?}"
        );
    }

    // A view wrapper over an exploded property keeps the composite form as
    // well (the wrapper becomes a Windowed composite variant).
    #[test]
    fn windowed_exploded_property_exports_as_a_composite() {
        let f = ExplodedEdgeFilter.window(2i64, 4i64).property("w").gt(1i64);
        let tree = f.try_as_filter_tree().unwrap();
        assert!(
            matches!(
                tree,
                FilterTree::ExplodedEdge(CompositeExplodedEdgeFilter::Windowed(_))
            ),
            "expected ExplodedEdge(Windowed), got {tree:?}"
        );
    }

    // The exploded predicates also export as plain edge filters; the
    // node → edge → exploded order must keep that export unchanged.
    #[test]
    fn exploded_predicates_still_export_as_plain_edge_filters() {
        let tree = ExplodedEdgeFilter.is_valid().try_as_filter_tree().unwrap();
        assert!(
            matches!(tree, FilterTree::Edge(_)),
            "expected Edge for is_valid, got {tree:?}"
        );
    }

    // A mixed node∧exploded combination exports structurally, with the
    // exploded leg tagged as its own kind.
    #[test]
    fn mixed_node_and_exploded_exports_structurally() {
        let n = NodeFilter.property("x").eq(1i64);
        let x = ExplodedEdgeFilter.property("w").gt(1i64);
        let tree = n.and(x).try_as_filter_tree().unwrap();
        let FilterTree::And(ref items) = tree else {
            panic!("expected structural And, got {tree:?}");
        };
        assert!(matches!(items[0], FilterTree::Node(_)));
        assert!(matches!(items[1], FilterTree::ExplodedEdge(_)));
    }
}
