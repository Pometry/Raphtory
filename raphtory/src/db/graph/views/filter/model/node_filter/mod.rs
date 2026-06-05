use crate::{
    db::{
        api::{
            state::{
                ops::{
                    filter::{
                        AndOp, MaskOp, NodeIdFilterOp, NodeNameFilterOp, NodeTypeFilterOp, NotOp,
                        OrOp,
                    },
                    Id, Name, NodeOp, Type, TypeId,
                },
                NodeStateValue, TypedNodeState,
            },
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter,
                filter::Filter,
                is_active_node_filter::IsActiveNode,
                latest_filter::Latest,
                layered_filter::Layered,
                node_expr::{DegreeExpr, Metadata, Property},
                node_filter::validate::validate,
                node_state_filter::NodeStateBoolColOp,
                property_filter::builders::{MetadataFilterBuilder, PropertyFilterBuilder},
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                AndFilter, CombinedFilter, ComposableFilter, CompositeExplodedEdgeFilter,
                EntityMarker, InternalPropertyFilterFactory, InternalViewWrapOps,
                NodeViewFilterOps, NotFilter, OrFilter, TryAsCompositeFilter, Wrap,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, PropertyFilter},
};
use raphtory_api::core::{entities::GID, storage::timeindex::EventTime, Direction};
use std::{fmt, fmt::Display, sync::Arc};

pub mod builders;
pub mod ops;
mod validate;

#[derive(Clone, Debug, Default, Copy, PartialEq, Eq)]
pub struct NodeFilter;

impl From<NodeFilter> for EntityMarker {
    fn from(_value: NodeFilter) -> Self {
        EntityMarker::Node
    }
}

impl NodeFilter {
    #[inline]
    pub fn id() -> Id {
        Id
    }

    /// Selects the node name field for filtering.
    ///
    /// Returns `Name` which implements `NodeExprFilterOps` — use `.eq("Alice")`,
    /// `.contains("ali")`, `.is_in([…])`, etc. directly on the returned value.
    #[inline]
    pub fn name() -> Name {
        Name
    }

    /// Selects the node type field for filtering.
    ///
    /// Returns `Type` which implements `NodeExprFilterOps`.
    #[inline]
    pub fn node_type() -> Type {
        Type
    }

    /// Build a filter from a boolean column inside a TypedNodeState.
    pub fn by_column<'graph, V, G, T>(
        state: &TypedNodeState<'graph, V, G, T>,
        col: &str,
    ) -> Result<NodeStateBoolColOp, GraphError>
    where
        V: NodeStateValue + 'graph,
        T: Clone + Send + Sync + 'graph,
    {
        state.bool_col_filter(col)
    }

    /// Total degree expression — supports `.gt(n)`, `.lt(n)`, etc.
    #[inline]
    pub fn degree() -> DegreeExpr {
        DegreeExpr(Direction::BOTH)
    }

    /// In-degree expression.
    #[inline]
    pub fn in_degree() -> DegreeExpr {
        DegreeExpr(Direction::IN)
    }

    /// Out-degree expression.
    #[inline]
    pub fn out_degree() -> DegreeExpr {
        DegreeExpr(Direction::OUT)
    }

    /// Current (latest) value of a named property — serializable.
    #[inline]
    pub fn property(name: impl Into<String>) -> Property {
        Property::new(name)
    }

    /// Static metadata field — serializable.
    #[inline]
    pub fn metadata(name: impl Into<String>) -> Metadata {
        Metadata::new(name)
    }
}

impl Wrap for NodeFilter {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalViewWrapOps for NodeFilter {
    type Window = Windowed<NodeFilter>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

impl InternalPropertyFilterFactory for NodeFilter {
    type Entity = NodeFilter;
    type PropertyBuilder = PropertyFilterBuilder<Self::Entity>;
    type MetadataBuilder = MetadataFilterBuilder<Self::Entity>;

    fn entity(&self) -> Self::Entity {
        NodeFilter
    }

    fn property_builder(&self, property: String) -> Self::PropertyBuilder {
        PropertyFilterBuilder(property, self.entity())
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        MetadataFilterBuilder(property, self.entity())
    }
}

impl NodeViewFilterOps for NodeFilter {
    type Output<T: CombinedFilter> = T;

    fn is_active(&self) -> Self::Output<IsActiveNode> {
        IsActiveNode
    }
}

#[derive(Debug, Clone)]
pub struct NodeIdFilter(pub Filter);

impl Display for NodeIdFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeIdFilter {
    fn from(filter: Filter) -> Self {
        NodeIdFilter(filter)
    }
}

impl ComposableFilter for NodeIdFilter {}

impl CreateFilter for NodeIdFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = NodeFilteredGraph<G, NodeIdFilterOp>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NodeIdFilterOp;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        validate(graph.id_type(), &self.0)?;
        Ok(NodeFilteredGraph::new(graph, NodeIdFilterOp::new(self.0)))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        validate(graph.id_type(), &self.0)?;
        Ok(NodeIdFilterOp::new(self.0))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl TryAsCompositeFilter for NodeIdFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Id(self.0.clone()))
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

impl Id {
    pub fn eq(self, value: impl Into<GID>) -> NodeIdFilter {
        NodeIdFilter(Filter::eq_id("node_id", value))
    }

    pub fn ne(self, value: impl Into<GID>) -> NodeIdFilter {
        NodeIdFilter(Filter::ne_id("node_id", value))
    }

    pub fn lt(self, value: impl Into<GID>) -> NodeIdFilter {
        NodeIdFilter(Filter::lt("node_id", value))
    }

    pub fn le(self, value: impl Into<GID>) -> NodeIdFilter {
        NodeIdFilter(Filter::le("node_id", value))
    }

    pub fn gt(self, value: impl Into<GID>) -> NodeIdFilter {
        NodeIdFilter(Filter::gt("node_id", value))
    }

    pub fn ge(self, value: impl Into<GID>) -> NodeIdFilter {
        NodeIdFilter(Filter::ge("node_id", value))
    }

    pub fn starts_with(self, s: impl Into<String>) -> NodeIdFilter {
        NodeIdFilter(Filter::starts_with("node_id", s))
    }

    pub fn ends_with(self, s: impl Into<String>) -> NodeIdFilter {
        NodeIdFilter(Filter::ends_with("node_id", s))
    }

    pub fn contains(self, s: impl Into<String>) -> NodeIdFilter {
        NodeIdFilter(Filter::contains("node_id", s))
    }

    pub fn not_contains(self, s: impl Into<String>) -> NodeIdFilter {
        NodeIdFilter(Filter::not_contains("node_id", s))
    }

    pub fn fuzzy_search(
        self,
        s: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> NodeIdFilter {
        NodeIdFilter(Filter::fuzzy_search(
            "node_id",
            s,
            levenshtein_distance,
            prefix_match,
        ))
    }

    pub fn is_in<I, T>(self, values: I) -> NodeIdFilter
    where
        I: IntoIterator<Item = T>,
        T: Into<GID>,
    {
        NodeIdFilter(Filter::is_in_id("node_id", values))
    }

    pub fn is_not_in<I, T>(self, values: I) -> NodeIdFilter
    where
        I: IntoIterator<Item = T>,
        T: Into<GID>,
    {
        NodeIdFilter(Filter::is_not_in_id("node_id", values))
    }
}

#[derive(Debug, Clone)]
pub struct NodeNameFilter(pub Filter);

impl Display for NodeNameFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeNameFilter {
    fn from(filter: Filter) -> Self {
        NodeNameFilter(filter)
    }
}

impl ComposableFilter for NodeNameFilter {}

impl CreateFilter for NodeNameFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = NodeFilteredGraph<G, NodeNameFilterOp>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NodeNameFilterOp;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(NodeFilteredGraph::new(graph, NodeNameFilterOp::new(self.0)))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeNameFilterOp::new(self.0))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl TryAsCompositeFilter for NodeNameFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Name(self.0.clone()))
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

#[derive(Debug, Clone)]
pub struct NodeTypeFilter(pub Filter);

impl Display for NodeTypeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeTypeFilter {
    fn from(filter: Filter) -> Self {
        NodeTypeFilter(filter)
    }
}

impl ComposableFilter for NodeTypeFilter {}

impl CreateFilter for NodeTypeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = NodeFilteredGraph<G, NodeTypeFilterOp>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NodeTypeFilterOp;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let node_types_filter = graph
            .node_meta()
            .node_type_meta()
            .keys()
            .iter()
            .map(|k| self.0.matches(Some(k))) // TODO: _default check
            .collect::<Vec<_>>();
        Ok(NodeFilteredGraph::new(
            graph,
            TypeId.mask(node_types_filter.into()),
        ))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let node_types_filter = graph
            .node_meta()
            .node_type_meta()
            .keys()
            .iter()
            .map(|k| self.0.matches(Some(k))) // TODO: _default check
            .collect::<Vec<_>>();
        Ok(TypeId.mask(node_types_filter.into()))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl TryAsCompositeFilter for NodeTypeFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Type(self.0.clone()))
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeNodeFilter {
    Id(Filter),
    Name(Filter),
    Type(Filter),
    Property(PropertyFilter<NodeFilter>),
    Windowed(Box<Windowed<CompositeNodeFilter>>),
    Latest(Box<Latest<CompositeNodeFilter>>),
    SnapshotAt(Box<SnapshotAt<CompositeNodeFilter>>),
    SnapshotLatest(Box<SnapshotLatest<CompositeNodeFilter>>),
    Layered(Box<Layered<CompositeNodeFilter>>),
    IsActiveNode(IsActiveNode),
    And(Box<CompositeNodeFilter>, Box<CompositeNodeFilter>),
    Or(Box<CompositeNodeFilter>, Box<CompositeNodeFilter>),
    Not(Box<CompositeNodeFilter>),
}

impl Display for CompositeNodeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeNodeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Windowed(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Layered(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Latest(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::SnapshotAt(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::SnapshotLatest(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::IsActiveNode(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Id(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Name(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Type(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeNodeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeNodeFilter::Not(filter) => write!(f, "NOT({})", filter),
        }
    }
}

impl CreateFilter for CompositeNodeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, Self::NodeFilter<'graph, G>>;

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
        let filter = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        match self {
            CompositeNodeFilter::Id(i) => Ok(Arc::new(NodeIdFilter(i).create_node_filter(graph)?)),
            CompositeNodeFilter::Name(i) => {
                Ok(Arc::new(NodeNameFilter(i).create_node_filter(graph)?))
            }
            CompositeNodeFilter::Type(i) => {
                Ok(Arc::new(NodeTypeFilter(i).create_node_filter(graph)?))
            }
            CompositeNodeFilter::Property(i) => Ok(Arc::new(i.create_node_filter(graph)?)),
            CompositeNodeFilter::Windowed(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_node_filter(dyn_graph)
            }
            CompositeNodeFilter::Layered(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_node_filter(dyn_graph)
            }
            CompositeNodeFilter::Latest(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_node_filter(dyn_graph)
            }
            CompositeNodeFilter::SnapshotAt(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_node_filter(dyn_graph)
            }
            CompositeNodeFilter::SnapshotLatest(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_node_filter(dyn_graph)
            }
            CompositeNodeFilter::IsActiveNode(i) => Ok(Arc::new(i.create_node_filter(graph)?)),
            CompositeNodeFilter::And(l, r) => Ok(Arc::new(AndOp {
                left: l.clone().create_node_filter(graph.clone())?,
                right: r.clone().create_node_filter(graph.clone())?,
            })),
            CompositeNodeFilter::Or(l, r) => Ok(Arc::new(OrOp {
                left: l.clone().create_node_filter(graph.clone())?,
                right: r.clone().create_node_filter(graph.clone())?,
            })),
            CompositeNodeFilter::Not(filter) => {
                Ok(Arc::new(NotOp(filter.clone().create_node_filter(graph)?)))
            }
        }
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        match self.clone() {
            CompositeNodeFilter::Id(i) => Ok(Arc::new(NodeIdFilter(i).filter_graph_view(graph)?)),
            CompositeNodeFilter::Name(i) => {
                Ok(Arc::new(NodeNameFilter(i).filter_graph_view(graph)?))
            }
            CompositeNodeFilter::Type(i) => {
                Ok(Arc::new(NodeTypeFilter(i).filter_graph_view(graph)?))
            }
            CompositeNodeFilter::Property(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeNodeFilter::Windowed(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeNodeFilter::Layered(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeNodeFilter::Latest(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeNodeFilter::SnapshotAt(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeNodeFilter::SnapshotLatest(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeNodeFilter::IsActiveNode(i) => Ok(Arc::new(i.filter_graph_view(graph)?)),
            CompositeNodeFilter::And(l, r) => {
                let (l, r) = (*l, *r);
                Ok(Arc::new(
                    AndFilter { left: l, right: r }.filter_graph_view(graph)?,
                ))
            }
            CompositeNodeFilter::Or(l, r) => {
                let (l, r) = (*l, *r);
                Ok(Arc::new(
                    OrFilter { left: l, right: r }.filter_graph_view(graph)?,
                ))
            }
            CompositeNodeFilter::Not(f) => {
                let base = *f;
                Ok(Arc::new(NotFilter(base).filter_graph_view(graph)?))
            }
        }
    }
}

impl TryAsCompositeFilter for CompositeNodeFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(self.clone())
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
