use crate::{
    api::core::Direction,
    db::{
        api::{
            state::{
                ops::{
                    filter::{
                        AndOp, MaskOp, NodeIdFilterOp, NodeNameFilterOp, NodeTypeFilterOp, NotOp,
                        OrOp,
                    },
                    node::{Id, Name, Type},
                    NodeOp, TypeId,
                },
                NodeStateValue, TypedNodeState,
            },
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            model::{
                degree_filter::{DegreeFilter, DegreeFilterBuilder, DegreeFilterFactory},
                edge_filter::CompositeEdgeFilter,
                filter::Filter,
                is_active_node_filter::IsActiveNode,
                latest_filter::Latest,
                layered_filter::Layered,
                node_expr::{exprs::DegreeExpr, EntityExpr},
                node_filter::{
                    builders::{NodeIdFilterBuilder, NodeNameFilterBuilder, NodeTypeFilterBuilder},
                    validate::validate,
                },
                node_state_filter::NodeStateBoolColOp,
                property_filter::builders::{MetadataFilterBuilder, PropertyFilterBuilder},
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                AndFilter, CombinedFilter, ComposableFilter, CompositeExplodedEdgeFilter,
                CreateView, EntityMarker, InternalPropertyFilterFactory, InternalViewWrapOps,
                NodeViewFilterOps, NotFilter, OrFilter, Wrap,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::PropertyFilter,
};
use raphtory_api::core::storage::timeindex::EventTime;
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
    pub fn id() -> NodeIdFilterBuilder {
        NodeIdFilterBuilder
    }

    #[inline]
    pub fn name() -> NodeNameFilterBuilder {
        NodeNameFilterBuilder
    }

    #[inline]
    pub fn node_type() -> NodeTypeFilterBuilder {
        NodeTypeFilterBuilder
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
        PropertyFilterBuilder(property, InternalPropertyFilterFactory::entity(self))
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        MetadataFilterBuilder(property, InternalPropertyFilterFactory::entity(self))
    }
}

impl DegreeFilterFactory for NodeFilter {
    fn degree(&self) -> DegreeFilterBuilder {
        DegreeFilterBuilder::new(Direction::BOTH)
    }

    fn in_degree(&self) -> DegreeFilterBuilder {
        DegreeFilterBuilder::new(Direction::IN)
    }

    fn out_degree(&self) -> DegreeFilterBuilder {
        DegreeFilterBuilder::new(Direction::OUT)
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
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        NodeFilteredGraph<G, NodeIdFilterOp>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NodeIdFilterOp;

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
        validate(graph.id_type(), &self.0)?;
        Ok(NodeFilteredGraph::new(graph, NodeIdFilterOp::new(self.0)))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
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
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        NodeFilteredGraph<G, NodeNameFilterOp>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NodeNameFilterOp;

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
        Ok(NodeFilteredGraph::new(graph, NodeNameFilterOp::new(self.0)))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Ok(NodeNameFilterOp::new(self.0))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
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
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        NodeFilteredGraph<G, NodeTypeFilterOp>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NodeTypeFilterOp;

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

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeNodeFilter {
    Node(Filter),
    Property(PropertyFilter<NodeFilter>),
    Degree(DegreeFilter),
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
            CompositeNodeFilter::Degree(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Layered(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Latest(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::SnapshotAt(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::SnapshotLatest(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::IsActiveNode(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Node(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeNodeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeNodeFilter::Not(filter) => write!(f, "NOT({})", filter),
        }
    }
}

// ── expr-layer factory ──

pub trait NodeFilterFactory:
    InternalViewWrapOps<Window = Self::NodeWindow> + CreateView + EntityExpr
{
    type NodeWindow: NodeFilterFactory + NodeViewFilterOps;
    #[inline]
    fn id(&self) -> Id {
        Id
    }

    /// Selects the node name field for filtering.
    ///
    /// Returns `Name` which implements `NodeExprFilterOps` — use `.eq("Alice")`,
    /// `.contains("ali")`, `.is_in([…])`, etc. directly on the returned value.
    #[inline]
    fn name(&self) -> Name {
        Name
    }

    /// Selects the node type field for filtering.
    ///
    /// Returns `Type` which implements `NodeExprFilterOps`.
    #[inline]
    fn node_type(&self) -> Type {
        Type
    }

    /// Build a filter from a boolean column inside a TypedNodeState.
    fn by_column<'graph, V, G, T>(
        state: &TypedNodeState<'graph, V, G, T>,
        col: &str,
    ) -> Result<NodeStateBoolColOp, GraphError>
    where
        V: NodeStateValue + 'graph,
        T: Clone + Send + Sync + 'graph,
        Self: Sized,
    {
        state.bool_col_filter(col)
    }

    /// Total degree expression — supports `.gt(n)`, `.lt(n)`, etc.
    fn degree(&self) -> DegreeExpr<Self> {
        DegreeExpr {
            dir: Direction::BOTH,
            view_expr: self.clone(),
        }
    }

    /// In-degree expression.
    fn in_degree(&self) -> DegreeExpr<Self> {
        DegreeExpr {
            dir: Direction::IN,
            view_expr: self.clone(),
        }
    }

    /// Out-degree expression.
    #[inline]
    fn out_degree(&self) -> DegreeExpr<Self> {
        DegreeExpr {
            dir: Direction::OUT,
            view_expr: self.clone(),
        }
    }
}

impl NodeFilterFactory for NodeFilter {
    type NodeWindow = Self::Window;
}

impl<T: NodeFilterFactory + NodeViewFilterOps> NodeFilterFactory for Windowed<T> {
    type NodeWindow = T::NodeWindow;
}

impl<T: NodeFilterFactory + NodeViewFilterOps> NodeFilterFactory for Latest<T> {
    type NodeWindow = Self::Window;
}

impl<T: NodeFilterFactory + NodeViewFilterOps> NodeFilterFactory for SnapshotAt<T> {
    type NodeWindow = Self::Window;
}

impl<T: NodeFilterFactory + NodeViewFilterOps> NodeFilterFactory for SnapshotLatest<T> {
    type NodeWindow = Self::Window;
}

impl<T: NodeFilterFactory + NodeViewFilterOps> NodeFilterFactory for Layered<T> {
    type NodeWindow = Self::Window;
}
