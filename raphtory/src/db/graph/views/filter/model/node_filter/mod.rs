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
                node_expr::{DegreeExpr, Metadata, Property, TemporalProp},
                node_filter::validate::validate,
                node_state_filter::NodeStateBoolColOp,
                property_filter::builders::{MetadataFilterBuilder, PropertyFilterBuilder},
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                AndFilter, CombinedFilter, ComposableFilter, CompositeExplodedEdgeFilter,
                CreateView, EntityMarker, InternalViewWrapOps, NodeViewFilterOps, NotFilter,
                OrFilter, PropertyFilterFactory, TryAsCompositeFilter, Wrap,
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

pub trait NodeFilterFactory: PropertyFilterFactory + Clone {
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
        &self,
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

    fn is_active(&self) -> IsActiveNode<Self> {
        IsActiveNode {
            view_expr: self.clone(),
        }
    }
}

impl NodeFilterFactory for NodeFilter {}

impl TryAsCompositeFilter for NodeFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
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

impl NodeFilter {
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

    /// Full temporal history of a named property as a sequence of `Prop` values.
    ///
    /// Values are scoped to the current view window (all time if no window applied).
    /// Chain with `.any()`, `.all()`, `.sum()`, `.avg()`, `.min()`, `.max()`,
    /// `.first()`, `.last()`, or `.len()` to produce a filter or scalar expression.
    #[inline]
    pub fn temporal_property(name: impl Into<String>) -> TemporalProp<NodeFilter> {
        TemporalProp::new(NodeFilter, name)
    }
}

/// Extension trait that adds `.temporal_property(name)` to any view expression type.
///
/// Implemented for all `T: CreateView + Clone` so that `NodeFilter`, `Windowed<NodeFilter>`,
/// `Layered<NodeFilter>`, etc. all support the same entry point.
pub trait TemporalNodeExprBuilderOps: CreateView + Clone + Send + Sync + Sized + 'static {
    fn temporal_property(self, name: impl Into<String>) -> TemporalProp<Self> {
        TemporalProp::new(self, name)
    }
}

impl<T: CreateView + Clone + Sized + Send + Sync + 'static> TemporalNodeExprBuilderOps for T {}

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
    IsActiveNode(Box<CompositeNodeFilter>),
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
