use crate::{
    api::core::Direction,
    db::{
        api::{
            state::{
                ops::{
                    filter::{MaskOp, NodeIdFilterOp, NodeNameFilterOp, NodeTypeFilterOp},
                    Id, Name, NodeOp, Type, TypeId,
                },
                NodeStateValue, TypedNodeState,
            },
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            model::{
                filter::Filter,
                is_active_node_filter::IsActiveNode,
                latest_filter::Latest,
                layered_filter::Layered,
                node_expr::DegreeExpr,
                node_filter::validate::validate,
                node_state_filter::NodeStateBoolColOp,
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                ComposableFilter, CreateView, EntityMarker, InternalViewWrapOps, NodeViewFilterOps,
                PropertyFilterFactory, Wrap,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::storage::timeindex::EventTime;
use std::{fmt, fmt::Display};

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

    fn is_active(&self) -> IsActiveNode<Self> {
        IsActiveNode {
            view_expr: self.clone(),
        }
    }
}

impl NodeFilterFactory for NodeFilter {}

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

impl<T: NodeFilterFactory + CreateView> NodeFilterFactory for Windowed<T> {}
impl<T: NodeFilterFactory + CreateView> NodeFilterFactory for Latest<T> {}
impl<T: NodeFilterFactory + CreateView> NodeFilterFactory for SnapshotAt<T> {}
impl<T: NodeFilterFactory + CreateView> NodeFilterFactory for SnapshotLatest<T> {}
impl<T: NodeFilterFactory + CreateView> NodeFilterFactory for Layered<T> {}

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
