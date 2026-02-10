use std::sync::Arc;

use crate::{
    db::{
        api::{
            state::ops::{NodeOp},
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            model::{edge_filter::CompositeEdgeFilter, CompositeExplodedEdgeFilter, TryAsCompositeFilter},
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use crate::db::graph::views::filter::model::ComposableFilter;

#[derive(Clone)]
pub struct NodeStateFilter(pub Arc<dyn NodeOp<Output = bool> + Send + Sync>);

impl std::fmt::Debug for NodeStateFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeStateFilter(<dyn NodeOp<bool>>)")
    }
}

impl ComposableFilter for NodeStateFilter {}

impl CreateFilter for NodeStateFilter {
    type EntityFiltered<'g, G> = NodeFilteredGraph<G, Self::NodeFilter<'g, G>>
    where
        Self: 'g,
        G: GraphViewOps<'g>;

    type NodeFilter<'g, G> = Arc<dyn NodeOp<Output = bool> + 'g>
    where
        Self: 'g,
        G: GraphView + 'g;

    type FilteredGraph<'g, G> = Arc<dyn BoxableGraphView + 'g>
    where
        Self: 'g,
        G: GraphViewOps<'g>;

    fn create_filter<'g, G: GraphViewOps<'g>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'g, G>, GraphError> {
        let op = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'g, G: GraphView + 'g>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'g, G>, GraphError> {
        Ok(self.0)
    }

    fn filter_graph_view<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'g, G>, GraphError> {
        Ok(Arc::new(graph))
    }
}

impl TryAsCompositeFilter for NodeStateFilter {
    fn try_as_composite_node_filter(
        &self,
    ) -> Result<crate::db::graph::views::filter::model::node_filter::CompositeNodeFilter, GraphError> {
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

use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use raphtory_api::core::entities::VID;
use raphtory_storage::graph::graph::GraphStorage;
use crate::db::api::state::{Index, NodeStateValue, TypedNodeState};

/// A NodeOp<bool> backed by a boolean column in a TypedNodeState.
/// Does NOT depend on the graph type `G` at runtime; it only needs the column + optional keys.
#[derive(Clone)]
pub struct NodeStateBoolColOp {
    keys: Option<Index<VID>>,
    col: ArrayRef,
}

impl NodeStateBoolColOp {
    pub fn new<'graph, V, G, T>(
        state: &TypedNodeState<'graph, V, G, T>,
        col_name: &str,
    ) -> Result<Self, GraphError>
    where
        V: NodeStateValue + 'graph,
        T: Clone + Send + Sync + 'graph,
    {
        let col = state
            .state
            .values_ref()
            .column_by_name(col_name)
            .ok_or_else(|| GraphError::ColumnDoesNotExist(col_name.to_string()))?
            .clone();

        if col.data_type() != &DataType::Boolean {
            return Err(GraphError::ColumnDoesNotExist(format!(
                "Column {col_name} exists but is not boolean (found {:?})",
                col.data_type()
            )));
        }

        Ok(Self {
            keys: state.state.keys.clone(),
            col,
        })
    }

    #[inline]
    fn row_index(&self, node: VID) -> Option<usize> {
        match &self.keys {
            Some(index) => index.index(&node),
            None => Some(node.0),
        }
    }

    #[inline]
    fn bool_at_row(&self, row: usize, col: &BooleanArray) -> bool {
        col.is_valid(row) && col.value(row)
    }
}

impl NodeOp for NodeStateBoolColOp {
    type Output = bool;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> bool {
        let row = match self.row_index(node) {
            None => return false,
            Some(r) => r,
        };
        let bools: &BooleanArray = self.col.as_boolean();
        let r = self.bool_at_row(row, bools);

        println!("node {:?}, row {}, bool {}", node, row, r);
        !r
    }
}

impl<'graph, V, G, T> TypedNodeState<'graph, V, G, T>
where
    V: NodeStateValue + 'graph,
    T: Clone + Send + Sync + 'graph,
{
    pub fn bool_col_filter(&self, col: &str) -> Result<NodeStateBoolColOp, GraphError> {
        NodeStateBoolColOp::new(self, col)
    }
}
