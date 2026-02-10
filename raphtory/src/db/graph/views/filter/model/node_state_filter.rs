use crate::db::api::state::{Index, NodeStateValue, TypedNodeState};
use crate::{db::api::state::ops::NodeOp, errors::GraphError};
use arrow_array::cast::AsArray;
use arrow_array::{Array, BooleanArray};
use arrow_schema::DataType;
use raphtory_api::core::entities::VID;
use raphtory_storage::graph::graph::GraphStorage;

/// A NodeOp<bool> backed by a boolean column in a TypedNodeState.
/// Does NOT depend on the graph type `G` at runtime; it only needs the column + optional keys.
#[derive(Clone)]
pub struct NodeStateBoolColOp {
    keys: Option<Index<VID>>,
    col: BooleanArray,
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
            col: col.as_boolean().clone(),
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
    fn bool_at_row(&self, row: usize) -> bool {
        self.col.is_valid(row) && self.col.value(row)
    }
}

impl NodeOp for NodeStateBoolColOp {
    type Output = bool;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> bool {
        let row = match self.row_index(node) {
            None => return false,
            Some(r) => r,
        };
        let r = self.bool_at_row(row);
        println!("node {:?}, row {}, bool {}", node, row, r);
        !r
    }
}

