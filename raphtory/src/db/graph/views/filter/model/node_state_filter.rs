use crate::{
    db::{
        api::{
            state::{ops::NodeOp, Index, NodeStateValue, TypedNodeState},
            view::internal::{DynGraphArc, NodeList},
        },
        graph::views::filter::model::expr::OpaqueOp,
    },
    errors::GraphError,
};
use arrow_array::{cast::AsArray, Array, BooleanArray};
use arrow_schema::DataType;
use raphtory_api::core::entities::{
    properties::prop::{Prop, PropType},
    VID,
};
use raphtory_storage::graph::graph::GraphStorage;
use std::sync::Arc;

/// A NodeOp<bool> backed by a boolean column in a TypedNodeState.
/// Does NOT depend on the graph type `G` at runtime; it only needs the column + optional keys.
#[derive(Clone)]
pub struct NodeStateBoolColOp {
    keys: Index<VID>,
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
        self.keys.index(&node)
    }

    #[inline]
    fn bool_at_row(&self, row: usize) -> bool {
        self.col.is_valid(row) && self.col.value(row)
    }
}

impl NodeOp for NodeStateBoolColOp {
    type Output = bool;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        let elems = self.keys.clone();
        NodeList::List { elems }
    }

    fn apply(&self, _storage: &GraphStorage, node: VID) -> bool {
        let row = match self.row_index(node) {
            None => return false,
            Some(r) => r,
        };
        self.bool_at_row(row)
    }
}

/// The column as a yes/no node value, so it combines with other node
/// predicates and negates like one.
impl OpaqueOp for NodeStateBoolColOp {
    fn node_value_op<'g>(
        &self,
        _graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(self.clone().map(|b| Some(Prop::Bool(b)))))
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn nullable(&self) -> bool {
        false
    }
}
