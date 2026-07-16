use crate::{
    model::graph::{
        node::GqlNode, node_id::GqlNodeId, nodes::GqlNodes, property::GqlPropertyOutputVal,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, SimpleObject, Union};
use raphtory::{
    db::{
        api::{
            state::{NodeStateOutput, NodeStateValue, OutputTypedNodeState, TypedNodeState},
            view::{BoxableGraphView, DynamicGraph},
        },
        graph::node::NodeView,
    },
    prelude::{NodeStateOps, Prop},
};
use std::{cmp::Ordering, sync::Arc};

/// A mapping from the nodes of a graph to the values computed for them by an algorithm.
///
/// The output is columnar: every column of the underlying state is exposed as
/// a `NodeStateColumn` whose `values` are row-aligned with `nodes`.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "NodeState")]
pub(crate) struct GqlNodeState {
    pub(crate) state: OutputTypedNodeState<'static, DynamicGraph>,
}

impl<V, T> From<TypedNodeState<'static, V, DynamicGraph, T>> for GqlNodeState
where
    V: NodeStateValue + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn from(state: TypedNodeState<'static, V, DynamicGraph, T>) -> Self {
        Self {
            state: state.to_output_nodestate(),
        }
    }
}

/// A single cell of a node state column: either a plain property value, a
/// node, or a collection of nodes.
#[derive(Union, Clone)]
#[graphql(name = "NodeStateValue")]
pub(crate) enum GqlNodeStateValue {
    Prop(GqlNodeStateProp),
    Node(GqlNode),   // TODO: test this
    Nodes(GqlNodes), // TODO: test this
}

/// A plain property value of a node state cell.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateProp")]
pub(crate) struct GqlNodeStateProp {
    /// The property value; null if the node has no value in this column.
    value: Option<GqlPropertyOutputVal>,
}

impl From<NodeStateOutput<'static, Arc<dyn BoxableGraphView>>> for GqlNodeStateValue {
    fn from(value: NodeStateOutput<'static, Arc<dyn BoxableGraphView>>) -> Self {
        match value {
            NodeStateOutput::Prop(prop) => GqlNodeStateValue::Prop(GqlNodeStateProp {
                value: prop.map(|p| GqlPropertyOutputVal(p.into())),
            }),
            NodeStateOutput::Node(node) => GqlNodeStateValue::Node(node.into()),
            NodeStateOutput::Nodes(nodes) => GqlNodeStateValue::Nodes(GqlNodes::new(nodes)),
        }
    }
}

/// One column of a node state: the values of a single output field of the
/// algorithm. Row-aligned with `NodeState.nodes`.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateColumn")]
pub(crate) struct GqlNodeStateColumn {
    /// Name of the column.
    name: String,
    /// The values of this column; `values[i]` belongs to `NodeState.nodes[i]`.
    values: Vec<GqlNodeStateValue>,
}

/// One column's value for a single node.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateEntry")]
pub(crate) struct GqlNodeStateEntry {
    /// Name of the column.
    name: String,
    /// The node's value in this column.
    value: GqlNodeStateValue,
}

/// A `(node, value)` pair, e.g. the result of a column aggregate.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateItem")]
pub(crate) struct GqlNodeStateItem {
    /// The node.
    node: GqlNode,
    /// The node's value.
    value: GqlPropertyOutputVal,
}

impl GqlNodeState {
    /// The `(node, value)` pairs of a plain-prop column, skipping empty cells. Useful for aggregate operations.
    /// None if the column does not exist or contains nodes.
    fn column_items(&self, column: &str) -> Option<Vec<(NodeView<'static, DynamicGraph>, Prop)>> {
        if self.state.state.node_cols.contains_key(column) {
            return None;
        }
        self.state.state.values_ref().schema().index_of(column).ok()?;
        Some(
            self.state
                .iter()
                .filter_map(|(node, mut row)| {
                    let value = row.swap_remove(column)??;
                    Some((node.cloned(), value.into()))
                })
                .collect(),
        )
    }

    /// Reduces a column to the item winning all `pick` comparisons. Basically executes a simple aggregate operation.
    /// None if the column is empty or its dtype is not comparable.
    fn reduce_column(
        &self,
        column: &str,
        pick: impl Fn(Ordering) -> bool,
    ) -> Option<GqlNodeStateItem> {
        let mut items = self.column_items(column)?.into_iter();
        let mut acc = items.next()?;
        // we only check this once because, in practice, the entire Arrow column has the same type,
        // so we expect Props to have the same dtype.
        if !acc.1.dtype().has_cmp() {
            return None;
        }
        for item in items {
            if !pick(acc.1.partial_cmp(&item.1)?) {
                acc = item;
            }
        }
        Some(GqlNodeStateItem {
            node: acc.0.into(),
            value: GqlPropertyOutputVal(acc.1),
        })
    }
}

// TODO: add paging: `columns`/`nodes` currently dump every row.

// TODO: still to be implemented, blocked on the datafusion feature gate (CVE):
// `sortBy` (`GenericNodeState::sort_by`), `topK` (`GenericNodeState::top_k`),
// `groupBy` (`TypedNodeState::get_groups`).
//
// Not exposed: `merge` (takes a second NodeState, which cannot be a query argument)
// and `to_parquet`/`from_parquet` (avoid server-side filesystem access).
#[ResolvedObjectFields]
impl GqlNodeState {
    /// Returns the number of nodes with a value in this state.
    async fn count(&self) -> usize {
        self.state.len()
    }

    /// The nodes with a value in this state, in row order. Aligned with `values`.
    async fn nodes(&self) -> GqlNodes {
        GqlNodes::new(self.state.nodes())
    }

    /// Returns the values for a node, one entry per column; null if the node has no value in this NodeState.
    async fn get(
        &self,
        #[graphql(desc = "Node id.")] node: GqlNodeId,
    ) -> Option<Vec<GqlNodeStateEntry>> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let row = self_clone.state.get_by_node(node)?;
            let transformed = self_clone.state.convert(row);
            Some(
                transformed
                    .into_iter()
                    .map(|(name, value)| GqlNodeStateEntry {
                        name,
                        value: value.into(),
                    })
                    .collect(),
            )
        })
        .await
    }

    /// Minimum `(node, value)` of a column. Null if the column does not exist, is empty,
    /// or its values are not comparable (e.g. contains nodes).
    async fn min(
        &self,
        #[graphql(desc = "Column name.")] column: String,
    ) -> Option<GqlNodeStateItem> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.reduce_column(&column, Ordering::is_le)).await
    }

    /// Maximum `(node, value)` of a column. Null if the column does not exist, is empty,
    /// or its values are not comparable (e.g. contains nodes).
    async fn max(
        &self,
        #[graphql(desc = "Column name.")] column: String,
    ) -> Option<GqlNodeStateItem> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.reduce_column(&column, Ordering::is_ge)).await
    }

    /// Median `(node, value)` of a column (lower median on even lengths). Null if the column
    /// does not exist, is empty, or its values are not comparable (e.g. contains nodes).
    async fn median(
        &self,
        #[graphql(desc = "Column name.")] column: String,
    ) -> Option<GqlNodeStateItem> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let mut items = self_clone.column_items(&column)?;
            if !items.first()?.1.dtype().has_cmp() {
                return None;
            }
            items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
            let len = items.len();
            let (node, value) = items.swap_remove((len - 1) / 2);
            Some(GqlNodeStateItem {
                node: node.into(),
                value: GqlPropertyOutputVal(value),
            })
        })
        .await
    }

    /// Returns a view of this state with the rows sorted by node id.
    async fn sort_by_id(&self) -> GqlNodeState {
        let self_clone = self.clone();
        blocking_compute(move || GqlNodeState {
            state: self_clone.state.sort_by_id(),
        })
        .await
    }

    /// The columns of the state, one per output field of the algorithm.
    /// `values` are row-aligned with `nodes`.
    async fn columns(&self) -> Vec<GqlNodeStateColumn> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let num_rows = self_clone.state.len();
            let mut columns: Vec<(String, Vec<GqlNodeStateValue>)> = self_clone
                .state
                .state
                .values_ref()
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().clone(), Vec::with_capacity(num_rows)))
                .collect();
            for row in self_clone.state.values_to_rows() {
                let mut transformed = self_clone.state.convert(row);
                for (name, values) in columns.iter_mut() {
                    if let Some(value) = transformed.swap_remove(name) {
                        values.push(value.into());
                    }
                }
            }
            columns
                .into_iter()
                .map(|(name, values)| GqlNodeStateColumn { name, values })
                .collect()
        })
        .await
    }
}
