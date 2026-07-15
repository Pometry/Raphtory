use crate::{
    model::graph::{node::GqlNode, nodes::GqlNodes, property::GqlPropertyOutputVal},
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, SimpleObject, Union};
use raphtory::{
    db::api::{
        state::{NodeStateOutput, NodeStateValue, OutputTypedNodeState, TypedNodeState},
        view::{BoxableGraphView, DynamicGraph},
    },
    prelude::NodeStateOps,
};
use std::sync::Arc;

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
    Node(GqlNode), // TODO: test this
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

// TODO: add paging: `columns`/`nodes` currently dump every row.

// TODO: NodeStateOps surface — expose the remaining operations
// follow `PyOutputNodeState` (raphtory/src/python/graph/node_state/output_node_state.rs):
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
