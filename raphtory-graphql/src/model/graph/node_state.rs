use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::api::state::{NodeStateValue, OutputTypedNodeState, TypedNodeState},
    db::api::view::DynamicGraph,
    prelude::NodeStateOps,
};

/// A mapping from the nodes of a graph to the computed values by an algorithm.
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

#[ResolvedObjectFields]
impl GqlNodeState {
    // TODO: expose the remaining `NodeStateOps` surface, following
    // `PyOutputNodeState` (raphtory/src/python/graph/node_state/output_node_state.rs):
    // `get(node)`, `groupBy(cols)`, `topK(sortParams, k)`, `sortBy(sortParams)`,
    // `values`, `items`, `nodes`, min/max/mean aggregates, ...
    // Operations returning another node state should return `GqlNodeState` so
    // queries can keep chaining.

    /// Returns the number of nodes with a value in this state.
    async fn count(&self) -> usize {
        self.state.len()
    }
}
