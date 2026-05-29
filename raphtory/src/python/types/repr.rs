use crate::{
    db::api::state::{LazyNodeState, NodeOp, NodeState, NodeStateValue, TypedNodeState},
    prelude::{GraphViewOps, NodeStateOps, NodeViewOps},
    python::graph::node_state::NodeFilterOp,
};

pub use raphtory_api::python::repr::{iterator_dict_repr, iterator_repr, Repr, StructReprBuilder};

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + 'graph,
        Op: NodeOp + 'graph,
    > Repr for LazyNodeState<'graph, Op, G, GH, F>
where
    Op::Output: Repr + Send + Sync + 'graph,
{
    fn repr(&self) -> String {
        StructReprBuilder::new("LazyNodeState")
            .add_fields_from_iter(self.iter().map(|(n, v)| (n.name(), v)))
            .finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>, V: Repr + Clone + Send + Sync + 'graph> Repr
    for NodeState<'graph, V, G>
{
    fn repr(&self) -> String {
        StructReprBuilder::new("NodeState")
            .add_fields_from_iter(self.iter().map(|(n, v)| (n.name(), v)))
            .finish()
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        V: Repr + NodeStateValue + 'graph,
        T: Clone + Send + Sync + 'graph,
    > Repr for TypedNodeState<'graph, V, G, T>
{
    fn repr(&self) -> String {
        StructReprBuilder::new("TypedNodeState")
            .add_fields_from_iter(self.iter().map(|(n, v)| (n.name(), v)))
            .finish()
    }
}
