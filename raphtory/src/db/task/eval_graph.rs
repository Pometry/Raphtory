use crate::{
    core::{
        entities::nodes::node_ref::AsNodeRef,
        state::compute_state::{ComputeState, ComputeStateVec},
    },
    db::{
        api::storage::storage_ops::GraphStorage,
        task::{
            edge::eval_edge::EvalEdgeView,
            node::{eval_node::EvalNodeView, eval_node_state::EVState},
            task_state::PrevLocalState,
        },
    },
    prelude::GraphViewOps,
};
use std::{cell::RefCell, rc::Rc};

#[derive(Debug)]
pub struct EvalGraph<'graph, 'a, G, S, CS: Clone = ComputeStateVec> {
    pub(crate) ss: usize,
    pub(crate) base_graph: &'graph G,
    pub(crate) storage: &'graph GraphStorage,
    pub(crate) local_state_prev: &'graph PrevLocalState<'a, S>,
    pub(crate) node_state: Rc<RefCell<EVState<'a, CS>>>,
}

impl<'graph, 'a, G, S, CS: Clone> Clone for EvalGraph<'graph, 'a, G, S, CS> {
    fn clone(&self) -> Self {
        Self {
            ss: self.ss,
            base_graph: self.base_graph,
            storage: self.storage,
            local_state_prev: self.local_state_prev,
            node_state: self.node_state.clone(),
        }
    }
}

impl<'graph, 'a: 'graph, G: GraphViewOps<'graph>, S: 'static, CS: ComputeState + Clone>
    EvalGraph<'graph, 'a, G, S, CS>
{
    pub fn node(&self, n: impl AsNodeRef) -> Option<EvalNodeView<'graph, 'a, G, S, &'graph G, CS>> {
        let node = (&self.base_graph).node(n)?;
        Some(EvalNodeView::new_local(node.node, self.clone(), None))
    }

    pub fn edge<N: AsNodeRef>(
        &self,
        src: N,
        dst: N,
    ) -> Option<EvalEdgeView<'graph, 'a, G, &'graph G, CS, S>> {
        let edge = (&self.base_graph).edge(src, dst)?;
        Some(EvalEdgeView::new(
            self.ss,
            edge,
            self.storage,
            self.node_state.clone(),
            self.local_state_prev,
        ))
    }
}
