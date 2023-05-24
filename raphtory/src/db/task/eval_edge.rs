use crate::core::edge_ref::EdgeRef;
use crate::core::state::compute_state::ComputeState;
use crate::core::state::shuffle_state::ShuffleComputeState;
use crate::core::vertex_ref::VertexRef;
use crate::core::Prop;
use crate::db::edge::EdgeView;
use crate::db::task::eval_vertex::EvalVertexView;
use crate::db::view_api::edge::{EdgeViewInternalOps, EdgeViewOps};
use crate::db::view_api::{EdgeListOps, GraphViewOps};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clone)]
pub struct EvalEdgeView<'a, G: GraphViewOps, CS: ComputeState> {
    ss: usize,
    ev: EdgeView<G>,
    shard_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
    global_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
    local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
}

impl<'a, G: GraphViewOps, CS: ComputeState> EvalEdgeView<'a, G, CS> {
    fn new_from_view(&self, ev: EdgeView<G>) -> Self {
        Self {
            ss: self.ss,
            ev,
            shard_state: self.shard_state.clone(),
            global_state: self.global_state.clone(),
            local_state: self.local_state.clone(),
        }
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState> EdgeViewInternalOps<G, EvalVertexView<'a, G, CS>>
    for EvalEdgeView<'a, G, CS>
{
    fn graph(&self) -> G {
        self.ev.graph()
    }

    fn eref(&self) -> EdgeRef {
        self.ev.eref()
    }

    fn new_vertex(&self, v: VertexRef) -> EvalVertexView<'a, G, CS> {
        let vv = self.ev.new_vertex(v);
        let g = vv.graph.clone();
        EvalVertexView::new_from_view(
            self.ss,
            vv,
            g,
            self.shard_state.clone(),
            self.global_state.clone(),
            self.local_state.clone(),
        )
    }

    fn new_edge(&self, e: EdgeRef) -> Self {
        EvalEdgeView::new(
            self.ss,
            e,
            self.graph(),
            self.shard_state.clone(),
            self.global_state.clone(),
            self.local_state.clone(),
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState> EdgeViewOps for EvalEdgeView<'a, G, CS> {
    type Graph = G;
    type Vertex = EvalVertexView<'a, G, CS>;
    type EList = Box<dyn Iterator<Item = Self> + 'a>;

    fn explode(&self) -> Self::EList {
        let eev = self.clone();
        Box::new(self.ev.explode().map(move |ev| eev.new_from_view(ev)))
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState> EdgeListOps
    for Box<dyn Iterator<Item = EvalEdgeView<'a, G, CS>> + 'a>
{
    type Graph = G;
    type Vertex = EvalVertexView<'a, G, CS>;
    type Edge = EvalEdgeView<'a, G, CS>;
    type ValueType<T> = T;
    type VList = Box<dyn Iterator<Item = Self::Vertex> + 'a>;
    type IterType<T> = Box<dyn Iterator<Item = T> + 'a>;

    fn has_property(self, name: String, include_static: bool) -> Self::IterType<bool> {
        Box::new(self.map(move |e| e.has_property(name.clone(), include_static)))
    }

    fn property(self, name: String, include_static: bool) -> Self::IterType<Option<Prop>> {
        Box::new(self.map(move |e| e.property(name.clone(), include_static)))
    }

    fn properties(self, include_static: bool) -> Self::IterType<HashMap<String, Prop>> {
        Box::new(self.map(move |e| e.properties(include_static)))
    }

    fn property_names(self, include_static: bool) -> Self::IterType<Vec<String>> {
        Box::new(self.map(move |e| e.property_names(include_static)))
    }

    fn has_static_property(self, name: String) -> Self::IterType<bool> {
        Box::new(self.map(move |e| e.has_static_property(name.clone())))
    }

    fn static_property(self, name: String) -> Self::IterType<Option<Prop>> {
        Box::new(self.map(move |e| e.static_property(name.clone())))
    }

    fn property_history(self, name: String) -> Self::IterType<Vec<(i64, Prop)>> {
        Box::new(self.map(move |e| e.property_history(name.clone())))
    }

    fn property_histories(self) -> Self::IterType<HashMap<String, Vec<(i64, Prop)>>> {
        Box::new(self.map(|e| e.property_histories()))
    }

    fn src(self) -> Self::VList {
        Box::new(self.map(|e| e.src()))
    }

    fn dst(self) -> Self::VList {
        Box::new(self.map(|e| e.dst()))
    }

    fn explode(self) -> Self::IterType<Self::Edge> {
        Box::new(self.flat_map(|e| e.explode()))
    }

    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.earliest_time()))
    }

    fn latest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.latest_time()))
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState> EvalEdgeView<'a, G, CS> {
    pub fn new(
        ss: usize,
        edge: EdgeRef,
        g: G,
        shard_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
        global_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
        local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
    ) -> Self {
        Self {
            ss,
            ev: EdgeView::new(g, edge),
            shard_state,
            global_state,
            local_state,
        }
    }

    pub fn new_(
        ss: usize,
        ev: EdgeView<G>,
        shard_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
        global_state: Rc<RefCell<Cow<'a, ShuffleComputeState<CS>>>>,
        local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
    ) -> Self {
        Self {
            ss,
            ev,
            shard_state,
            global_state,
            local_state,
        }
    }
}
