use crate::{
    core::{
        state::compute_state::ComputeState,
        tgraph::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef},
        Prop,
    },
    db::{
        api::view::*,
        task::{
            task_state::Local2,
            vertex::{eval_vertex::EvalVertexView, eval_vertex_state::EVState},
        },
    },
};
use std::{cell::RefCell, collections::HashMap, iter, marker::PhantomData, rc::Rc};

pub struct EvalEdgeView<'a, G: GraphViewOps, CS: ComputeState, S> {
    ss: usize,
    ev: EdgeRef,
    graph: &'a G,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    local_state_prev: &'a Local2<'a, S>,
    _s: PhantomData<S>,
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> EvalEdgeView<'a, G, CS, S> {
    pub(crate) fn new(
        ss: usize,
        ev: EdgeRef,
        graph: &'a G,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
        local_state_prev: &'a Local2<'a, S>,
    ) -> Self {
        Self {
            ss,
            ev,
            graph,
            vertex_state,
            local_state_prev,
            _s: PhantomData,
        }
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static>
    EdgeViewInternalOps<G, EvalVertexView<'a, G, CS, S>> for EvalEdgeView<'a, G, CS, S>
{
    fn graph(&self) -> G {
        self.graph.clone()
    }

    fn eref(&self) -> EdgeRef {
        self.ev.clone()
    }

    fn new_vertex(&self, v: VertexRef) -> EvalVertexView<'a, G, CS, S> {
        EvalVertexView::new_local(
            self.ss,
            self.graph.localise_vertex_unchecked(v),
            self.graph,
            None,
            self.local_state_prev,
            self.vertex_state.clone(),
        )
    }

    fn new_edge(&self, e: EdgeRef) -> Self {
        EvalEdgeView::new_(
            self.ss,
            e,
            self.graph,
            self.local_state_prev,
            self.vertex_state.clone(),
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> EdgeViewOps for EvalEdgeView<'a, G, CS, S> {
    type Graph = G;
    type Vertex = EvalVertexView<'a, G, CS, S>;
    type EList = Box<dyn Iterator<Item = Self> + 'a>;

    fn explode(&self) -> Self::EList {
        let iter: Box<dyn Iterator<Item = EdgeRef>> = match self.ev.time() {
            Some(_) => Box::new(iter::once(self.ev)),
            None => Box::new(self.graph.edge_t(self.ev)),
        };

        let ss = self.ss;
        let g = self.graph;
        let vertex_state = self.vertex_state.clone();
        let local_state_prev = self.local_state_prev;
        Box::new(
            iter.map(move |ev| {
                EvalEdgeView::new(ss, ev, g, vertex_state.clone(), local_state_prev)
            }),
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> EdgeListOps
    for Box<dyn Iterator<Item = EvalEdgeView<'a, G, CS, S>> + 'a>
{
    type Graph = G;
    type Vertex = EvalVertexView<'a, G, CS, S>;
    type Edge = EvalEdgeView<'a, G, CS, S>;
    type ValueType<T> = T;
    type VList = Box<dyn Iterator<Item = Self::Vertex> + 'a>;
    type IterType<T> = Box<dyn Iterator<Item = T> + 'a>;

    fn has_property(self, name: String, include_static: bool) -> Self::IterType<bool> {
        Box::new(self.map(move |e| e.has_property(&name, include_static)))
    }

    fn property(self, name: String, include_static: bool) -> Self::IterType<Option<Prop>> {
        Box::new(self.map(move |e| e.property(&name, include_static)))
    }

    fn properties(self, include_static: bool) -> Self::IterType<HashMap<String, Prop>> {
        Box::new(self.map(move |e| e.properties(include_static)))
    }

    fn property_names(self, include_static: bool) -> Self::IterType<Vec<String>> {
        Box::new(self.map(move |e| e.property_names(include_static)))
    }

    fn has_static_property(self, name: String) -> Self::IterType<bool> {
        Box::new(self.map(move |e| e.has_static_property(&name)))
    }

    fn static_property(self, name: String) -> Self::IterType<Option<Prop>> {
        Box::new(self.map(move |e| e.static_property(&name)))
    }

    fn static_properties(self) -> Self::IterType<HashMap<String, Prop>> {
        Box::new(self.map(move |e| e.static_properties()))
    }

    fn property_history(self, name: String) -> Self::IterType<Vec<(i64, Prop)>> {
        Box::new(self.map(move |e| e.property_history(&name)))
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

    fn id(self) -> Self::IterType<(u64, u64)> {
        Box::new(self.map(|e| e.id()))
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

impl<'a, G: GraphViewOps, CS: ComputeState, S> EvalEdgeView<'a, G, CS, S> {
    pub(crate) fn new_(
        ss: usize,
        ev: EdgeRef,
        graph: &'a G,
        local_state_prev: &'a Local2<'a, S>,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    ) -> Self {
        Self {
            ss,
            ev,
            graph,
            vertex_state,
            local_state_prev,
            _s: PhantomData,
        }
    }
}
