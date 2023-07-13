use crate::core::storage::locked_view::LockedView;
use crate::db::api::properties::internal::{
    StaticPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps,
};
use crate::db::api::properties::Properties;
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef},
        state::compute_state::ComputeState,
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
use std::{cell::RefCell, iter, marker::PhantomData, rc::Rc};

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
        self.ev
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

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> StaticPropertiesOps
    for EvalEdgeView<'a, G, CS, S>
{
    fn static_property_keys<'b>(&'b self) -> Box<dyn Iterator<Item = LockedView<'b, String>> + 'b> {
        self.graph.static_edge_prop_names(self.ev)
    }

    fn get_static_property(&self, key: &str) -> Option<Prop> {
        self.graph.static_edge_prop(self.ev, key)
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> Clone for EvalEdgeView<'a, G, CS, S> {
    fn clone(&self) -> Self {
        Self {
            ss: self.ss,
            ev: self.ev,
            graph: self.graph,
            vertex_state: self.vertex_state.clone(),
            local_state_prev: self.local_state_prev,
            _s: Default::default(),
        }
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> TemporalPropertyViewOps
    for EvalEdgeView<'a, G, CS, S>
{
    fn temporal_history(&self, id: &String) -> Vec<i64> {
        self.graph
            .temporal_edge_prop_vec(self.ev, id)
            .into_iter()
            .map(|(t, _)| t)
            .collect()
    }

    fn temporal_values(&self, id: &String) -> Vec<Prop> {
        self.graph
            .temporal_edge_prop_vec(self.ev, id)
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> TemporalPropertiesOps
    for EvalEdgeView<'a, G, CS, S>
{
    fn temporal_property_keys<'b>(
        &'b self,
    ) -> Box<dyn Iterator<Item = LockedView<'b, String>> + 'b> {
        self.graph.temporal_edge_prop_names(self.ev)
    }

    fn get_temporal_property(&self, key: &str) -> Option<String> {
        (!self.graph.temporal_edge_prop_vec(self.ev, key).is_empty()).then_some(key.to_owned())
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

    fn properties(self) -> Self::IterType<Properties<Self::Edge>> {
        Box::new(self.map(move |e| e.properties()))
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
