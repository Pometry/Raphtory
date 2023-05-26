use std::{cell::RefCell, marker::PhantomData, rc::Rc};

use crate::{
    core::{edge_ref::EdgeRef, state::compute_state::ComputeState},
    db::view_api::{edge::EdgeViewInternalOps, EdgeListOps, EdgeViewOps, GraphViewOps},
};

use super::{eval_vertex_state::EVState, task_state::Local2, window_eval_vertex::WindowEvalVertex};

pub struct WindowEvalEdgeView<'a, G: GraphViewOps, CS: ComputeState, S: 'static> {
    ss: usize,
    ev: EdgeRef,
    g: &'a G,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    local_state_prev: &'a Local2<'a, S>,
    t_start: i64,
    t_end: i64,
    _s: PhantomData<S>,
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> WindowEvalEdgeView<'a, G, CS, S> {
    pub fn id(&self) -> (u64, u64) {
        (
            self.g
                .vertex_id(self.g.localise_vertex_unchecked(self.ev.src())),
            self.g
                .vertex_id(self.g.localise_vertex_unchecked(self.ev.dst())),
        )
    }

    pub(crate) fn new(
        ss: usize,
        ev: EdgeRef,
        g: &'a G,
        local_state_prev: &'a Local2<'a, S>,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
        t_start: i64,
        t_end: i64,
    ) -> Self {
        Self {
            ss,
            ev,
            g,
            vertex_state,
            local_state_prev,
            t_start,
            t_end,
            _s: PhantomData,
        }
    }

    pub fn history(&self) -> Vec<i64> {
        self.graph().edge_timestamps(self.eref(), Some(self.t_start..self.t_end))
    }
}
impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static>
    EdgeViewInternalOps<G, WindowEvalVertex<'a, G, CS, S>> for WindowEvalEdgeView<'a, G, CS, S>
{
    fn graph(&self) -> G {
        self.g.clone()
    }

    fn eref(&self) -> EdgeRef {
        self.ev.clone()
    }

    fn new_vertex(&self, v: crate::core::vertex_ref::VertexRef) -> WindowEvalVertex<'a, G, CS, S> {
        WindowEvalVertex::new(
            self.ss,
            self.g.localise_vertex_unchecked(v),
            self.g,
            None,
            self.local_state_prev,
            self.vertex_state.clone(),
            self.t_start,
            self.t_end,
        )
    }

    fn new_edge(&self, e: EdgeRef) -> Self {
        WindowEvalEdgeView::new(
            self.ss,
            e,
            self.g,
            self.local_state_prev,
            self.vertex_state.clone(),
            self.t_start,
            self.t_end,
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> EdgeViewOps
    for WindowEvalEdgeView<'a, G, CS, S>
{
    type Graph = G;

    type Vertex = WindowEvalVertex<'a, G, CS, S>;

    type EList = Box<dyn Iterator<Item = Self> + 'a>;

    fn explode(&self) -> Self::EList {
        todo!()
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> EdgeListOps
    for Box<dyn Iterator<Item = WindowEvalEdgeView<'a, G, CS, S>> + 'a>
{
    type Graph = G;
    type Vertex = WindowEvalVertex<'a, G, CS, S>;
    type Edge = WindowEvalEdgeView<'a, G, CS, S>;
    type ValueType<T> = T;
    type VList = Box<dyn Iterator<Item = Self::Vertex> + 'a>;
    type IterType<T> = Box<dyn Iterator<Item = T> + 'a>;

    fn has_property(self, name: String, include_static: bool) -> Self::IterType<bool> {
        todo!()
    }

    fn property(
        self,
        name: String,
        include_static: bool,
    ) -> Self::IterType<Option<crate::core::Prop>> {
        todo!()
    }

    fn properties(
        self,
        include_static: bool,
    ) -> Self::IterType<std::collections::HashMap<String, crate::core::Prop>> {
        todo!()
    }

    fn property_names(self, include_static: bool) -> Self::IterType<Vec<String>> {
        todo!()
    }

    fn has_static_property(self, name: String) -> Self::IterType<bool> {
        todo!()
    }

    fn static_property(self, name: String) -> Self::IterType<Option<crate::core::Prop>> {
        todo!()
    }

    fn property_history(self, name: String) -> Self::IterType<Vec<(i64, crate::core::Prop)>> {
        todo!()
    }

    fn property_histories(
        self,
    ) -> Self::IterType<std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>>> {
        todo!()
    }

    fn src(self) -> Self::VList {
        todo!()
    }

    fn dst(self) -> Self::VList {
        todo!()
    }

    fn explode(self) -> Self::IterType<Self::Edge> {
        todo!()
    }

    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        todo!()
    }

    fn latest_time(self) -> Self::IterType<Option<i64>> {
        todo!()
    }
}
