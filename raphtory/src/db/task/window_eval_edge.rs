use std::{cell::RefCell, collections::HashMap, iter, marker::PhantomData, rc::Rc};

use crate::db::view_api::edge::EdgeViewInternalOps;
use crate::db::view_api::internal::*;
use crate::{
    core::{edge_ref::EdgeRef, state::compute_state::ComputeState, Prop},
    db::view_api::*,
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
        self.graph()
            .edge_window_t(self.eref(), self.t_start..self.t_end)
            .map(|e| e.time().expect("exploded"))
            .collect()
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
        let e = self.ev.clone();
        let t_start = self.t_start;
        let t_end = self.t_end;
        let ss = self.ss;
        let g = self.g;
        let vertex_state = self.vertex_state.clone();
        let local_state_prev = self.local_state_prev;

        match self.ev.time() {
            Some(_) => Box::new(iter::once(self.new_edge(e))),
            None => {
                let ts = self.g.edge_window_t(e, t_start..t_end);
                Box::new(ts.map(move |ex| {
                    WindowEvalEdgeView::new(
                        ss,
                        ex,
                        g,
                        local_state_prev,
                        vertex_state.clone(),
                        t_start,
                        t_end,
                    )
                }))
            }
        }
    }

    fn history(&self) -> Vec<i64> {
        self.graph()
            .edge_history_window(self.ev, self.t_start..self.t_end)
            .collect()
    }

    fn property_history(&self, name: &str) -> Vec<(i64, Prop)> {
        match self.eref().time() {
            None => self.graph().temporal_edge_prop_vec_window(
                self.eref(),
                name,
                self.t_start,
                self.t_end,
            ),
            Some(t) => self.graph().temporal_edge_prop_vec_window(
                self.eref(),
                name,
                t,
                t.saturating_add(1),
            ),
        }
    }

    fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        // match on the self.edge.time option property and run two function s
        // one for static and one for temporal
        match self.eref().time() {
            None => self
                .graph()
                .temporal_edge_props_window(self.eref(), self.t_start, self.t_end),
            Some(t) => self
                .graph()
                .temporal_edge_props_window(self.eref(), t, t.saturating_add(1)),
        }
    }

    /// Check if edge is active at a given time point
    fn active(&self, t: i64) -> bool {
        match self.eref().time() {
            Some(tt) => tt == t,
            None => {
                (self.t_start..self.t_end).contains(&t)
                    && self.graph().has_edge_ref_window(
                        self.eref().src(),
                        self.eref().dst(),
                        t,
                        t.saturating_add(1),
                        self.eref().layer(),
                    )
            }
        }
    }

    /// Gets the first time an edge was seen
    fn earliest_time(&self) -> Option<i64> {
        self.eref().time().or_else(|| {
            self.graph()
                .edge_earliest_time_window(self.eref(), self.t_start..self.t_end)
        })
    }

    /// Gets the latest time an edge was updated
    fn latest_time(&self) -> Option<i64> {
        self.eref().time().or_else(|| {
            self.graph()
                .edge_latest_time_window(self.eref(), self.t_start..self.t_end)
        })
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
        Box::new(self.map(move |e| e.has_property(&name, include_static)))
    }

    fn property(
        self,
        name: String,
        include_static: bool,
    ) -> Self::IterType<Option<crate::core::Prop>> {
        Box::new(self.map(move |e| e.property(&name, include_static)))
    }

    fn properties(
        self,
        include_static: bool,
    ) -> Self::IterType<std::collections::HashMap<String, crate::core::Prop>> {
        Box::new(self.map(move |e| e.properties(include_static)))
    }

    fn property_names(self, include_static: bool) -> Self::IterType<Vec<String>> {
        Box::new(self.map(move |e| e.property_names(include_static)))
    }

    fn has_static_property(self, name: String) -> Self::IterType<bool> {
        Box::new(self.map(move |e| e.has_static_property(&name)))
    }

    fn static_property(self, name: String) -> Self::IterType<Option<Prop>> {
        Box::new(self.map(move |it| it.static_property(&name)))
    }

    fn static_properties(self) -> Self::IterType<HashMap<String, Prop>> {
        Box::new(self.map(move |it| it.static_properties()))
    }

    fn property_history(self, name: String) -> Self::IterType<Vec<(i64, Prop)>> {
        Box::new(self.map(move |it| it.property_history(&name)))
    }

    fn property_histories(
        self,
    ) -> Self::IterType<std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>>> {
        Box::new(self.map(|it| it.property_histories()))
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
        Box::new(self.flat_map(move |it| it.explode()))
    }

    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.earliest_time()))
    }

    fn latest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.latest_time()))
    }
}
