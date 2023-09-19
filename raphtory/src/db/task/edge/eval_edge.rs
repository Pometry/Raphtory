use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        state::compute_state::ComputeState,
        storage::locked_view::LockedView,
        utils::time::IntoTime,
        ArcStr, Prop, Prop,
    },
    db::{
        api::{
            properties::{
                internal::{ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps},
                Properties,
            },
            view::*,
        },
        graph::{edge::EdgeView, views::window_graph::WindowedGraph},
        task::{
            edge::window_eval_edge::WindowEvalEdgeView,
            task_state::Local2,
            vertex::{
                eval_vertex::EvalVertexView, eval_vertex_state::EVState,
                window_eval_vertex::edge_filter,
            },
        },
    },
};
use std::{cell::RefCell, iter, marker::PhantomData, rc::Rc};

pub struct EvalEdgeView<'a, G: GraphViewOps, CS: ComputeState, S: 'static> {
    ss: usize,
    ev: EdgeRef,
    graph: &'a G,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    local_state_prev: &'a Local2<'a, S>,
    _s: PhantomData<S>,
}

impl<'a, G: GraphViewOps, CS: ComputeState, S> TimeOps for EvalEdgeView<'a, G, CS, S> {
    type WindowedViewType = WindowEvalEdgeView<'a, G, CS, S>;

    fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    fn window<T: IntoTime>(&self, t_start: T, t_end: T) -> Self::WindowedViewType {
        let t_start = t_start.into_time();
        let t_end = t_end.into_time();
        let edge_filter = edge_filter(self.graph, t_start, t_end).map(Rc::new);
        WindowEvalEdgeView::new(
            self.ss,
            self.ev,
            self.graph,
            self.local_state_prev,
            self.vertex_state.clone(),
            t_start,
            t_end,
            edge_filter,
        )
    }
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

    fn layer_ids(&self) -> LayerIds {
        self.graph.layer_ids().constrain_from_edge(self.ev)
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

    fn new_vertex(&self, v: VID) -> EvalVertexView<'a, G, CS, S> {
        EvalVertexView::new_local(
            self.ss,
            v,
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

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> ConstPropertiesOps
    for EvalEdgeView<'a, G, CS, S>
{
    fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.graph.edge_meta().const_prop_meta().get_id(name)
    }

    fn get_const_prop_name(&self, id: usize) -> ArcStr {
        self.graph.edge_meta().const_prop_meta().get_name(id)
    }

    fn const_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        self.graph
            .const_edge_prop_ids(self.ev, self.graph.layer_ids())
    }

    fn get_const_prop(&self, prop_id: usize) -> Option<Prop> {
        self.graph
            .get_const_edge_prop(self.ev, prop_id, self.graph.layer_ids())
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
    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.graph
            .temporal_edge_prop_vec(self.ev, id, self.graph.layer_ids())
            .into_iter()
            .map(|(t, _)| t)
            .collect()
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.graph
            .temporal_edge_prop_vec(self.ev, id, self.graph.layer_ids())
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> TemporalPropertiesOps
    for EvalEdgeView<'a, G, CS, S>
{
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph
            .edge_meta()
            .temporal_prop_meta()
            .get_id(name)
            .filter(|id| {
                self.graph
                    .has_temporal_edge_prop(self.ev, *id, self.layer_ids())
            })
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph.edge_meta().temporal_prop_meta().get_name(id)
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(
            self.graph
                .temporal_edge_prop_ids(self.ev, self.layer_ids())
                .filter(|id| {
                    self.graph
                        .has_temporal_edge_prop(self.ev, *id, self.layer_ids())
                }),
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
            None => Box::new(self.graph.edge_exploded(self.ev, LayerIds::All)),
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

    fn explode_layers(&self) -> Self::EList {
        let iter: Box<dyn Iterator<Item = EdgeRef>> = match self.ev.time() {
            Some(_) => Box::new(iter::once(self.ev)),
            None => Box::new(self.graph.edge_layers(self.ev, LayerIds::All)),
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

    fn time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.time()))
    }

    fn layer_name(self) -> Self::IterType<Option<ArcStr>> {
        Box::new(self.map(|e| e.layer_name().map(|v| v.clone())))
    }

    fn layer_names(self) -> Self::IterType<Vec<String>> {
        Box::new(self.map(|e| e.layer_names()))
    }

    fn history(self) -> Self::IterType<Vec<i64>> {
        Box::new(self.map(|e| e.history()))
    }

    fn start(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.earliest_time()))
    }

    fn start_date_time(self) -> Self::IterType<Option<chrono::NaiveDateTime>> {
        Box::new(self.map(|e| e.start_date_time()))
    }

    fn end(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.latest_time()))
    }

    fn end_date_time(self) -> Self::IterType<Option<chrono::NaiveDateTime>> {
        Box::new(self.map(|e| e.end_date_time()))
    }

    fn date_time(self) -> Self::IterType<Option<chrono::NaiveDateTime>> {
        Box::new(self.map(|e| e.date_time()))
    }

    fn earliest_date_time(self) -> Self::IterType<Option<chrono::NaiveDateTime>> {
        Box::new(self.map(|e| e.earliest_date_time()))
    }

    fn latest_date_time(self) -> Self::IterType<Option<chrono::NaiveDateTime>> {
        Box::new(self.map(|e| e.latest_date_time()))
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
