use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        state::compute_state::ComputeState,
        utils::time::IntoTime,
        ArcStr, Prop,
    },
    db::{
        api::{
            properties::{
                internal::{ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps},
                Properties,
            },
            view::{internal::*, *},
        },
        graph::views::window_graph::WindowedGraph,
        task::{
            task_state::Local2,
            vertex::{
                eval_vertex_state::EVState,
                window_eval_vertex::{edge_filter, WindowEvalVertex},
            },
        },
    },
};
use std::{cell::RefCell, iter, marker::PhantomData, rc::Rc};

pub struct WindowEvalEdgeView<'a, G: GraphViewOps, CS: ComputeState, S: 'static> {
    ss: usize,
    ev: EdgeRef,
    g: &'a G,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    local_state_prev: &'a Local2<'a, S>,
    t_start: i64,
    t_end: i64,
    _s: PhantomData<S>,
    edge_filter: Option<Rc<EdgeFilter>>,
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
        edge_filter: Option<Rc<EdgeFilter>>,
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
            edge_filter,
        }
    }

    pub fn history(&self) -> Vec<i64> {
        self.graph()
            .edge_window_exploded(self.eref(), self.t_start..self.t_end, LayerIds::All)
            .map(|e| e.time_t().expect("exploded"))
            .collect()
    }

    fn layer_ids(&self) -> LayerIds {
        self.g.layer_ids().constrain_from_edge(self.ev)
    }

    pub fn start(&self) -> Option<i64> {
        self.graph()
            .edge_earliest_time_window(self.eref(), self.t_start..self.t_end, LayerIds::All)
    }

    pub fn start_date_time(&self) -> Option<chrono::NaiveDateTime> {
        self.graph()
            .edge_earliest_time_window(self.eref(), self.t_start..self.t_end, LayerIds::All)
            .map(|t| chrono::NaiveDateTime::from_timestamp_millis(t).unwrap())
    }

    pub fn end(&self) -> Option<i64> {
        self.graph()
            .edge_latest_time_window(self.eref(), self.t_start..self.t_end, LayerIds::All)
    }

    pub fn end_date_time(&self) -> Option<chrono::NaiveDateTime> {
        self.graph()
            .edge_latest_time_window(self.eref(), self.t_start..self.t_end, LayerIds::All)
            .map(|t| chrono::NaiveDateTime::from_timestamp_millis(t).unwrap())
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> TimeOps
    for WindowEvalEdgeView<'a, G, CS, S>
{
    type WindowedViewType = WindowEvalEdgeView<'a, G, CS, S>;

    fn start(&self) -> Option<i64> {
        Some(self.t_start)
    }

    fn end(&self) -> Option<i64> {
        Some(self.t_end)
    }

    fn window<T: IntoTime>(&self, t_start: T, t_end: T) -> Self::WindowedViewType {
        let t_start = t_start.into_time().max(self.t_start);
        let t_end = t_end.into_time().min(self.t_end);
        let edge_filter = edge_filter(self.g, t_start, t_end).map(Rc::new);
        WindowEvalEdgeView {
            ss: self.ss,
            ev: self.ev,
            g: self.g,
            vertex_state: self.vertex_state.clone(),
            local_state_prev: self.local_state_prev,
            t_start,
            t_end,
            _s: Default::default(),
            edge_filter,
        }
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static>
    EdgeViewInternalOps<WindowedGraph<G>, WindowEvalVertex<'a, G, CS, S>>
    for WindowEvalEdgeView<'a, G, CS, S>
{
    fn graph(&self) -> WindowedGraph<G> {
        WindowedGraph::new(self.g.clone(), self.t_start, self.t_end)
    }

    fn eref(&self) -> EdgeRef {
        self.ev.clone()
    }

    fn new_vertex(&self, v: VID) -> WindowEvalVertex<'a, G, CS, S> {
        WindowEvalVertex::new(
            self.ss,
            v,
            self.g,
            None,
            self.local_state_prev,
            self.vertex_state.clone(),
            self.t_start,
            self.t_end,
            self.edge_filter.clone(),
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
            self.edge_filter.clone(),
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> ConstPropertiesOps
    for WindowEvalEdgeView<'a, G, CS, S>
{
    fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.g.edge_meta().const_prop_meta().get_id(name)
    }

    fn get_const_prop_name(&self, id: usize) -> ArcStr {
        self.g.edge_meta().const_prop_meta().get_name(id)
    }

    fn const_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        self.g.const_edge_prop_ids(self.ev, self.g.layer_ids())
    }

    fn get_const_prop(&self, prop_id: usize) -> Option<Prop> {
        self.graph()
            .get_const_edge_prop(self.ev, prop_id, self.g.layer_ids())
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> Clone for WindowEvalEdgeView<'a, G, CS, S> {
    fn clone(&self) -> Self {
        Self {
            ss: self.ss,
            ev: self.ev,
            g: self.g,
            vertex_state: self.vertex_state.clone(),
            local_state_prev: self.local_state_prev,
            t_start: self.t_start,
            t_end: self.t_end,
            _s: Default::default(),
            edge_filter: self.edge_filter.clone(),
        }
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> TemporalPropertyViewOps
    for WindowEvalEdgeView<'a, G, CS, S>
{
    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.g
            .temporal_edge_prop_vec_window(
                self.ev,
                id,
                self.t_start,
                self.t_end,
                self.g.layer_ids(),
            )
            .last()
            .map(|(_, v)| v.to_owned())
    }

    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.g
            .temporal_edge_prop_vec_window(
                self.ev,
                id,
                self.t_start,
                self.t_end,
                self.g.layer_ids(),
            )
            .into_iter()
            .map(|(t, _)| t)
            .collect()
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.g
            .temporal_edge_prop_vec_window(
                self.ev,
                id,
                self.t_start,
                self.t_end,
                self.g.layer_ids(),
            )
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> TemporalPropertiesOps
    for WindowEvalEdgeView<'a, G, CS, S>
{
    fn get_temporal_prop_id(&self, key: &str) -> Option<usize> {
        self.g
            .edge_meta()
            .temporal_prop_meta()
            .get_id(key)
            .filter(|&id| {
                self.g.has_temporal_edge_prop_window(
                    self.ev,
                    id,
                    self.t_start..self.t_end,
                    self.layer_ids(),
                )
            })
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.g.edge_meta().temporal_prop_meta().get_name(id)
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(
            self.g
                .temporal_edge_prop_ids(self.ev, self.g.layer_ids())
                .filter(|&id| {
                    self.g.has_temporal_edge_prop_window(
                        self.ev,
                        id,
                        self.t_start..self.t_end,
                        self.layer_ids(),
                    )
                }),
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> EdgeViewOps
    for WindowEvalEdgeView<'a, G, CS, S>
{
    type Graph = WindowedGraph<G>;

    type Vertex = WindowEvalVertex<'a, G, CS, S>;

    type EList = Box<dyn Iterator<Item = Self> + 'a>;

    fn history(&self) -> Vec<i64> {
        self.graph()
            .edge_window_exploded(self.ev, self.t_start..self.t_end, self.g.layer_ids())
            .map(|eref| eref.time_t().expect("exploded"))
            .collect()
    }

    /// Check if edge is active at a given time point
    fn active(&self, t: i64) -> bool {
        match self.eref().time_t() {
            Some(tt) => tt <= t && t <= self.latest_time().unwrap_or(tt),
            None => {
                let layer_ids = self.graph().layer_ids().constrain_from_edge(self.eref());
                let entry = self.graph().core_edge(self.eref().pid());
                (self.t_start..self.t_end).contains(&t)
                    && self
                        .graph()
                        .include_edge_window(&entry, t..t.saturating_add(1), &layer_ids)
            }
        }
    }

    fn explode(&self) -> Self::EList {
        let e = self.ev.clone();
        let t_start = self.t_start;
        let t_end = self.t_end;
        let ss = self.ss;
        let g = self.g;
        let layer_ids = g.layer_ids();
        let vertex_state = self.vertex_state.clone();
        let local_state_prev = self.local_state_prev;
        let edge_filter = self.edge_filter.clone();
        match self.ev.time() {
            Some(_) => Box::new(iter::once(self.new_edge(e))),
            None => {
                let ts = self.g.edge_window_exploded(e, t_start..t_end, layer_ids);
                Box::new(ts.map(move |ex| {
                    WindowEvalEdgeView::new(
                        ss,
                        ex,
                        g,
                        local_state_prev,
                        vertex_state.clone(),
                        t_start,
                        t_end,
                        edge_filter.clone(),
                    )
                }))
            }
        }
    }

    fn explode_layers(&self) -> Self::EList {
        let e = self.ev.clone();
        let t_start = self.t_start;
        let t_end = self.t_end;
        let ss = self.ss;
        let g = self.g;
        let vertex_state = self.vertex_state.clone();
        let local_state_prev = self.local_state_prev;
        let edge_filter = self.edge_filter.clone();
        let layer_ids = g.layer_ids();

        match self.ev.time() {
            Some(_) => Box::new(iter::once(self.new_edge(e))),
            None => {
                let ts = self.g.edge_window_layers(e, t_start..t_end, layer_ids);
                Box::new(ts.map(move |ex| {
                    WindowEvalEdgeView::new(
                        ss,
                        ex,
                        g,
                        local_state_prev,
                        vertex_state.clone(),
                        t_start,
                        t_end,
                        edge_filter.clone(),
                    )
                }))
            }
        }
    }

    /// Gets the first time an edge was seen
    fn earliest_time(&self) -> Option<i64> {
        self.eref().time_t().or_else(|| {
            self.graph().edge_earliest_time_window(
                self.eref(),
                self.t_start..self.t_end,
                LayerIds::All,
            )
        })
    }

    /// Gets the latest time an edge was updated
    fn latest_time(&self) -> Option<i64> {
        self.eref().time_t().or_else(|| {
            self.graph().edge_latest_time_window(
                self.eref(),
                self.t_start..self.t_end,
                LayerIds::All,
            )
        })
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> EdgeListOps
    for Box<dyn Iterator<Item = WindowEvalEdgeView<'a, G, CS, S>> + 'a>
{
    type Graph = WindowedGraph<G>;
    type Vertex = WindowEvalVertex<'a, G, CS, S>;
    type Edge = WindowEvalEdgeView<'a, G, CS, S>;
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
        Box::new(self.flat_map(move |it| it.explode()))
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

    fn layer_names(self) -> Self::IterType<BoxedIter<ArcStr>> {
        Box::new(self.map(|e| e.layer_names()))
    }

    fn history(self) -> Self::IterType<Vec<i64>> {
        Box::new(self.map(|e| e.history()))
    }

    fn start(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.start()))
    }

    fn start_date_time(self) -> Self::IterType<Option<chrono::NaiveDateTime>> {
        Box::new(self.map(|e| e.start_date_time()))
    }

    fn end(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.end()))
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

    fn at<T: IntoTime>(self, time: T) -> Self::IterType<WindowEvalEdgeView<'a, G, CS, S>> {
        let new_time = time.into_time();
        Box::new(self.map(move |e| e.at(new_time)))
    }

    fn window<T: IntoTime>(
        self,
        t_start: T,
        t_end: T,
    ) -> Self::IterType<WindowEvalEdgeView<'a, G, CS, S>> {
        let t_start = t_start.into_time();
        let t_end = t_end.into_time();
        Box::new(self.map(move |e| e.window(t_start, t_end)))
    }
}
