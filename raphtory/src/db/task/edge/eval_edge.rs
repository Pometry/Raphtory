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
            view::{internal::OneHopFilter, *},
        },
        graph::{edge::EdgeView, views::window_graph::WindowedGraph},
        task::{
            task_state::Local2,
            vertex::{eval_vertex::EvalVertexView, eval_vertex_state::EVState},
        },
    },
    vectors::Document::Edge,
};
use std::{cell::RefCell, iter, marker::PhantomData, rc::Rc};

pub struct EvalEdgeView<'a, G, CS: Clone, S> {
    ss: usize,
    edge: EdgeView<G>,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    local_state_prev: &'a Local2<'a, S>,
    _s: PhantomData<S>,
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> EvalEdgeView<'a, G, CS, S> {
    pub(crate) fn new(
        ss: usize,
        edge: EdgeView<G>,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
        local_state_prev: &'a Local2<'a, S>,
    ) -> Self {
        Self {
            ss,
            edge,
            vertex_state,
            local_state_prev,
            _s: PhantomData,
        }
    }

    pub(crate) fn new_edge(&self, edge: EdgeView<G>) -> Self {
        Self::new(
            self.ss,
            edge,
            self.vertex_state.clone(),
            self.local_state_prev,
        )
    }

    fn layer_ids(&self) -> LayerIds {
        self.edge.layer_ids()
    }
}

impl<'graph, G: GraphViewOps + 'graph, CS: ComputeState, S: 'static>
    EdgeViewInternalOps<'graph, G, EvalVertexView<'graph, G, S, G, CS>>
    for EvalEdgeView<'graph, G, CS, S>
{
    fn graph(&self) -> G {
        self.edge.graph()
    }

    fn eref(&self) -> EdgeRef {
        self.edge.eref()
    }

    fn new_vertex(&self, v: VID) -> EvalVertexView<'graph, G, S, G, CS> {
        EvalVertexView::new_local(
            self.ss,
            v,
            self.graph(),
            None,
            self.local_state_prev,
            self.vertex_state.clone(),
        )
    }

    fn new_edge(&self, e: EdgeRef) -> Self {
        let ev = EdgeView::new(self.graph(), e);
        EvalEdgeView::new(
            self.ss,
            ev,
            self.vertex_state.clone(),
            self.local_state_prev,
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> ConstPropertiesOps
    for EvalEdgeView<'a, G, CS, S>
{
    fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.edge.get_const_prop_id(name)
    }

    fn get_const_prop_name(&self, id: usize) -> ArcStr {
        self.edge.get_const_prop_name(id)
    }

    fn const_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        self.edge.const_prop_ids()
    }

    fn get_const_prop(&self, prop_id: usize) -> Option<Prop> {
        self.edge.get_const_prop(prop_id)
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> Clone for EvalEdgeView<'a, G, CS, S> {
    fn clone(&self) -> Self {
        Self {
            ss: self.ss,
            edge: self.edge.clone(),
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
        self.edge.temporal_history(id)
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.edge.temporal_values(id)
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> TemporalPropertiesOps
    for EvalEdgeView<'a, G, CS, S>
{
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.edge.get_temporal_prop_id(name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.edge.get_temporal_prop_name(id)
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        self.edge.temporal_prop_ids()
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> OneHopFilter<'a>
    for EvalEdgeView<'a, G, CS, S>
{
    type Graph = G;
    type Filtered<GH: GraphViewOps> = EvalEdgeView<'a, GH, CS, S>;

    fn current_filter(&self) -> &Self::Graph {
        &self.edge.graph
    }
    fn one_hop_filtered<GH: GraphViewOps>(&self, filtered_graph: GH) -> Self::Filtered<GH> {
        let edge = self.edge.one_hop_filtered(filtered_graph);
        EvalEdgeView::new(
            self.ss,
            edge,
            self.vertex_state.clone(),
            self.local_state_prev,
        )
    }
}

impl<'graph, G: GraphViewOps + 'graph, CS: ComputeState, S: 'static> EdgeViewOps<'graph>
    for EvalEdgeView<'graph, G, CS, S>
{
    type Graph = G;
    type Vertex = EvalVertexView<'graph, G, S, G, CS>;
    type EList = Box<dyn Iterator<Item = Self> + 'graph>;

    fn explode(&self) -> Self::EList {
        let base_edge = self.clone();
        Box::new(self.edge.explode().map(move |e| base_edge.new_edge(e)))
    }

    fn explode_layers(&self) -> Self::EList {
        let base_edge = self.clone();
        Box::new(
            self.edge
                .explode_layers()
                .map(move |e| base_edge.new_edge(e)),
        )
    }
}

impl<'graph, G: GraphViewOps + 'graph, CS: ComputeState, S: 'static> EdgeListOps<'graph>
    for Box<dyn Iterator<Item = EvalEdgeView<'graph, G, CS, S>> + 'graph>
{
    type Graph = G;
    type Vertex = EvalVertexView<'graph, G, S, G, CS>;
    type Edge = EvalEdgeView<'graph, G, CS, S>;
    type ValueType<T> = T;
    type VList = Box<dyn Iterator<Item = Self::Vertex> + 'graph>;
    type IterType<T> = Box<dyn Iterator<Item = T> + 'graph>;

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

    fn earliest_date_time(self) -> Self::IterType<Option<chrono::NaiveDateTime>> {
        Box::new(self.map(|e| e.earliest_date_time()))
    }

    fn latest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|e| e.latest_time()))
    }

    fn latest_date_time(self) -> Self::IterType<Option<chrono::NaiveDateTime>> {
        Box::new(self.map(|e| e.latest_date_time()))
    }

    fn date_time(self) -> Self::IterType<Option<chrono::NaiveDateTime>> {
        Box::new(self.map(|e| e.date_time()))
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

    fn at<T: IntoTime>(
        self,
        time: T,
    ) -> Self::IterType<EvalEdgeView<'graph, WindowedGraph<G>, CS, S>> {
        let new_time = time.into_time();
        Box::new(self.map(move |e| e.at(new_time)))
    }

    fn window<T: IntoTime>(
        self,
        start: T,
        end: T,
    ) -> Self::IterType<EvalEdgeView<'graph, WindowedGraph<G>, CS, S>> {
        let start = start.into_time();
        let end = end.into_time();
        Box::new(self.map(move |e| e.window(start, end)))
    }
}

impl<'graph, G: GraphViewOps + 'graph, GH: GraphViewOps + 'graph, CS: ComputeState, S: 'static>
    VertexListOps<'graph>
    for Box<(dyn Iterator<Item = EvalVertexView<'graph, G, S, GH, CS>> + 'graph)>
{
    type Vertex = EvalVertexView<'graph, G, S, GH, CS>;
    type Edge = EvalEdgeView<'graph, G, CS, S>;
    type IterType<T: 'graph> = Box<dyn Iterator<Item = T> + 'graph>;
    type ValueType<T: 'graph> = T;

    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        todo!()
    }

    fn latest_time(self) -> Self::IterType<Option<i64>> {
        todo!()
    }

    fn window(
        self,
        start: i64,
        end: i64,
    ) -> Self::IterType<<Self::Vertex as TimeOps>::WindowedViewType> {
        todo!()
    }

    fn at(self, end: i64) -> Self::IterType<<Self::Vertex as TimeOps>::WindowedViewType> {
        todo!()
    }

    fn id(self) -> Self::IterType<u64> {
        todo!()
    }

    fn name(self) -> Self::IterType<String> {
        todo!()
    }

    fn properties(
        self,
    ) -> Self::IterType<Properties<<Self::Vertex as VertexViewOps<'graph>>::PropType>> {
        todo!()
    }

    fn history(self) -> Self::IterType<Vec<i64>> {
        todo!()
    }

    fn degree(self) -> Self::IterType<usize> {
        todo!()
    }

    fn in_degree(self) -> Self::IterType<usize> {
        todo!()
    }

    fn out_degree(self) -> Self::IterType<usize> {
        todo!()
    }

    fn edges(self) -> Self::IterType<Self::Edge> {
        todo!()
    }

    fn in_edges(self) -> Self::IterType<Self::Edge> {
        todo!()
    }

    fn out_edges(self) -> Self::IterType<Self::Edge> {
        todo!()
    }

    fn neighbours(self) -> Self::IterType<Self::Vertex> {
        todo!()
    }

    fn in_neighbours(self) -> Self::IterType<Self::Vertex> {
        todo!()
    }

    fn out_neighbours(self) -> Self::IterType<Self::Vertex> {
        todo!()
    }
}
