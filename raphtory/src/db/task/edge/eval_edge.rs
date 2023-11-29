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
            view::{
                internal::{InternalLayerOps, OneHopFilter},
                *,
            },
        },
        graph::{edge::EdgeView, views::window_graph::WindowedGraph},
        task::{
            task_state::Local2,
            vertex::{eval_vertex::EvalVertexView, eval_vertex_state::EVState},
        },
    },
};

use std::{cell::RefCell, rc::Rc};

pub struct EvalEdgeView<'graph, 'a, G, GH, CS: Clone, S> {
    ss: usize,
    edge: EdgeView<&'graph G, GH>,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    local_state_prev: &'graph Local2<'a, S>,
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > EvalEdgeView<'graph, 'a, G, GH, CS, S>
{
    pub(crate) fn new(
        ss: usize,
        edge: EdgeView<&'graph G, GH>,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
        local_state_prev: &'graph Local2<'a, S>,
    ) -> Self {
        Self {
            ss,
            edge,
            vertex_state,
            local_state_prev,
        }
    }

    pub(crate) fn new_edge(&self, edge: EdgeView<&'graph G, GH>) -> Self {
        Self::new(
            self.ss,
            edge,
            self.vertex_state.clone(),
            self.local_state_prev,
        )
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > EdgeViewInternalOps<'graph> for EvalEdgeView<'graph, 'a, G, GH, CS, S>
{
    type BaseGraph = &'graph G;
    type Graph = GH;
    type EList = Box<dyn Iterator<Item = Self> + 'graph>;
    type Neighbour = EvalVertexView<'graph, 'a, G, S, &'graph G, CS>;

    fn graph(&self) -> &GH {
        self.edge.graph()
    }

    fn eref(&self) -> EdgeRef {
        self.edge.eref()
    }

    fn new_vertex(&self, v: VID) -> EvalVertexView<'graph, 'a, G, S, &'graph G, CS> {
        let vertex = self.edge.new_vertex(v);
        EvalVertexView::new_from_vertex(
            self.ss,
            vertex,
            None,
            self.local_state_prev,
            self.vertex_state.clone(),
        )
    }

    fn new_edge(&self, e: EdgeRef) -> Self {
        let ev = self.edge.new_edge(e);
        EvalEdgeView::new(
            self.ss,
            ev,
            self.vertex_state.clone(),
            self.local_state_prev,
        )
    }

    fn internal_explode(&self) -> Self::EList {
        let base_edge = self.clone();
        Box::new(self.edge.explode().map(move |e| base_edge.new_edge(e)))
    }

    fn internal_explode_layers(&self) -> Self::EList {
        let base_edge = self.clone();
        Box::new(
            self.edge
                .explode_layers()
                .map(move |e| base_edge.new_edge(e)),
        )
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > ConstPropertiesOps for EvalEdgeView<'graph, 'a, G, GH, CS, S>
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

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > Clone for EvalEdgeView<'graph, 'a, G, GH, CS, S>
{
    fn clone(&self) -> Self {
        Self {
            ss: self.ss,
            edge: self.edge.clone(),
            vertex_state: self.vertex_state.clone(),
            local_state_prev: self.local_state_prev,
        }
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > TemporalPropertyViewOps for EvalEdgeView<'graph, 'a, G, GH, CS, S>
{
    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.edge.temporal_history(id)
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.edge.temporal_values(id)
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > TemporalPropertiesOps for EvalEdgeView<'graph, 'a, G, GH, CS, S>
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

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > InternalLayerOps for EvalEdgeView<'graph, 'a, G, GH, CS, S>
{
    fn layer_ids(&self) -> LayerIds {
        self.edge.layer_ids()
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.edge.layer_ids_from_names(key)
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > OneHopFilter<'graph> for EvalEdgeView<'graph, 'a, G, GH, CS, S>
{
    type Graph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = EvalEdgeView<'graph, 'a, G, GHH, CS, S>;

    fn current_filter(&self) -> &Self::Graph {
        &self.edge.graph
    }
    fn one_hop_filtered<GHH: GraphViewOps<'graph>>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let edge = self.edge.one_hop_filtered(filtered_graph);
        EvalEdgeView::new(
            self.ss,
            edge,
            self.vertex_state.clone(),
            self.local_state_prev,
        )
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
    > EdgeListOps<'graph>
    for Box<dyn Iterator<Item = EvalEdgeView<'graph, 'a, G, GH, CS, S>> + 'graph>
{
    type Edge = EvalEdgeView<'graph, 'a, G, GH, CS, S>;
    type ValueType<T> = T;
    type VList = Box<dyn Iterator<Item = EvalVertexView<'graph, 'a, G, S, &'graph G, CS>> + 'graph>;
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
    ) -> Self::IterType<EvalEdgeView<'graph, 'a, G, WindowedGraph<GH>, CS, S>> {
        let new_time = time.into_time();
        Box::new(self.map(move |e| e.at(new_time)))
    }

    fn window<T: IntoTime>(
        self,
        start: T,
        end: T,
    ) -> Self::IterType<EvalEdgeView<'graph, 'a, G, WindowedGraph<GH>, CS, S>> {
        let start = start.into_time();
        let end = end.into_time();
        Box::new(self.map(move |e| e.window(start, end)))
    }
}

impl<
        'graph,
        'a: 'graph,
        G: GraphViewOps<'graph>,
        S,
        CS: ComputeState + 'a,
        GH: GraphViewOps<'graph>,
    > VertexListOps<'graph>
    for Box<(dyn Iterator<Item = EvalVertexView<'graph, 'a, G, S, GH, CS>> + 'graph)>
{
    type Vertex = EvalVertexView<'graph, 'a, G, S, GH, CS>;
    type Neighbour = EvalVertexView<'graph, 'a, G, S, &'graph G, CS>;
    type Edge = EvalEdgeView<'graph, 'a, G, GH, CS, S>;
    type IterType<T: 'graph> = Box<dyn Iterator<Item = T> + 'graph>;
    type ValueType<T: 'graph> = T;

    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|v| v.earliest_time()))
    }

    fn latest_time(self) -> Self::IterType<Option<i64>> {
        Box::new(self.map(|v| v.latest_time()))
    }

    fn window(
        self,
        start: i64,
        end: i64,
    ) -> Self::IterType<<Self::Vertex as TimeOps<'graph>>::WindowedViewType> {
        Box::new(self.map(move |v| v.window(start, end)))
    }

    fn at(self, end: i64) -> Self::IterType<<Self::Vertex as TimeOps<'graph>>::WindowedViewType> {
        Box::new(self.map(move |v| v.at(end)))
    }

    fn id(self) -> Self::IterType<u64> {
        Box::new(self.map(|v| v.id()))
    }

    fn name(self) -> Self::IterType<String> {
        Box::new(self.map(|v| v.name()))
    }

    fn properties(
        self,
    ) -> Self::IterType<Properties<<Self::Vertex as VertexViewOps<'graph>>::PropType>> {
        Box::new(self.map(|v| v.properties()))
    }

    fn history(self) -> Self::IterType<Vec<i64>> {
        Box::new(self.map(|v| v.history()))
    }

    fn degree(self) -> Self::IterType<usize> {
        Box::new(self.map(|v| v.degree()))
    }

    fn in_degree(self) -> Self::IterType<usize> {
        Box::new(self.map(|v| v.in_degree()))
    }

    fn out_degree(self) -> Self::IterType<usize> {
        Box::new(self.map(|v| v.out_degree()))
    }

    fn edges(self) -> Self::IterType<Self::Edge> {
        Box::new(self.flat_map(|v| v.edges()))
    }

    fn in_edges(self) -> Self::IterType<Self::Edge> {
        Box::new(self.flat_map(|v| v.in_edges()))
    }

    fn out_edges(self) -> Self::IterType<Self::Edge> {
        Box::new(self.flat_map(|v| v.out_edges()))
    }

    fn neighbours(self) -> Self::IterType<Self::Neighbour> {
        Box::new(self.flat_map(|v| v.neighbours()))
    }

    fn in_neighbours(self) -> Self::IterType<Self::Neighbour> {
        Box::new(self.flat_map(|v| v.in_neighbours()))
    }

    fn out_neighbours(self) -> Self::IterType<Self::Neighbour> {
        Box::new(self.flat_map(|v| v.out_neighbours()))
    }
}
