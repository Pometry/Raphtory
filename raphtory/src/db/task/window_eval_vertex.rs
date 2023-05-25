use std::{cell::RefCell, collections::HashMap, marker::PhantomData, rc::Rc};

use crate::{
    core::{
        state::compute_state::ComputeState, time::IntoTime, vertex_ref::LocalVertexRef, Direction,
        Prop,
    },
    db::{
        edge::EdgeView,
        path::{Operations, PathFromVertex},
        view_api::{BoxedIter, GraphViewOps, TimeOps, VertexViewOps},
    },
};

use super::{eval_edge::EvalEdgeView, eval_vertex_state::EVState, task_state::Local2};

pub struct WindowEvalVertex<'a, G: GraphViewOps, CS: ComputeState, S: 'static> {
    ss: usize,
    vertex: LocalVertexRef,
    pub(crate) graph: &'a G,
    local_state: Option<&'a mut S>,
    local_state_prev: &'a Local2<'a, S>,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    t_start: i64,
    t_end: i64,
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> WindowEvalVertex<'a, G, CS, S> {
    pub(crate) fn new(
        ss: usize,
        vertex: LocalVertexRef,
        graph: &'a G,
        local_state: Option<&'a mut S>,
        local_state_prev: &'a Local2<'a, S>,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
        t_start: i64,
        t_end: i64,
    ) -> Self {
        WindowEvalVertex {
            ss,
            vertex,
            graph,
            local_state,
            local_state_prev,
            vertex_state,
            t_start,
            t_end,
        }
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> TimeOps for WindowEvalVertex<'a, G, CS, S> {
    type WindowedViewType = WindowEvalVertex<'a, G, CS, S>;

    fn start(&self) -> Option<i64> {
        Some(self.t_start)
    }

    fn end(&self) -> Option<i64> {
        Some(self.t_end)
    }

    fn window<T: IntoTime>(&self, t_start: T, t_end: T) -> Self::WindowedViewType {
        WindowEvalVertex {
            ss: self.ss,
            vertex: self.vertex.clone(),
            graph: self.graph,
            local_state: None,
            local_state_prev: self.local_state_prev,
            vertex_state: self.vertex_state.clone(),
            t_start: t_start.into_time().max(self.t_start),
            t_end: t_end.into_time().min(self.t_end),
        }
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> VertexViewOps
    for WindowEvalVertex<'a, G, CS, S>
{
    type Graph = G;
    type ValueType<T> = T;
    type PathType<'b> = WindowEvalPathFromVertex<'a, G, CS, S> where Self: 'b;
    type EList = Box<dyn Iterator<Item = EvalEdgeView<'a, G, CS, S>> + 'a>;

    fn id(&self) -> Self::ValueType<u64> {
        self.graph.vertex_id(self.vertex)
    }

    fn name(&self) -> Self::ValueType<String> {
        self.graph.vertex_name(self.vertex)
    }

    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.graph
            .vertex_earliest_time_window(self.vertex, self.t_start, self.t_end)
    }

    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.graph
            .vertex_latest_time_window(self.vertex, self.t_start, self.t_end)
    }

    fn property(
        &self,
        name: String,
        include_static: bool,
    ) -> Self::ValueType<Option<crate::core::Prop>> {
        let props = self.property_history(name.clone());
        match props.last() {
            None => {
                if include_static {
                    self.graph.static_vertex_prop(self.vertex, name)
                } else {
                    None
                }
            }
            Some((_, prop)) => Some(prop.clone()),
        }
    }

    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.graph
            .vertex_timestamps_window(self.vertex, self.t_start, self.t_end)
    }

    fn property_history(&self, name: String) -> Self::ValueType<Vec<(i64, crate::core::Prop)>> {
        self.graph
            .temporal_vertex_prop_vec_window(self.vertex, name, self.t_start, self.t_end)
    }

    fn properties(
        &self,
        include_static: bool,
    ) -> Self::ValueType<std::collections::HashMap<String, crate::core::Prop>> {
        let mut props: HashMap<String, Prop> = self
            .property_histories()
            .iter()
            .map(|(key, values)| (key.clone(), values.last().unwrap().1.clone()))
            .collect();

        if include_static {
            for prop_name in self.graph.static_vertex_prop_names(self.vertex) {
                if let Some(prop) = self
                    .graph
                    .static_vertex_prop(self.vertex, prop_name.clone())
                {
                    props.insert(prop_name, prop);
                }
            }
        }
        props
    }

    fn property_histories(
        &self,
    ) -> Self::ValueType<std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>>> {
        self.graph
            .temporal_vertex_props_window(self.vertex, self.t_start, self.t_end)
    }

    fn property_names(&self, include_static: bool) -> Self::ValueType<Vec<String>> {
        let mut names: Vec<String> = self.graph.temporal_vertex_prop_names(self.vertex);
        if include_static {
            names.extend(self.graph.static_vertex_prop_names(self.vertex))
        }
        names
    }

    fn has_property(&self, name: String, include_static: bool) -> Self::ValueType<bool> {
        (!self.property_history(name.clone()).is_empty())
            || (include_static
                && self
                    .graph
                    .static_vertex_prop_names(self.vertex)
                    .contains(&name))
    }

    fn has_static_property(&self, name: String) -> Self::ValueType<bool> {
        self.graph
            .static_vertex_prop_names(self.vertex)
            .contains(&name)
    }

    fn static_property(&self, name: String) -> Self::ValueType<Option<crate::core::Prop>> {
        self.graph.static_vertex_prop(self.vertex, name)
    }

    fn degree(&self) -> Self::ValueType<usize> {
        let dir = Direction::BOTH;
        self.graph
            .degree_window(self.vertex, self.t_start, self.t_end, dir, None)
    }

    fn in_degree(&self) -> Self::ValueType<usize> {
        let dir = Direction::IN;
        self.graph
            .degree_window(self.vertex, self.t_start, self.t_end, dir, None)
    }

    fn out_degree(&self) -> Self::ValueType<usize> {
        let dir = Direction::OUT;
        self.graph
            .degree_window(self.vertex, self.t_start, self.t_end, dir, None)
    }

    fn edges(&self) -> Self::EList {
        let ss = self.ss;
        let vertex_state = self.vertex_state.clone();
        let local = self.local_state_prev;
        let graph = self.graph;
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, self.t_start, self.t_end, Direction::BOTH, None)
                .map(move |e| EvalEdgeView::new_(ss, e, graph, local, vertex_state.clone())),
        )
    }

    fn in_edges(&self) -> Self::EList {
        let ss = self.ss;
        let vertex_state = self.vertex_state.clone();
        let local = self.local_state_prev;
        let graph = self.graph;
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, self.t_start, self.t_end, Direction::IN, None)
                .map(move |e| EvalEdgeView::new_(ss, e, graph, local, vertex_state.clone())),
        )
    }

    fn out_edges(&self) -> Self::EList {
        let ss = self.ss;
        let vertex_state = self.vertex_state.clone();
        let local = self.local_state_prev;
        let graph = self.graph;
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, self.t_start, self.t_end, Direction::OUT, None)
                .map(move |e| EvalEdgeView::new_(ss, e, graph, local, vertex_state.clone())),
        )
    }

    fn neighbours(&self) -> Self::PathType<'_> {
        let neighbours = PathFromVertex::new(
            self.graph.clone(),
            self.vertex,
            Operations::NeighboursWindow {
                dir: Direction::BOTH,
                t_start: self.t_start,
                t_end: self.t_end,
            },
        );

        WindowEvalPathFromVertex::new_from_path_and_vertex(neighbours, self)
    }

    fn in_neighbours(&self) -> Self::PathType<'_> {
        let neighbours = PathFromVertex::new(
            self.graph.clone(),
            self.vertex,
            Operations::NeighboursWindow {
                dir: Direction::IN,
                t_start: self.t_start,
                t_end: self.t_end,
            },
        );

        WindowEvalPathFromVertex::new_from_path_and_vertex(neighbours, self)
    }

    fn out_neighbours(&self) -> Self::PathType<'_> {
        let neighbours = PathFromVertex::new(
            self.graph.clone(),
            self.vertex,
            Operations::NeighboursWindow {
                dir: Direction::OUT,
                t_start: self.t_start,
                t_end: self.t_end,
            },
        );

        WindowEvalPathFromVertex::new_from_path_and_vertex(neighbours, self)
    }
}

pub struct WindowEvalPathFromVertex<'a, G: GraphViewOps, CS: ComputeState, S> {
    path: PathFromVertex<G>,
    ss: usize,
    g: &'a G,
    vertex_state: Rc<RefCell<EVState<'a, CS>>>,
    local_state_prev: &'a Local2<'a, S>,
    _s: PhantomData<S>,
    t_start: i64,
    t_end: i64,
}
impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> WindowEvalPathFromVertex<'a, G, CS, S> {
    fn update_path(&self, path: PathFromVertex<G>) -> Self {
        WindowEvalPathFromVertex {
            path,
            ss: self.ss,
            g: self.g,
            vertex_state: self.vertex_state.clone(),
            local_state_prev: self.local_state_prev,
            t_start: self.t_start,
            t_end: self.t_end,
            _s: PhantomData,
        }
    }

    pub(crate) fn new_from_path_and_vertex(
        path: PathFromVertex<G>,
        vertex: &WindowEvalVertex<'a, G, CS, S>,
    ) -> WindowEvalPathFromVertex<'a, G, CS, S> {
        WindowEvalPathFromVertex {
            path,
            ss: vertex.ss,
            g: vertex.graph,
            vertex_state: vertex.vertex_state.clone(),
            local_state_prev: vertex.local_state_prev,
            _s: PhantomData,
            t_start: vertex.t_start,
            t_end: vertex.t_end,
        }
    }

    pub(crate) fn new(
        path: PathFromVertex<G>,
        ss: usize,
        g: &'a G,
        vertex_state: Rc<RefCell<EVState<'a, CS>>>,
        local_state_prev: &'a Local2<'a, S>,
        t_start: i64,
        t_end: i64,
    ) -> Self {
        WindowEvalPathFromVertex {
            path,
            ss,
            g,
            vertex_state,
            local_state_prev,
            _s: PhantomData,
            t_start,
            t_end,
        }
    }

    fn edges_internal(
        &self,
        dir: Direction,
    ) -> Box<dyn Iterator<Item = EvalEdgeView<'a, G, CS, S>> + 'a> {
        let ss = self.ss;
        let g = self.g;
        let vertex_state = self.vertex_state.clone();
        let local_state_prev = self.local_state_prev;
        let t_start = self.t_start;
        let t_end = self.t_end;

        let iter = self
            .path
            .iter_refs()
            .flat_map(move |v_ref| {
                let local_ref = g.localise_vertex_unchecked(v_ref);
                g.vertex_edges_window(local_ref, t_start, t_end, dir, None)
            })
            .map(move |e_ref| {
                EvalEdgeView::new(ss, e_ref, g, vertex_state.clone(), local_state_prev)
            });

        Box::new(iter)
    }

    fn degree(&self, dir: Direction) -> Box<dyn Iterator<Item = usize> + 'a> {
        let g = self.g;
        let t_start = self.t_start;
        let t_end = self.t_end;

        let iter = self.path.iter_refs().map(move |v_ref| {
            let local_ref = g.localise_vertex_unchecked(v_ref);
            g.degree_window(local_ref, t_start, t_end, dir, None)
        });

        Box::new(iter)
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> TimeOps
    for WindowEvalPathFromVertex<'a, G, CS, S>
{
    type WindowedViewType = WindowEvalPathFromVertex<'a, G, CS, S>;

    fn start(&self) -> Option<i64> {
        Some(self.t_start)
    }

    fn end(&self) -> Option<i64> {
        Some(self.t_end)
    }

    fn window<T: IntoTime>(&self, t_start: T, t_end: T) -> Self::WindowedViewType {
        WindowEvalPathFromVertex::new(
            self.path.clone(),
            self.ss,
            self.g,
            self.vertex_state.clone(),
            self.local_state_prev,
            t_start.into_time().max(self.t_start),
            t_end.into_time().min(self.t_end),
        )
    }
}

impl<'a, G: GraphViewOps, CS: ComputeState, S: 'static> VertexViewOps
    for WindowEvalPathFromVertex<'a, G, CS, S>
{
    type Graph = G;

    type ValueType<T> = Box<dyn Iterator<Item = T> + 'a>;

    type PathType<'b> = WindowEvalPathFromVertex<'a, G, CS, S> where Self: 'b;

    type EList = Box<dyn Iterator<Item = EvalEdgeView<'a, G, CS, S>> + 'a>;

    fn id(&self) -> Self::ValueType<u64> {
        self.path.id()
    }

    fn name(&self) -> Self::ValueType<String> {
        self.path.name()
    }

    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.path.earliest_time()
    }

    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.path.latest_time()
    }

    fn property(
        &self,
        name: String,
        include_static: bool,
    ) -> Self::ValueType<Option<crate::core::Prop>> {
        let g = self.g;
        let t_start = self.t_start;
        let t_end = self.t_end;

        let iter = self.path.iter_refs().map(move |v_ref| {
            let local_ref = g.localise_vertex_unchecked(v_ref);
            let props = g.temporal_vertex_prop_vec_window(local_ref, name.clone(), t_start, t_end);
            match props.last() {
                None => {
                    if include_static {
                        g.static_vertex_prop(local_ref, name.clone())
                    } else {
                        None
                    }
                }
                Some((_, prop)) => Some(prop.clone()),
            }
        });
        Box::new(iter)
    }

    fn history(&self) -> Self::ValueType<Vec<i64>> {
        let g = self.g;
        let t_start = self.t_start;
        let t_end = self.t_end;

        let iter = self.path.iter_refs().map(move |v_ref| {
            let local_ref = g.localise_vertex_unchecked(v_ref);
            g.vertex_timestamps_window(local_ref, t_start, t_end)
        });

        Box::new(iter)
    }

    fn property_history(&self, name: String) -> Self::ValueType<Vec<(i64, crate::core::Prop)>> {
        let g = self.g;
        let t_start = self.t_start;
        let t_end = self.t_end;

        let iter = self.path.iter_refs().map(move |v_ref| {
            let local_ref = g.localise_vertex_unchecked(v_ref);
            g.temporal_vertex_prop_vec_window(local_ref, name.clone(), t_start, t_end)
        });

        Box::new(iter)
    }

    fn properties(
        &self,
        include_static: bool,
    ) -> Self::ValueType<std::collections::HashMap<String, crate::core::Prop>> {
        self.path.properties(include_static)
    }

    fn property_histories(
        &self,
    ) -> Self::ValueType<std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>>> {
        let g = self.g;
        let t_start = self.t_start;
        let t_end = self.t_end;

        let iter = self.path.iter_refs().map(move |v_ref| {
            let local_ref = g.localise_vertex_unchecked(v_ref);
            g.temporal_vertex_props_window(local_ref, t_start, t_end)
        });

        Box::new(iter)
    }

    fn property_names(&self, include_static: bool) -> Self::ValueType<Vec<String>> {
        self.path.property_names(include_static)
    }

    fn has_property(&self, name: String, include_static: bool) -> Self::ValueType<bool> {
        let g = self.g;
        let t_start = self.t_start;
        let t_end = self.t_end;
        let iter = self.path.iter_refs().map(move |v_ref| {
            let local_ref = g.localise_vertex_unchecked(v_ref);
            let props = g.temporal_vertex_prop_vec_window(local_ref, name.clone(), t_start, t_end);

            !props.is_empty()
                || (include_static && g.static_vertex_prop_names(local_ref).contains(&name))
        });

        Box::new(iter)
    }

    fn has_static_property(&self, name: String) -> Self::ValueType<bool> {
        self.path.has_static_property(name)
    }

    fn static_property(&self, name: String) -> Self::ValueType<Option<crate::core::Prop>> {
        self.path.static_property(name)
    }

    fn degree(&self) -> Self::ValueType<usize> {
        self.degree(Direction::BOTH)
    }

    fn in_degree(&self) -> Self::ValueType<usize> {
        self.degree(Direction::IN)
    }

    fn out_degree(&self) -> Self::ValueType<usize> {
        self.degree(Direction::OUT)
    }

    fn edges(&self) -> Self::EList {
        self.edges_internal(Direction::BOTH)
    }

    fn in_edges(&self) -> Self::EList {
        self.edges_internal(Direction::IN)
    }

    fn out_edges(&self) -> Self::EList {
        self.edges_internal(Direction::OUT)
    }

    fn neighbours(&self) -> Self::PathType<'_> {
        self.update_path(
            self.path
                .neighbours_window(Direction::BOTH, self.t_start, self.t_end),
        )
    }

    fn in_neighbours(&self) -> Self::PathType<'_> {
        self.update_path(
            self.path
                .neighbours_window(Direction::IN, self.t_start, self.t_end),
        )
    }

    fn out_neighbours(&self) -> Self::PathType<'_> {
        self.update_path(
            self.path
                .neighbours_window(Direction::OUT, self.t_start, self.t_end),
        )
    }
}
