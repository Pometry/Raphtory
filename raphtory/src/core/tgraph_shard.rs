//! A data structure for sharding a temporal graph.
//!
//! When a raphtory graph is created, the code will automatically shard the graph depending
//! on how many shards you set the graph to originally have when initializing it.
//!
//! For example, Graph::new(4) will create a graph with 4 shards.
//!
//! Each of these shards will be stored in a separate file, and will be loaded into memory when needed.
//!
//! Each shard will have its own set of vertex and edge data, and will be able to be queried independently.

use self::errors::GraphError;
use self::lock::OptionLock;
use crate::core::edge_ref::EdgeRef;
use crate::core::tgraph::TemporalGraph;
use crate::core::vertex::InputVertex;
use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::{Direction, Prop, Time};
use genawaiter::sync::{gen, GenBoxed};
use genawaiter::yield_;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

mod lock {
    use serde::{Deserialize, Serialize};
    use std::ops::{Deref, DerefMut};

    #[derive(Serialize, Deserialize, Debug)]
    #[repr(transparent)]
    pub struct OptionLock<T>(parking_lot::RwLock<Option<T>>);

    impl<T> OptionLock<T> {
        pub fn new(t: T) -> Self {
            Self(parking_lot::RwLock::new(Some(t)))
        }
    }

    #[repr(transparent)]
    pub struct MyReadGuard<'a, T>(parking_lot::RwLockReadGuard<'a, Option<T>>);

    #[repr(transparent)]
    pub struct MyWriteGuard<'a, T>(parking_lot::RwLockWriteGuard<'a, Option<T>>);

    impl<T> OptionLock<T> {
        pub fn read(&self) -> MyReadGuard<T> {
            MyReadGuard(self.0.read())
        }

        pub fn write(&self) -> MyWriteGuard<T> {
            MyWriteGuard(self.0.write())
        }
    }

    impl<T> Deref for MyReadGuard<'_, T> {
        type Target = Option<T>;
        fn deref(&self) -> &Self::Target {
            &self.0.deref()
        }
    }

    impl<T> Deref for MyWriteGuard<'_, T> {
        type Target = Option<T>;
        fn deref(&self) -> &Self::Target {
            self.0.deref()
        }
    }

    impl<T> DerefMut for MyWriteGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.0.deref_mut()
        }
    }

    unsafe impl<T> Send for MyReadGuard<'_, T> {}
}

pub mod errors {
    use crate::core::tgraph::errors::MutateGraphError;
    use crate::core::time::error::ParseTimeError;

    #[derive(thiserror::Error, Debug, PartialEq)]
    pub enum GraphError {
        #[error("Immutable graph reference already exists. You can access mutable graph apis only exclusively.")]
        IllegalGraphAccess,
        #[error("Incorrect property given.")]
        IncorrectPropertyType,
        #[error("Failed to mutate graph")]
        FailedToMutateGraph { source: MutateGraphError },
        #[error("Failed to parse time string")]
        ParseTime {
            #[from]
            source: ParseTimeError,
        },
        // wasm
        #[error("Vertex is not String or Number")]
        VertexIdNotStringOrNumber,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[repr(transparent)]
pub struct TGraphShard<TemporalGraph> {
    pub rc: Arc<OptionLock<TemporalGraph>>,
}

impl Clone for TGraphShard<TemporalGraph> {
    fn clone(&self) -> Self {
        Self {
            rc: self.rc.clone(),
        }
    }
}

impl TGraphShard<TemporalGraph> {
    pub fn new(id: usize) -> Self {
        Self {
            rc: Arc::new(OptionLock::new(TemporalGraph::new(id))),
        }
    }

    pub fn new_from_tgraph(g: TemporalGraph) -> Self {
        Self {
            rc: Arc::new(OptionLock::new(g)),
        }
    }

    pub fn load_from_file<P: AsRef<Path>>(
        path: P,
    ) -> Result<TGraphShard<TemporalGraph>, Box<bincode::ErrorKind>> {
        // use BufReader for better performance
        let f = std::fs::File::open(path)?;
        let mut reader = std::io::BufReader::new(f);
        bincode::deserialize_from(&mut reader)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<bincode::ErrorKind>> {
        // use BufWriter for better performance
        let f = std::fs::File::create(path)?;
        let mut writer = std::io::BufWriter::new(f);
        bincode::serialize_into(&mut writer, self)
    }

    #[inline(always)]
    fn write_shard<A, F>(&self, f: F) -> Result<A, GraphError>
    where
        F: FnOnce(&mut TemporalGraph) -> Result<A, GraphError>,
    {
        let mut binding = self.rc.write();
        let shard = binding.as_mut().ok_or(GraphError::IllegalGraphAccess)?;
        f(shard)
    }

    #[inline(always)]
    fn read_shard<A, F>(&self, f: F) -> A
    where
        F: FnOnce(&TemporalGraph) -> A,
    {
        let binding = self.rc.read();
        let shard = binding.as_ref().unwrap();
        f(shard)
    }

    pub fn local_vertex(&self, v: VertexRef) -> Option<LocalVertexRef> {
        self.read_shard(|tg| tg.local_vertex(v))
    }

    pub fn local_vertex_window(&self, v: VertexRef, w: Range<i64>) -> Option<LocalVertexRef> {
        self.read_shard(|tg| tg.local_vertex_window(v, w))
    }

    pub fn vertex_id(&self, v: LocalVertexRef) -> u64 {
        self.read_shard(|tg| tg.vertex_id(v))
    }

    pub fn freeze(&self) -> ImmutableTGraphShard<TemporalGraph> {
        let mut inner = self.rc.write();
        let g = inner.take().unwrap();
        ImmutableTGraphShard { rc: Arc::new(g) }
    }

    pub fn allocate_layer(&self, id: usize) -> Result<(), GraphError> {
        self.write_shard(|tg| Ok(tg.allocate_layer(id)))
    }

    pub fn earliest_time(&self) -> i64 {
        self.read_shard(|tg| tg.earliest_time)
    }

    pub fn latest_time(&self) -> i64 {
        self.read_shard(|tg| tg.latest_time)
    }

    pub fn len(&self) -> usize {
        self.read_shard(|tg| tg.len())
    }

    pub fn out_edges_len(&self, layer: Option<usize>) -> usize {
        self.read_shard(|tg| tg.out_edges_len(layer))
    }

    pub fn out_edges_len_window(&self, w: &Range<Time>, layer: Option<usize>) -> usize {
        self.read_shard(|tg| tg.out_edges_len_window(w, layer))
    }

    pub fn len_window(&self, w: Range<i64>) -> usize {
        self.read_shard(|tg| tg.len_window(&w))
    }

    pub fn has_edge(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        self.read_shard(|tg| tg.has_edge(src, dst, layer))
    }

    pub fn has_edge_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        w: Range<i64>,
        layer: usize,
    ) -> bool {
        self.read_shard(|tg| tg.has_edge_window(src, dst, &w, layer))
    }

    pub fn has_vertex(&self, v: VertexRef) -> bool {
        self.read_shard(|tg| tg.has_vertex(v))
    }

    pub fn has_vertex_window(&self, v: VertexRef, w: Range<i64>) -> bool {
        self.read_shard(|tg| tg.has_vertex_window(v, &w))
    }

    pub fn add_vertex<T: InputVertex>(
        &self,
        t: i64,
        v: T,
        props: &Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.write_shard(move |tg| {
            let res = tg.add_vertex_with_props(t, v, props);
            res.map_err(|e| GraphError::FailedToMutateGraph { source: e })
        })
    }

    pub fn add_vertex_properties(
        &self,
        v: u64,
        data: &Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.write_shard(|tg| {
            let res = tg.add_vertex_properties(v, data);
            res.map_err(|e| GraphError::FailedToMutateGraph { source: e })
        })
    }

    pub fn add_edge<T: InputVertex>(
        &self,
        t: i64,
        src: T,
        dst: T,
        props: &Vec<(String, Prop)>,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.write_shard(|tg| Ok(tg.add_edge_with_props(t, src, dst, props, layer)))
    }

    pub fn add_edge_remote_out<T: InputVertex>(
        &self,
        t: i64,
        src: T,
        dst: T,
        props: &Vec<(String, Prop)>,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.write_shard(|tg| Ok(tg.add_edge_remote_out(t, src, dst, props, layer)))
    }

    pub fn add_edge_remote_into<T: InputVertex>(
        &self,
        t: i64,
        src: T,
        dst: T,
        props: &Vec<(String, Prop)>,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.write_shard(|tg| Ok(tg.add_edge_remote_into(t, src, dst, props, layer)))
    }

    pub fn add_edge_properties(
        &self,
        src: u64,
        dst: u64,
        data: &Vec<(String, Prop)>,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.write_shard(|tg| {
            let res = tg.add_edge_properties(src, dst, data, layer);
            res.map_err(|e| GraphError::FailedToMutateGraph { source: e })
        })
    }

    pub fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize {
        self.read_shard(|tg: &TemporalGraph| tg.degree(v, d, layer))
    }

    pub fn degree_window(
        &self,
        v: LocalVertexRef,
        w: Range<i64>,
        d: Direction,
        layer: Option<usize>,
    ) -> usize {
        self.read_shard(|tg: &TemporalGraph| tg.degree_window(v, &w, d, layer))
    }

    pub fn vertex_earliest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.read_shard(|tg| tg.vertex_earliest_time(v))
    }

    pub fn vertex_earliest_time_window(&self, v: LocalVertexRef, w: Range<i64>) -> Option<i64> {
        self.read_shard(move |tg| tg.vertex_earliest_time_window(v, w))
    }

    pub fn vertex_latest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.read_shard(|tg| tg.vertex_latest_time(v))
    }

    pub fn vertex_latest_time_window(&self, v: LocalVertexRef, w: Range<i64>) -> Option<i64> {
        self.read_shard(|tg| tg.vertex_latest_time_window(v, w))
    }

    pub fn vertex(&self, v: u64) -> Option<LocalVertexRef> {
        self.read_shard(|tg| tg.vertex(v))
    }

    pub fn vertex_window(&self, v: u64, w: Range<i64>) -> Option<LocalVertexRef> {
        self.read_shard(|tg| tg.vertex_window(v, &w))
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        let tgshard = self.rc.clone();
        let iter: GenBoxed<LocalVertexRef> = GenBoxed::new_boxed(|co| async move {
            let binding = tgshard.read();
            if let Some(g) = binding.as_ref() {
                let iter = (*g).vertices();
                for vv in iter {
                    co.yield_(vv).await;
                }
            }
        });

        Box::new(iter.into_iter())
    }

    pub fn vertices_window(
        &self,
        w: Range<i64>,
    ) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        let tgshard = self.rc.clone();
        let iter: GenBoxed<LocalVertexRef> = GenBoxed::new_boxed(|co| async move {
            let binding = tgshard.read();
            if let Some(g) = binding.as_ref() {
                let iter = (*g).vertices_window(w);
                for vv in iter {
                    co.yield_(vv).await;
                }
            }
        });

        Box::new(iter.into_iter())
    }

    pub fn edge(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        self.read_shard(|tg| tg.edge(src, dst, layer))
    }

    pub fn edge_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        w: Range<i64>,
        layer: usize,
    ) -> Option<EdgeRef> {
        self.read_shard(|tg| tg.edge_window(src, dst, &w, layer))
    }

    pub fn vertex_edges(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let tgshard = self.rc.clone();
        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            let binding = tgshard.read();
            if let Some(g) = binding.as_ref() {
                let iter = (*g).vertex_edges(v, d, layer);
                for ev in iter {
                    co.yield_(ev).await;
                }
            }
        });

        Box::new(iter.into_iter())
    }

    pub fn vertex_edges_window(
        &self,
        v: LocalVertexRef,
        w: Range<i64>,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let tgshard = self.clone();
        let iter = gen!({
            let binding = tgshard.rc.read();
            if let Some(g) = binding.as_ref() {
                let chunks = (*g).vertex_edges_window(v, &w, d, layer);
                let iter = chunks.into_iter();
                for v_id in iter {
                    yield_!(v_id)
                }
            }
        });

        Box::new(iter.into_iter())
    }

    pub fn vertex_edges_window_t(
        &self,
        v: LocalVertexRef,
        w: Range<i64>,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let tgshard = self.clone();
        let iter = gen!({
            let mut binding = tgshard.rc.read();
            if let Some(g) = binding.as_ref() {
                let chunks = (*g).vertex_edges_window_t(v, &w, d, layer);
                let iter = chunks.into_iter();
                for v_id in iter {
                    yield_!(v_id)
                }
            }
        });

        Box::new(iter.into_iter())
    }

    pub fn neighbours(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        let tgshard = self.clone();
        let iter = gen!({
            let binding = tgshard.rc.read();
            if let Some(g) = binding.as_ref() {
                let chunks = (*g).neighbours(v, d, layer);
                let iter = chunks.into_iter();
                for v_id in iter {
                    yield_!(v_id)
                }
            }
        });

        Box::new(iter.into_iter())
    }

    pub fn neighbours_window(
        &self,
        v: LocalVertexRef,
        w: Range<i64>,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        let tgshard = self.clone();
        let iter = gen!({
            let binding = tgshard.rc.read();
            if let Some(g) = binding.as_ref() {
                let chunks = (*g).neighbours_window(v, &w, d, layer);
                let iter = chunks.into_iter();
                for v_id in iter {
                    yield_!(v_id)
                }
            }
        });

        Box::new(iter.into_iter())
    }

    pub fn static_vertex_prop(&self, v: LocalVertexRef, name: String) -> Option<Prop> {
        self.read_shard(|tg| tg.static_vertex_prop(v, &name))
    }

    pub fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.read_shard(|tg| tg.static_vertex_prop_names(v))
    }

    pub fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.read_shard(|tg| tg.temporal_vertex_prop_names(v))
    }

    pub fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: String) -> Vec<(i64, Prop)> {
        self.read_shard(|tg| tg.temporal_vertex_prop_vec(v, &name))
    }

    pub fn temporal_vertex_prop_vec_window(
        &self,
        v: LocalVertexRef,
        name: String,
        w: Range<i64>,
    ) -> Vec<(i64, Prop)> {
        self.read_shard(|tg| (tg.temporal_vertex_prop_vec_window(v, &name, &w)))
    }

    pub fn vertex_timestamps(&self, v: LocalVertexRef) -> Vec<i64> {
        self.read_shard(|tg| tg.vertex_timestamps(v))
    }

    pub fn vertex_timestamps_window(&self, v: LocalVertexRef, w: Range<i64>) -> Vec<i64> {
        self.read_shard(|tg| tg.vertex_timestamps_window(v, w))
    }

    pub fn temporal_vertex_props(&self, v: LocalVertexRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.read_shard(|tg| tg.temporal_vertex_props(v))
    }

    pub fn temporal_vertex_props_window(
        &self,
        v: LocalVertexRef,
        w: Range<i64>,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.read_shard(|tg| tg.temporal_vertex_props_window(v, &w))
    }
    pub fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop> {
        self.read_shard(|tg| tg.static_edge_prop(e, &name))
    }

    pub fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.read_shard(|tg| tg.static_edge_prop_names(e))
    }

    pub fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.read_shard(|tg| (tg.temporal_edge_prop_names(e)))
    }

    pub fn temporal_edge_prop_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)> {
        self.read_shard(|tg| tg.temporal_edge_prop_vec(e, &name))
    }

    pub fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        w: Range<i64>,
    ) -> Vec<(i64, Prop)> {
        self.read_shard(|tg| tg.temporal_edge_prop_vec_window(e, &name, w.clone()))
    }

    pub fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.read_shard(|tg| tg.temporal_edge_props(e))
    }

    pub fn edge_timestamps(&self, e: EdgeRef, window: Option<Range<i64>>) -> Vec<i64> {
        self.read_shard(|tg| tg.edge_timestamps(e, window))
    }

    pub fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.read_shard(|tg| tg.temporal_edge_props_window(e, w.clone()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ImmutableTGraphShard<TemporalGraph> {
    rc: Arc<TemporalGraph>,
}

impl Clone for ImmutableTGraphShard<TemporalGraph> {
    fn clone(&self) -> Self {
        Self {
            rc: self.rc.clone(),
        }
    }
}

impl ImmutableTGraphShard<TemporalGraph> {
    pub fn unfreeze(self) -> Result<TGraphShard<TemporalGraph>, Arc<TemporalGraph>> {
        let tg = Arc::try_unwrap(self.rc)?;
        Ok(TGraphShard::new_from_tgraph(tg))
    }

    pub fn earliest_time(&self) -> i64 {
        self.rc.earliest_time
    }

    pub fn latest_time(&self) -> i64 {
        self.rc.latest_time
    }

    pub fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize {
        self.rc.degree(v, d, layer)
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send + '_> {
        self.rc.vertices()
    }

    pub fn vertex_edges(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send + '_> {
        self.rc.vertex_edges(v, d, layer)
    }

    pub fn out_edges_len(&self, layer: Option<usize>) -> usize {
        self.rc.out_edges_len(layer)
    }
}

#[cfg(test)]
mod temporal_graph_partition_test {
    use crate::core::{tgraph_shard::TGraphShard, Direction};
    use itertools::Itertools;
    use quickcheck::{Arbitrary, TestResult};
    use rand::Rng;

    // non overlaping time intervals
    #[derive(Clone, Debug)]
    struct Intervals(Vec<(i64, i64)>);

    impl Arbitrary for Intervals {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let mut some_nums = Vec::<i64>::arbitrary(g);
            some_nums.sort();
            let intervals = some_nums
                .into_iter()
                .tuple_windows()
                .filter(|(a, b)| a != b)
                .collect_vec();
            Intervals(intervals)
        }
    }

    #[quickcheck]
    fn shard_contains_vertex(vs: Vec<(u64, i64)>) -> TestResult {
        if vs.is_empty() {
            return TestResult::discard();
        }

        let g = TGraphShard::new(0);

        let rand_index = rand::thread_rng().gen_range(0..vs.len());
        let rand_vertex = vs.get(rand_index).unwrap().0;

        for (v, t) in vs {
            g.add_vertex(t.into(), v as u64, &vec![])
                .expect("failed to add vertex");
        }

        TestResult::from_bool(g.has_vertex(rand_vertex.into()))
    }

    #[test]
    fn shard_contains_vertex_window() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = TGraphShard::new(0);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![], 0).unwrap();
        }

        assert!(g.has_vertex_window(1.into(), -1..7));
        assert!(!g.has_vertex_window(2.into(), 0..1));
        assert!(g.has_vertex_window(3.into(), 0..8));
    }

    #[quickcheck]
    fn add_vertex_to_shard_len_grows(vs: Vec<(u8, u8)>) {
        let g = TGraphShard::new(0);

        let expected_len = vs.iter().map(|(_, v)| v).sorted().dedup().count();
        for (t, v) in vs {
            g.add_vertex(t.into(), v as u64, &vec![])
                .expect("failed to add vertex");
        }

        assert_eq!(g.len(), expected_len)
    }

    #[test]
    fn shard_vertices() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = TGraphShard::new(0);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![], 0).unwrap();
        }

        let actual = g.vertices().map(|v| g.vertex_id(v)).collect::<Vec<_>>();
        assert_eq!(actual, vec![1, 2, 3]);
    }

    // add one single vertex per interval
    // then go through each window
    // and select the vertices
    // should recover each inserted vertex exactly once
    #[quickcheck]
    fn iterate_vertex_windows(intervals: Intervals) {
        let g = TGraphShard::new(0);

        for (v, (t_start, _)) in intervals.0.iter().enumerate() {
            g.add_vertex(*t_start, v as u64, &vec![]).unwrap()
        }

        for (v, (t_start, t_end)) in intervals.0.iter().enumerate() {
            let vertex_window = g
                .vertices_window(*t_start..*t_end)
                .map(|v| g.vertex_id(v))
                .collect::<Vec<_>>();
            let iter = &mut vertex_window.iter();
            let v_actual = iter.next();
            assert_eq!(Some(v as u64), Some(*v_actual.unwrap()));
            assert_eq!(None, iter.next()); // one vertex per interval
        }
    }

    #[test]
    fn get_shard_degree() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = TGraphShard::new(0);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![], 0).unwrap();
        }

        let expected = vec![(2, 3, 3), (2, 1, 2), (1, 1, 2)];
        let actual = (1..=3)
            .map(|i| {
                let i = g.vertex(i).unwrap();
                (
                    g.degree(i, Direction::IN, None),
                    g.degree(i, Direction::OUT, None),
                    g.degree(i, Direction::BOTH, None),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn get_shard_degree_window() {
        let g = TGraphShard::new(0);

        g.add_vertex(1, 100, &vec![]).expect("failed to add vertex");
        g.add_vertex(2, 101, &vec![]).expect("failed to add vertex");
        g.add_vertex(3, 102, &vec![]).expect("failed to add vertex");
        g.add_vertex(4, 103, &vec![]).expect("failed to add vertex");
        g.add_vertex(5, 104, &vec![]).expect("failed to add vertex");
        g.add_vertex(5, 105, &vec![]).expect("failed to add vertex");

        g.add_edge(6, 100, 101, &vec![], 0).unwrap();
        g.add_edge(7, 100, 102, &vec![], 0).unwrap();
        g.add_edge(8, 101, 103, &vec![], 0).unwrap();
        g.add_edge(9, 102, 104, &vec![], 0).unwrap();
        g.add_edge(9, 110, 104, &vec![], 0).unwrap();

        let v100 = g.vertex(100).unwrap();
        let v101 = g.vertex(101).unwrap();
        let v103 = g.vertex(103).unwrap();
        let v104 = g.vertex(104).unwrap();
        let v105 = g.vertex(105).unwrap();

        assert_eq!(
            g.degree_window(v101, 0i64..i64::MAX, Direction::IN, None),
            1
        );
        assert_eq!(g.degree_window(v100, 0..i64::MAX, Direction::IN, None), 0);
        assert_eq!(g.degree_window(v101, 0..1, Direction::IN, None), 0);
        assert_eq!(g.degree_window(v101, 10..20, Direction::IN, None), 0);
        assert_eq!(g.degree_window(v105, 0..i64::MAX, Direction::IN, None), 0);
        assert_eq!(g.degree_window(v104, 0..i64::MAX, Direction::IN, None), 2);
        assert_eq!(g.degree_window(v101, 0..i64::MAX, Direction::OUT, None), 1);
        assert_eq!(g.degree_window(v103, 0..i64::MAX, Direction::OUT, None), 0);
        assert_eq!(g.degree_window(v105, 0..i64::MAX, Direction::OUT, None), 0);
        assert_eq!(g.degree_window(v101, 0..1, Direction::OUT, None), 0);
        assert_eq!(g.degree_window(v101, 10..20, Direction::OUT, None), 0);
        assert_eq!(g.degree_window(v100, 0..i64::MAX, Direction::OUT, None), 2);
        assert_eq!(g.degree_window(v101, 0..i64::MAX, Direction::BOTH, None), 2);
        assert_eq!(g.degree_window(v100, 0..i64::MAX, Direction::BOTH, None), 2);
        assert_eq!(g.degree_window(v100, 0..1, Direction::BOTH, None), 0);
        assert_eq!(g.degree_window(v100, 10..20, Direction::BOTH, None), 0);
        assert_eq!(g.degree_window(v105, 0..i64::MAX, Direction::BOTH, None), 0);
    }

    #[test]
    fn get_shard_neighbours() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = TGraphShard::new(0);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![], 0).unwrap();
        }

        let expected = vec![(2, 3, 5), (2, 1, 3), (1, 1, 2)];
        let actual = (1..=3)
            .map(|i| {
                let i = g.vertex(i).unwrap();
                (
                    g.vertex_edges(i, Direction::IN, None).collect_vec().len(),
                    g.vertex_edges(i, Direction::OUT, None).collect_vec().len(),
                    g.vertex_edges(i, Direction::BOTH, None).collect_vec().len(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn get_shard_neighbours_window() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = TGraphShard::new(0);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![], 0).unwrap();
        }

        let expected = vec![(2, 3, 2), (1, 0, 0), (1, 0, 0)];
        let actual = (1..=3)
            .map(|i| {
                let i = g.vertex(i).unwrap();
                (
                    g.vertex_edges_window(i, -1..7, Direction::IN, None)
                        .collect_vec()
                        .len(),
                    g.vertex_edges_window(i, 1..7, Direction::OUT, None)
                        .collect_vec()
                        .len(),
                    g.vertex_edges_window(i, 0..1, Direction::BOTH, None)
                        .collect_vec()
                        .len(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn get_shard_neighbours_window_t() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = TGraphShard::new(0);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![], 0).unwrap();
        }

        let in_actual = (1..=3)
            .map(|i| {
                let i = g.vertex(i).unwrap();
                g.vertex_edges_window_t(i, -1..7, Direction::IN, None)
                    .map(|e| e.time().unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![0, 1, -1], vec![1], vec![2]], in_actual);

        let out_actual = (1..=3)
            .map(|i| {
                let i = g.vertex(i).unwrap();
                g.vertex_edges_window_t(i, 1..7, Direction::OUT, None)
                    .map(|e| e.time().unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![1, 1, 2], vec![], vec![]], out_actual);

        let both_actual = (1..=3)
            .map(|i| {
                let i = g.vertex(i).unwrap();
                g.vertex_edges_window_t(i, 0..1, Direction::BOTH, None)
                    .map(|e| e.time().unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![0, 0], vec![], vec![]], both_actual);
    }

    // struct Example<T>(Arc<parking_lot::RwLock<Vec<T>>>);

    // impl<T: Clone> Example<T> {
    //     fn iter(&self) -> impl Iterator<Item = T> {
    //         let tgshard = self.0.clone();
    //         let iter: GenBoxed<T> = GenBoxed::new_boxed(|co| async move {
    //             let g = tgshard.read();
    //             let iter = (*g).iter();
    //             for t in iter {
    //                 co.yield_(t.clone()).await;
    //             }
    //         });

    //         iter.into_iter()
    //     }
    // }

    // struct Example2<T>(Arc<MyLock<Vec<T>>>);

    // impl<T: Clone + std::marker::Send + std::marker::Sync + 'static> Example2<T> {
    //     fn iter(&self) -> impl Iterator<Item = T> {
    //         let tgshard = self.0.clone();
    //         let iter: GenBoxed<T> = GenBoxed::new_boxed(|co| async move {
    //             let g = tgshard.read();
    //             let iter = (*g).iter();
    //             for t in iter {
    //                 co.yield_(t.clone()).await;
    //             }
    //         });

    //         iter.into_iter()
    //     }
    // }
}
