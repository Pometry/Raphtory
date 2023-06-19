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
use crate::core::tgraph_shard::lock::MyReadGuard;
use crate::core::timeindex::TimeIndex;
use crate::core::tprop::TProp;
use crate::core::vertex::InputVertex;
use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::{Direction, Prop};
use genawaiter::sync::{gen, GenBoxed};
use genawaiter::yield_;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, Range};
use std::path::Path;
use std::sync::Arc;

pub enum LockedView<'a, T: ?Sized> {
    Locked(parking_lot::MappedRwLockReadGuard<'a, T>),
    Frozen(&'a T),
}

impl<'a, T> Deref for LockedView<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            LockedView::Locked(guard) => guard.deref(),
            LockedView::Frozen(r) => r,
        }
    }
}

mod lock {
    use crate::core::tgraph_shard::LockedView;
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

    impl<'a, T> MyReadGuard<'a, T> {
        pub fn map<U: ?Sized, F>(s: Self, f: F) -> LockedView<'a, U>
        where
            F: FnOnce(&T) -> &U,
        {
            LockedView::Locked(parking_lot::RwLockReadGuard::map(s.0, |t_opt| {
                f(t_opt.as_ref().expect("frozen"))
            }))
        }
    }

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
            self.0.deref()
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
        #[error("Failed to mutate graph property")]
        FailedToMutateGraphProperty { source: MutateGraphError },
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

    fn map<U, F>(&self, f: F) -> LockedView<'_, U>
    where
        F: FnOnce(&TemporalGraph) -> &U,
    {
        MyReadGuard::map(self.rc.read(), f)
    }

    pub fn local_vertex(&self, v: VertexRef) -> Option<LocalVertexRef> {
        self.read_shard(|tg| tg.local_vertex(v))
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
        self.write_shard(|tg| {
            tg.allocate_layer(id);
            Ok(())
        })
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

    pub fn is_empty(&self) -> bool {
        self.read_shard(|tg| tg.is_empty())
    }

    pub fn out_edges_len(&self, layer: Option<usize>) -> usize {
        self.read_shard(|tg| tg.out_edges_len(layer))
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

    pub fn add_property(&self, t: i64, props: &Vec<(String, Prop)>) -> Result<(), GraphError> {
        self.write_shard(|tg| {
            tg.add_property(t, props);
            Ok(())
        })
    }

    pub fn add_static_property(&self, props: &Vec<(String, Prop)>) -> Result<(), GraphError> {
        self.write_shard(|tg| {
            let res = tg.add_static_property(props);
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
        self.write_shard(|tg| {
            tg.add_edge_with_props(t, src, dst, props, layer);
            Ok(())
        })
    }

    pub fn delete_edge<T: InputVertex>(
        &self,
        t: i64,
        src: T,
        dst: T,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.write_shard(|tg| {
            tg.delete_edge(t, src, dst, layer);
            Ok(())
        })
    }

    pub fn add_edge_remote_out<T: InputVertex>(
        &self,
        t: i64,
        src: T,
        dst: T,
        props: &Vec<(String, Prop)>,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.write_shard(|tg| {
            tg.add_edge_remote_out(t, src, dst, props, layer);
            Ok(())
        })
    }

    pub fn delete_edge_remote_out<T: InputVertex>(
        &self,
        t: i64,
        src: T,
        dst: T,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.write_shard(|tg| {
            tg.delete_edge_remote_out(t, src, dst, layer);
            Ok(())
        })
    }

    pub fn add_edge_remote_into<T: InputVertex>(
        &self,
        t: i64,
        src: T,
        dst: T,
        props: &Vec<(String, Prop)>,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.write_shard(|tg| {
            tg.add_edge_remote_into(t, src, dst, props, layer);
            Ok(())
        })
    }

    pub fn delete_edge_remote_into<T: InputVertex>(
        &self,
        t: i64,
        src: T,
        dst: T,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.write_shard(|tg| {
            tg.delete_edge_remote_into(t, src, dst, layer);
            Ok(())
        })
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

    pub fn vertex(&self, v: u64) -> Option<LocalVertexRef> {
        self.read_shard(|tg| tg.vertex(v))
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

    pub fn static_vertex_prop(&self, v: LocalVertexRef, name: &str) -> Option<Prop> {
        self.read_shard(|tg| tg.static_vertex_prop(v, name))
    }

    pub fn static_prop(&self, name: &str) -> Option<Prop> {
        self.read_shard(|tg| tg.static_prop(name))
    }

    pub fn static_prop_names(&self) -> Vec<String> {
        self.read_shard(|tg| tg.static_prop_names())
    }

    pub fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.read_shard(|tg| tg.static_vertex_prop_names(v))
    }

    pub fn temporal_vertex_prop(&self, v: LocalVertexRef, name: &str) -> Option<LockedView<TProp>> {
        let has_prop = self.read_shard(|tg| tg.temporal_vertex_prop(v, name).is_some());
        has_prop
            .then(|| self.map(move |tg| tg.temporal_vertex_prop(v, name).expect("just checked")))
    }

    pub fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.read_shard(|tg| tg.temporal_vertex_prop_names(v))
    }

    pub fn temporal_prop_names(&self) -> Vec<String> {
        self.read_shard(|tg| tg.temporal_prop_names())
    }

    pub fn temporal_prop(&self, name: &str) -> Option<LockedView<TProp>> {
        let has_prop = self.read_shard(|tg| tg.temporal_prop(name).is_some());
        has_prop.then(|| self.map(move |tg| tg.temporal_prop(name).expect("just checked")))
    }

    pub fn vertex_additions(&self, v: LocalVertexRef) -> LockedView<TimeIndex> {
        self.map(|tg| tg.vertex_additions(v))
    }

    pub fn static_edge_prop(&self, e: EdgeRef, name: &str) -> Option<Prop> {
        self.read_shard(|tg| tg.static_edge_prop(e, name))
    }

    pub fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.read_shard(|tg| tg.static_edge_prop_names(e))
    }

    pub fn temporal_edge_prop(&self, e: EdgeRef, name: &str) -> Option<LockedView<TProp>> {
        self.read_shard(|tg| tg.temporal_edge_prop(e, name).is_some())
            .then(|| self.map(|tg| tg.temporal_edge_prop(e, name).expect("just checked")))
    }

    pub fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.read_shard(|tg| (tg.temporal_edge_prop_names(e)))
    }

    pub fn edge_additions(&self, e: EdgeRef) -> LockedView<TimeIndex> {
        self.map(|tg| tg.edge_additions(e))
    }

    pub fn edge_deletions(&self, e: EdgeRef) -> LockedView<TimeIndex> {
        self.map(|tg| tg.edge_deletions(e))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ImmutableTGraphShard<TemporalGraph> {
    pub(crate) rc: Arc<TemporalGraph>,
}

impl Clone for ImmutableTGraphShard<TemporalGraph> {
    fn clone(&self) -> Self {
        Self {
            rc: self.rc.clone(),
        }
    }
}

macro_rules! erase_lifetime {
    ($tgshard:expr, |$g:ident| { $f:expr }) => {
        let $g = $tgshard;
        let iter = GenBoxed::new_boxed(|co| async move {
            let iter = $f;
            for vv in iter {
                co.yield_(vv).await;
            }
        });
        return Box::new(iter.into_iter())
    };
}

impl ImmutableTGraphShard<TemporalGraph> {
    #[inline(always)]
    pub fn read_shard<'a, A, F>(&'a self, f: F) -> A
    where
        F: FnOnce(&'a TemporalGraph) -> A,
        A: 'a,
    {
        f(self.rc.as_ref())
    }

    pub fn unfreeze(self) -> Result<TGraphShard<TemporalGraph>, Arc<TemporalGraph>> {
        let tg = Arc::try_unwrap(self.rc)?;
        Ok(TGraphShard::new_from_tgraph(tg))
    }

    pub fn local_vertex(&self, v: VertexRef) -> Option<LocalVertexRef> {
        self.read_shard(|tg| tg.local_vertex(v))
    }

    pub fn vertex_id(&self, v: LocalVertexRef) -> u64 {
        self.read_shard(|tg| tg.vertex_id(v))
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

    pub fn is_empty(&self) -> bool {
        self.read_shard(|tg| tg.is_empty())
    }

    pub fn out_edges_len(&self, layer: Option<usize>) -> usize {
        self.read_shard(|tg| tg.out_edges_len(layer))
    }

    pub fn has_edge(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        self.read_shard(|tg| tg.has_edge(src, dst, layer))
    }

    pub fn has_vertex(&self, v: VertexRef) -> bool {
        self.read_shard(|tg| tg.has_vertex(v))
    }

    pub fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize {
        self.read_shard(|tg: &TemporalGraph| tg.degree(v, d, layer))
    }

    pub fn vertex(&self, v: u64) -> Option<LocalVertexRef> {
        self.read_shard(|tg| tg.vertex(v))
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        erase_lifetime!(self.rc.clone(), |tg| { tg.vertices() });
    }

    pub fn edge(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        self.read_shard(|tg| tg.edge(src, dst, layer))
    }

    pub fn vertex_edges(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        erase_lifetime!(self.rc.clone(), |tg| { tg.vertex_edges(v, d, layer) });
    }

    pub fn neighbours(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        erase_lifetime!(self.rc.clone(), |tg| { tg.neighbours(v, d, layer) });
    }

    pub fn static_vertex_prop(&self, v: LocalVertexRef, name: &str) -> Option<Prop> {
        self.read_shard(|tg| tg.static_vertex_prop(v, name))
    }

    pub fn static_prop(&self, name: &str) -> Option<Prop> {
        self.read_shard(|tg| tg.static_prop(name))
    }

    pub fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.read_shard(|tg| tg.static_vertex_prop_names(v))
    }

    pub fn static_prop_names(&self) -> Vec<String> {
        self.read_shard(|tg| tg.static_prop_names())
    }

    pub fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.read_shard(|tg| tg.temporal_vertex_prop_names(v))
    }

    pub fn temporal_prop_names(&self) -> Vec<String> {
        self.read_shard(|tg| tg.temporal_prop_names())
    }

    pub fn temporal_prop(&self, name: &str) -> Option<LockedView<TProp>> {
        self.rc.temporal_prop(name).map(LockedView::Frozen)
    }

    pub fn static_edge_prop(&self, e: EdgeRef, name: &str) -> Option<Prop> {
        self.read_shard(|tg| tg.static_edge_prop(e, name))
    }

    pub fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.read_shard(|tg| tg.static_edge_prop_names(e))
    }

    pub fn temporal_edge_prop(&self, e: EdgeRef, name: &str) -> Option<LockedView<TProp>> {
        self.read_shard(|tg| tg.temporal_edge_prop(e, name))
            .map(LockedView::Frozen)
    }

    pub fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.read_shard(|tg| (tg.temporal_edge_prop_names(e)))
    }
}

#[cfg(test)]
mod temporal_graph_partition_test {
    use crate::core::{tgraph_shard::TGraphShard, Direction};
    use itertools::Itertools;
    use quickcheck::TestResult;
    use rand::Rng;

    #[quickcheck]
    fn shard_contains_vertex(vs: Vec<(u64, i64)>) -> TestResult {
        if vs.is_empty() {
            return TestResult::discard();
        }

        let g = TGraphShard::new(0);

        let rand_index = rand::thread_rng().gen_range(0..vs.len());
        let rand_vertex = vs.get(rand_index).unwrap().0;

        for (v, t) in vs {
            g.add_vertex(t, v, &vec![]).expect("failed to add vertex");
        }

        TestResult::from_bool(g.has_vertex(rand_vertex.into()))
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
}
