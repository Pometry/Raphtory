use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

use genawaiter::rc::gen;
use genawaiter::yield_;

use crate::graph::{EdgeView, TemporalGraph};
use crate::{Direction, Prop};
use itertools::*;

#[derive(Debug)]
pub struct TEdge {
    src: u64,
    dst: u64,
    // edge_meta_id: AdjEdge,
    pub t: Option<i64>,
    is_remote: bool,
}

impl<'a> From<EdgeView<'a, TemporalGraph>> for TEdge {
    fn from(e: EdgeView<'a, TemporalGraph>) -> Self {
        Self {
            src: e.global_src(),
            dst: e.global_dst(),
            t: e.time(),
            is_remote: e.is_remote(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[repr(transparent)]
pub struct TemporalGraphPart(Arc<RwLock<TemporalGraph>>);

impl TemporalGraphPart {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<bincode::ErrorKind>> {
        // use BufReader for better performance
        let f = std::fs::File::open(path).unwrap();
        let mut reader = std::io::BufReader::new(f);
        bincode::deserialize_from(&mut reader)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<bincode::ErrorKind>> {
        // use BufWriter for better performance
        let f = std::fs::File::create(path).unwrap();
        let mut writer = std::io::BufWriter::new(f);
        bincode::serialize_into(&mut writer, self)
    }

    #[inline(always)]
    fn write_shard<A, F>(&self, f: F) -> A
    where
        F: Fn(&mut TemporalGraph) -> A,
    {
        let mut shard = self.0.write();
        f(&mut shard)
    }

    #[inline(always)]
    fn read_shard<A, F>(&self, f: F) -> A
    where
        F: Fn(&TemporalGraph) -> A,
    {
        let shard = self.0.read();
        f(&shard)
    }

    pub fn len(&self) -> usize {
        self.read_shard(|tg| tg.len())
    }

    pub fn out_edges_len(&self) -> usize {
        self.read_shard(|tg| tg.out_edges_len())
    }

    pub fn contains(&self, v: u64) -> bool {
        self.read_shard(|tg| tg.contains_vertex(v))
    }

    pub fn contains_window(&self, v: u64, t_start: i64, t_end: i64) -> bool {
        self.read_shard(|tg| tg.contains_vertex_window(&(t_start..t_end), v))
    }

    pub fn add_vertex(&self, v: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_vertex(v, t))
    }

    pub fn add_edge(&self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_edge_with_props(src, dst, t, props))
    }

    pub fn add_edge_remote_out(&self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_edge_remote_out(src, dst, t, props))
    }

    pub fn add_edge_remote_into(&self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_edge_remote_into(src, dst, t, props))
    }

    pub fn degree(&self, v: u64, d: Direction) -> usize {
        self.read_shard(|tg: &TemporalGraph| tg.degree(v, d))
    }

    pub fn degree_window(&self, v: u64, t_start: i64, t_end: i64, d: Direction) -> usize {
        self.read_shard(|tg: &TemporalGraph| tg.degree_window(v, &(t_start..t_end), d))
    }

    pub fn vertices(&self) -> impl Iterator<Item = u64> {
        let tg = self.clone();

        let vertices_iter = gen!({
            let g = tg.0.read();
            let iter = (*g).vertices().into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        vertices_iter.into_iter()
    }

    // TODO: check if there is any value in returning Vec<usize> vs just usize, what is the cost of the generator
    pub fn vertices_window(
        &self,
        t_start: i64,
        t_end: i64,
        chunk_size: usize,
    ) -> impl Iterator<Item = Vec<u64>> {
        let tg = self.clone();
        let vertices_iter = gen!({
            let g = tg.0.read();
            let chunks = (*g).vertices_window(t_start..t_end).chunks(chunk_size);
            let iter = chunks.into_iter().map(|chunk| chunk.collect::<Vec<_>>());
            for v_id in iter {
                yield_!(v_id)
            }
        });

        vertices_iter.into_iter()
    }

    pub fn neighbours(&self, v: u64, d: Direction) -> impl Iterator<Item = TEdge> {
        let tg = self.clone();
        let vertices_iter = gen!({
            let g = tg.0.read();
            let chunks = (*g).neighbours(v, d).map(|e| e.into());
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        vertices_iter.into_iter()
    }

    pub fn neighbours_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> impl Iterator<Item = TEdge> {
        let tg = self.clone();
        let vertices_iter = gen!({
            let g = tg.0.read();
            let chunks = (*g)
                .neighbours_window(v, &(t_start..t_end), d)
                .map(|e| e.into());
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        vertices_iter.into_iter()
    }

    pub fn neighbours_window_t(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> impl Iterator<Item = TEdge> {
        let tg = self.clone();
        let vertices_iter = gen!({
            let g = tg.0.read();
            let chunks = (*g)
                .neighbours_window_t(v, &(t_start..t_end), d)
                .map(|e| e.into());
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        vertices_iter.into_iter()
    }
}

#[cfg(test)]
mod temporal_graph_partition_test {
    use super::TemporalGraphPart;
    use crate::Direction;
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

        let g = TemporalGraphPart::default();

        let rand_index = rand::thread_rng().gen_range(0..vs.len());
        let rand_vertex = vs.get(rand_index).unwrap().0;

        for (v, t) in vs {
            g.add_vertex(v.into(), t.into(), &vec![]);
        }

        TestResult::from_bool(g.contains(rand_vertex))
    }

    #[test]
    fn shard_contains_vertex_window() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = TemporalGraphPart::default();

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        assert!(g.contains_window(1, -1, 7));
        assert!(!g.contains_window(2, 0, 1));
        assert!(g.contains_window(3, 0, 8));
    }

    #[quickcheck]
    fn add_vertex_to_shard_len_grows(vs: Vec<(u8, u8)>) {
        let g = TemporalGraphPart::default();

        let expected_len = vs.iter().map(|(v, _)| v).sorted().dedup().count();
        for (v, t) in vs {
            g.add_vertex(v.into(), t.into(), &vec![]);
        }

        assert_eq!(g.len(), expected_len)
    }

    #[test]
    fn shard_vertices() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = TemporalGraphPart::default();

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let actual = g.vertices().collect::<Vec<_>>();
        assert_eq!(actual, vec![1, 2, 3]);
    }

    // add one single vertex per interval
    // then go through each window
    // and select the vertices
    // should recover each inserted vertex exactly once
    #[quickcheck]
    fn iterate_vertex_windows(intervals: Intervals) {
        let g = TemporalGraphPart::default();

        for (v, (t_start, _)) in intervals.0.iter().enumerate() {
            g.add_vertex(v.try_into().unwrap(), *t_start, &vec![])
        }

        for (v, (t_start, t_end)) in intervals.0.iter().enumerate() {
            let vertex_window = g.vertices_window(*t_start, *t_end, 1);
            let iter = &mut vertex_window.into_iter().flatten();
            let v_actual = iter.next();
            assert_eq!(Some(v), v_actual);
            assert_eq!(None, iter.next()); // one vertex per interval
        }
    }

    #[test]
    fn get_shard_degree() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = TemporalGraphPart::default();

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = vec![(2, 3, 3), (2, 1, 2), (1, 1, 2)];
        let actual = (1..=3)
            .map(|i| {
                (
                    g.degree(i, Direction::IN),
                    g.degree(i, Direction::OUT),
                    g.degree(i, Direction::BOTH),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn get_shard_degree_window() {
        let g = TemporalGraphPart::default();

        g.add_vertex(100, 1, &vec![]);
        g.add_vertex(101, 2, &vec![]);
        g.add_vertex(102, 3, &vec![]);
        g.add_vertex(103, 4, &vec![]);
        g.add_vertex(104, 5, &vec![]);
        g.add_vertex(105, 5, &vec![]);

        g.add_edge(100, 101, 6, &vec![]);
        g.add_edge(100, 102, 7, &vec![]);
        g.add_edge(101, 103, 8, &vec![]);
        g.add_edge(102, 104, 9, &vec![]);
        g.add_edge(110, 104, 9, &vec![]);

        assert_eq!(g.degree_window(101, 0, i64::MAX, Direction::IN), 1);
        assert_eq!(g.degree_window(100, 0, i64::MAX, Direction::IN), 0);
        assert_eq!(g.degree_window(101, 0, 1, Direction::IN), 0);
        assert_eq!(g.degree_window(101, 10, 20, Direction::IN), 0);
        assert_eq!(g.degree_window(105, 0, i64::MAX, Direction::IN), 0);
        assert_eq!(g.degree_window(104, 0, i64::MAX, Direction::IN), 2);

        assert_eq!(g.degree_window(101, 0, i64::MAX, Direction::OUT), 1);
        assert_eq!(g.degree_window(103, 0, i64::MAX, Direction::OUT), 0);
        assert_eq!(g.degree_window(105, 0, i64::MAX, Direction::OUT), 0);
        assert_eq!(g.degree_window(101, 0, 1, Direction::OUT), 0);
        assert_eq!(g.degree_window(101, 10, 20, Direction::OUT), 0);
        assert_eq!(g.degree_window(100, 0, i64::MAX, Direction::OUT), 2);

        assert_eq!(g.degree_window(101, 0, i64::MAX, Direction::BOTH), 2);
        assert_eq!(g.degree_window(100, 0, i64::MAX, Direction::BOTH), 2);
        assert_eq!(g.degree_window(100, 0, 1, Direction::BOTH), 0);
        assert_eq!(g.degree_window(100, 10, 20, Direction::BOTH), 0);
        assert_eq!(g.degree_window(105, 0, i64::MAX, Direction::BOTH), 0);
    }

    #[test]
    fn get_shard_neighbours() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = TemporalGraphPart::default();

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = vec![(2, 3, 5), (2, 1, 3), (1, 1, 2)];
        let actual = (1..=3)
            .map(|i| {
                (
                    g.neighbours(i, Direction::IN).collect::<Vec<_>>().len(),
                    g.neighbours(i, Direction::OUT).collect::<Vec<_>>().len(),
                    g.neighbours(i, Direction::BOTH).collect::<Vec<_>>().len(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn get_shard_neighbours_window() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = TemporalGraphPart::default();

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = vec![(2, 3, 2), (1, 0, 0), (1, 0, 0)];
        let actual = (1..=3)
            .map(|i| {
                (
                    g.neighbours_window(i, -1, 7, Direction::IN)
                        .collect::<Vec<_>>()
                        .len(),
                    g.neighbours_window(i, 1, 7, Direction::OUT)
                        .collect::<Vec<_>>()
                        .len(),
                    g.neighbours_window(i, 0, 1, Direction::BOTH)
                        .collect::<Vec<_>>()
                        .len(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn get_shard_neighbours_window_t() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = TemporalGraphPart::default();

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let in_actual = (1..=3)
            .map(|i| {
                g.neighbours_window_t(i, -1, 7, Direction::IN)
                    .map(|e| e.t.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![-1, 0, 1], vec![1], vec![2]], in_actual);

        let out_actual = (1..=3)
            .map(|i| {
                g.neighbours_window_t(i, 1, 7, Direction::OUT)
                    .map(|e| e.t.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![1, 1, 2], vec![], vec![]], out_actual);

        let both_actual = (1..=3)
            .map(|i| {
                g.neighbours_window_t(i, 0, 1, Direction::BOTH)
                    .map(|e| e.t.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![0, 0], vec![], vec![]], both_actual);
    }
}
