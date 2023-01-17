use futures::Future;
use parking_lot::RwLock;
use std::sync::Arc;

use genawaiter::rc::gen;
use genawaiter::yield_;

use crate::graph::TemporalGraph;
use crate::Prop;

#[derive(Clone, Debug, Default)]
pub struct TemporalGraphPart(Arc<RwLock<TemporalGraph>>);

impl TemporalGraphPart {
    pub fn add_vertex(&self, t: u64, v: u64, props: Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_vertex(v, t))
    }

    pub fn vertices_window(
        &self,
        t_start: u64,
        t_end: u64,
    ) -> genawaiter::rc::Gen<usize, (), impl Future<Output = ()>> {
        let tg = self.clone();
        let vertices_iter = gen!({
            let g = tg.0.read();
            for v_id in (*g).vertices_window(t_start..t_end) {
                yield_!(v_id)
            }
        });

        vertices_iter
    }

    pub fn len(&self) -> usize {
        self.read_shard(|tg| tg.len())
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
}

#[cfg(test)]
mod temporal_graph_partition_test {
    use super::TemporalGraphPart;
    use itertools::Itertools;
    use quickcheck::Arbitrary;

    // non overlaping time intervals
    #[derive(Clone, Debug)]
    struct Intervals(Vec<(u64, u64)>);

    impl Arbitrary for Intervals {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let mut some_nums = Vec::<u64>::arbitrary(g);
            some_nums.sort();
            let intervals = some_nums.into_iter().tuple_windows().filter(|(a, b)| a != b).collect_vec();
            Intervals(intervals)
        }
    }

    #[quickcheck]
    fn add_vertex_to_graph_len_grows(vs: Vec<(u8, u8)>) {
        let g = TemporalGraphPart::default();

        let expected_len = vs.iter().map(|(v, _)| v).sorted().dedup().count();
        for (v, t) in vs {
            g.add_vertex(t.into(), v.into(), vec![]);
        }

        assert_eq!(g.len(), expected_len)
    }

    // add one single vertex per interval
    // then go through each window
    // and select the vertices
    // should recover each inserted vertex exactly once
    #[quickcheck]
    fn iterate_vertex_windows(intervals: Intervals) {
        let g = TemporalGraphPart::default();

        for (v, (t_start, _)) in intervals.0.iter().enumerate() {
            g.add_vertex(*t_start, v.try_into().unwrap(), vec![])
        }

       for (v, (t_start, t_end)) in intervals.0.iter().enumerate() {
           let vertex_window = g.vertices_window(*t_start, *t_end);
           let iter = &mut vertex_window.into_iter();
           let v_actual = iter.next();
           assert_eq!(Some(v), v_actual);
           assert_eq!(None, iter.next()); // one vertex per interval
        }
    }
}
