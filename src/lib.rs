use std::{collections::BTreeMap, ops::RangeBounds};

use guardian::ArcMutexGuardian;
use roaring::RoaringTreemap;

mod graph;

struct TVertexView<R: RangeBounds<u64>> {
    r: R,
    guard: ArcMutexGuardian<BTreeMap<u64, RoaringTreemap>>,
}

impl<R: RangeBounds<u64> + Clone> TVertexView<R> {
    fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        let iter = self
            .guard
            .range(self.r.clone())
            .flat_map(|(_, vs)| vs.iter());
        Box::new(iter)
    }
}

trait TemporalGraphStorage {
    fn add_vertex(&self, v: u64, t: u64) -> &Self;

    fn enumerate_vertices(&self) -> Vec<u64>;

    fn enumerate_vs_at<R: RangeBounds<u64>>(&self, t: R) -> TVertexView<R>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_vertex_at_time_t1() {
        let g = graph::TemporalGraph::new_mem();

        g.add_vertex(9, 1);

        assert_eq!(g.enumerate_vertices(), vec![9])
    }

    #[test]
    fn add_vertex_at_time_t1_t2() {
        let g = graph::TemporalGraph::new_mem();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        let actual: Vec<u64> = g.enumerate_vs_at(0..=1).iter().collect();
        assert_eq!(actual, vec![9]);
        let actual: Vec<u64> = g.enumerate_vs_at(2..10).iter().collect();
        assert_eq!(actual, vec![1]);
        let actual: Vec<u64> = g.enumerate_vs_at(0..10).iter().collect();
        assert_eq!(actual, vec![9, 1]);
    }
}
