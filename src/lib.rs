use std::ops::RangeBounds;

mod graph;

trait TemporalGraphStorage {
    fn add_vertex(&self, v: u64, t: u64) -> &Self;

    fn enumerate_vertices(&self) -> Vec<u64>;

    fn enumerate_vs_at<R: RangeBounds<u64>>(&self, t: R) -> Vec<u64>;
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

        assert_eq!(g.enumerate_vs_at(0..=1), vec![9]);
        assert_eq!(g.enumerate_vs_at(2..10), vec![1]);
        assert_eq!(g.enumerate_vs_at(0..10), vec![9,1]);

    }
}
