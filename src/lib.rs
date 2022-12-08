use std::ops::RangeBounds;


mod graph;

trait TemporalGraphStorage {
    fn add_vertex(&mut self, v: u64, t: u64) -> &mut Self;

    fn iter_vertices(&self) -> Box<dyn Iterator<Item = &u64> + '_>;

    fn enumerate_vs_at<R: RangeBounds<u64>>(&self, r:R) ->  Box<dyn Iterator<Item = u64> + '_>;

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_vertex_at_time_t1() {
        let mut g = graph::TemporalGraph::default();

        g.add_vertex(9, 1);

        assert_eq!(g.iter_vertices().collect::<Vec<&u64>>(), vec![&9])
    }

    #[test]
    fn add_vertex_at_time_t1_t2() {
        let mut g = graph::TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        println!("GRAPH {:?}", g);

        let actual: Vec<u64> = g.enumerate_vs_at(0..=1).collect();
        assert_eq!(actual, vec![9]);
        let actual: Vec<u64> = g.enumerate_vs_at(2..10).collect();
        assert_eq!(actual, vec![1]);
        let actual: Vec<u64> = g.enumerate_vs_at(0..10).collect();
        assert_eq!(actual, vec![9, 1]);
    }
}
