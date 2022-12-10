use std::ops::RangeBounds;


mod graph;
mod tvec;

trait TemporalGraphStorage {
    fn add_vertex(&mut self, v: u64, t: u64) -> &mut Self;

    /**
     * adds the edge in an idempotent manner
     * if src doesn't exit it's added at time t
     * if dst doesn't exit it's added at time t
     * 
     * both src and dst get an index at time t if they do exist
     */
    fn add_edge(&mut self, src: u64, dst: u64, t: u64) -> &mut Self;

    fn iter_vertices(&self) -> Box<dyn Iterator<Item = &u64> + '_>;

    fn enumerate_vs_at<R: RangeBounds<u64>>(&self, r:R) ->  Box<dyn Iterator<Item = u64> + '_>;

    fn outbound<R: RangeBounds<u64>>(&self, src: u64, r:R) ->  Box<dyn Iterator<Item = u64> + '_>;

    fn inbound<R: RangeBounds<u64>>(&self, dst: u64, r:R) ->  Box<dyn Iterator<Item = u64> + '_>;

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

    // #[test]
    // fn add_edge_at_time_t1() {

    //     let mut g = graph::TemporalGraph::default();

    //     g.add_vertex(9, 1);
    //     g.add_vertex(1, 2);

    //     // 9 and 1 are not visible at time 3
    //     let actual: Vec<u64> = g.enumerate_vs_at(3 .. 10).collect();
    //     assert_eq!(actual, vec![]);

    //     g.add_edge(9, 1, 3);

    //     println!("GRAPH {:?}", g);

    //     // 9 and 1 are now visible at time 3
    //     let actual: Vec<u64> = g.enumerate_vs_at(3 .. 10).collect();
    //     assert_eq!(actual, vec![9, 1]);

    //     // the outbound neighbours of 9 at time 0..2 is the empty set
    //     let actual: Vec<u64> = g.outbound(9, 0..2).collect();
    //     assert_eq!(actual, vec![]);

    //     // the outbound neighbours of 9 at time 0..2 are 1
    //     let actual: Vec<u64> = g.outbound(9, 0..3).collect();
    //     assert_eq!(actual, vec![1]);

    //     println!("GRAPH {:?}", g);
    // }
}
