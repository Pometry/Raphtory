use std::ops::Range;

pub mod graph;
pub mod tvec;
pub mod tcell;
pub mod sortedvec;
pub mod bitset;

pub trait TemporalGraphStorage {
    fn add_vertex(&mut self, v: u64, t: u64) -> &mut Self;

    /**
     * adds the edge in an idempotent manner
     * if src doesn't exit it's added at time t
     * if dst doesn't exit it's added at time t
     * 
     * both src and dst get an index at time t if they do exist
     */
    fn add_edge(&mut self, src: u64, dst: u64, t: u64) -> &mut Self;

    fn iter_vs(&self) -> Box<dyn Iterator<Item = &u64> + '_>;

    fn iter_vs_window(&self, r:Range<u64>) ->  Box<dyn Iterator<Item = u64> + '_>;

    fn outbound(&self, src: u64) ->  Box<dyn Iterator<Item = &u64> + '_>;

    fn inbound(&self, dst: u64) ->  Box<dyn Iterator<Item = &u64> + '_>;

    fn outbound_window(&self, src: u64, r:Range<u64>) ->  Box<dyn Iterator<Item = &u64> + '_>;

    fn inbound_window(&self, dst: u64, r:Range<u64>) ->  Box<dyn Iterator<Item = &u64> + '_>;

    fn outbound_window_t(&self, src: u64, r:Range<u64>) ->  Box<dyn Iterator<Item = (&u64, &u64)> + '_>;

    fn inbound_window_t(&self, dst: u64, r:Range<u64>) ->  Box<dyn Iterator<Item = (&u64, &u64)> + '_>;

    fn len(&self) -> usize;

    fn outbound_degree(&self, src: u64) -> usize;
    fn inbound_degree(&self, dst: u64) -> usize;

    fn outbound_degree_t(&self, dst: u64, r:Range<u64>) -> usize;
    fn inbound_degree_t(&self, dst: u64, r:Range<u64>) -> usize;

}

