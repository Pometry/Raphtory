use std::ops::Range;

pub mod graph;
pub mod tvec;
pub mod tcell;
pub mod sortedvec;

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

    fn iter_vertices(&self) -> Box<dyn Iterator<Item = &u64> + '_>;

    fn enumerate_vs_at(&self, r:Range<u64>) ->  Box<dyn Iterator<Item = u64> + '_>;

    fn outbound(&self, src: u64, r:Range<u64>) ->  Box<dyn Iterator<Item = &u64> + '_>;

    fn inbound(&self, dst: u64, r:Range<u64>) ->  Box<dyn Iterator<Item = &u64> + '_>;

    fn len(&self) -> usize;

}

