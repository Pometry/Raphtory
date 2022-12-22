use std::ops::Range;

use graph::TemporalGraph;
use props::TProp;

pub mod bitset;
mod edge;
pub mod graph;
mod misc;
mod props;
pub mod sortedvec;
mod tcell;
mod tset;
mod tvec;

#[derive(Clone, Copy)]
pub enum Direction {
    OUT,
    IN,
    BOTH,
}

pub enum Prop {
    Str(String),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
}

pub struct VertexView<'a, G> {
    g_id: &'a u64,
    pid: usize,
    g: &'a G,
}

impl<'a> VertexView<'a, TemporalGraph> {
    pub fn global_id(&self) -> u64 {
        *self.g_id
    }

    pub fn outbound_degree(&self) -> usize {
        self.g.index[self.pid].out_degree()
    }

    pub fn inbound_degree(&self) -> usize {
        self.g.index[self.pid].in_degree()
    }

    // FIXME: all the functions using global ID need to be changed to use the physical ID instead
    pub fn outbound(&'a self) -> Box<dyn Iterator<Item = EdgeView<'a, TemporalGraph>> + 'a> {
        self.g.outbound(*self.g_id)
    }

    pub fn inbound(&'a self) -> Box<dyn Iterator<Item = EdgeView<'a, TemporalGraph>> + 'a> {
        self.g.inbound(*self.g_id)
    }

}

pub struct EdgeView<'a, G: Sized> {
    src_id: usize,
    dst_id: &'a usize,
    t: Option<&'a u64>,
    g: &'a G,
}

impl<'a> EdgeView<'a, TemporalGraph> {
    pub fn global_src(&self) -> u64 {
        *self.g.index[self.src_id].logical()
    }

    pub fn global_dst(&self) -> u64 {
        *self.g.index[*self.dst_id].logical()
    }

    pub fn props(&self, name: &'a str) -> Box<dyn Iterator<Item = (&'a u64, Prop)> + 'a>  {
        Box::new(std::iter::empty())
    }
}
