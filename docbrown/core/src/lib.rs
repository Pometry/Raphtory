#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;
mod bitset;
pub mod graph;
pub mod lsm;
mod adj;
mod misc;
mod props;
mod tadjset;
mod tcell;
pub mod tpartition;

/// Specify the direction of the neighbours
#[derive(Clone, Copy, PartialEq)]
pub enum Direction {
    OUT,
    IN,
    BOTH,
}

#[derive(Debug, PartialEq)]
pub enum Prop {
    Str(String),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
}

