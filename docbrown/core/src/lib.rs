#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

mod bitset;
pub mod graph;
pub mod lsm;
mod sorted_vec_map;
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

#[derive(Debug, PartialEq, Clone)]
pub enum Prop {
    Str(String),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
}

