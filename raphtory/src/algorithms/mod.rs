//! Implementations of various graph algorithms that can be run on the graph.
//!
//! The algorithms are grouped into modules based on the type of graph they can be run on.
//!
//! To run an algorithm simply import the module and call the function.
//!
//! # Examples
//!
//! ```rust
//! use raphtory::algorithms::degree::{average_degree};
//! use raphtory::db::graph::Graph;
//!  
//!  let g = Graph::new(1);
//!  let vs = vec![
//!      (1, 1, 2),
//!      (2, 1, 3),
//!      (3, 2, 1),
//!      (4, 3, 2),
//!      (5, 1, 4),
//!      (6, 4, 5),
//!   ];
//!
//!  for (t, src, dst) in &vs {
//!    g.add_edge(*t, *src, *dst, &vec![], None);
//!  };
//! println!("average_degree: {:?}", average_degree(&g));
//! ```

pub mod clustering_coefficient;
pub mod connected_components;
pub mod degree;
pub mod directed_graph_density;
pub mod hits;
pub mod local_clustering_coefficient;
pub mod local_triangle_count;
pub mod pagerank;
pub mod reciprocity;
pub mod triangle_count;
pub mod triplet_count;
pub mod generic_taint;

use num_traits::{abs, Bounded, Zero};
use std::ops::{Add, AddAssign, Div, Mul, Range, Sub};

#[derive(PartialEq, PartialOrd, Copy, Clone, Debug)]
struct MulF32(f32);

const MUL_F32_ZERO: MulF32 = MulF32(1.0f32);

impl Zero for MulF32 {
    fn zero() -> Self {
        MUL_F32_ZERO
    }

    fn set_zero(&mut self) {
        *self = Zero::zero();
    }

    fn is_zero(&self) -> bool {
        *self == MUL_F32_ZERO
    }
}

impl Add for MulF32 {
    type Output = MulF32;

    fn add(self, rhs: Self) -> Self::Output {
        MulF32(self.0 + rhs.0)
    }
}

impl AddAssign for MulF32 {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0 + rhs.0
    }
}

impl Sub for MulF32 {
    type Output = MulF32;

    fn sub(self, rhs: Self) -> Self::Output {
        MulF32(self.0 - rhs.0)
    }
}

impl Div for MulF32 {
    type Output = MulF32;

    fn div(self, rhs: Self) -> Self::Output {
        MulF32(self.0 / rhs.0)
    }
}

impl Mul for MulF32 {
    type Output = MulF32;

    fn mul(self, rhs: Self) -> Self::Output {
        MulF32(self.0 * rhs.0)
    }
}

impl Bounded for MulF32 {
    fn min_value() -> Self {
        MulF32(f32::MIN)
    }

    fn max_value() -> Self {
        MulF32(f32::MAX)
    }
}

#[derive(PartialEq, PartialOrd, Copy, Clone, Debug)]
struct SumF32(f32);

impl Zero for SumF32 {
    fn zero() -> Self {
        SumF32(0.0f32)
    }

    fn set_zero(&mut self) {
        *self = Zero::zero();
    }

    fn is_zero(&self) -> bool {
        *self == SumF32(1.0f32)
    }
}

impl Add for SumF32 {
    type Output = SumF32;

    fn add(self, rhs: Self) -> Self::Output {
        SumF32(self.0 + rhs.0)
    }
}

impl AddAssign for SumF32 {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0 + rhs.0
    }
}

impl Sub for SumF32 {
    type Output = SumF32;

    fn sub(self, rhs: Self) -> Self::Output {
        SumF32(self.0 - rhs.0)
    }
}

impl Div for SumF32 {
    type Output = SumF32;

    fn div(self, rhs: Self) -> Self::Output {
        SumF32(self.0 / rhs.0)
    }
}

impl Mul for SumF32 {
    type Output = SumF32;

    fn mul(self, rhs: Self) -> Self::Output {
        SumF32(self.0 * rhs.0)
    }
}

impl Bounded for SumF32 {
    fn min_value() -> Self {
        SumF32(f32::MIN)
    }

    fn max_value() -> Self {
        SumF32(f32::MAX)
    }
}

#[derive(PartialEq, PartialOrd, Copy, Clone, Debug)]
struct Bool(bool);

impl Zero for Bool {
    fn zero() -> Self {
        Bool(false)
    }

    fn set_zero(&mut self) {
        *self = Zero::zero();
    }

    fn is_zero(&self) -> bool {
        *self == Bool(false)
    }
}

impl Add for Bool {
    type Output = Bool;

    fn add(self, rhs: Self) -> Self::Output {
        rhs
    }
}
