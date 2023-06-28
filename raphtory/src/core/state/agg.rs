//! Computing aggregates over temporal graph data.

use std::{
    marker::PhantomData,
    ops::{AddAssign, Div},
};

use num_traits::{Bounded, Zero};

use super::StateType;

pub trait Accumulator<A, IN, OUT>: Send + Sync + 'static {
    fn zero() -> A;

    fn add0(a1: &mut A, a: IN);

    fn combine(a1: &mut A, a2: &A);

    fn finish(a: &A) -> OUT;
}

pub struct InitOneF32();
impl Init<f32> for InitOneF32 {
    fn init() -> f32 {
        1.0f32
    }
}

pub struct InitOneF64();
impl Init<f64> for InitOneF64 {
    fn init() -> f64 {
        1.0f64
    }
}

#[derive(Clone, Debug, Copy)]
pub struct AndDef();

impl Accumulator<bool, bool, bool> for AndDef {
    fn zero() -> bool {
        true
    }

    fn add0(a1: &mut bool, a: bool) {
        *a1 = *a1 && a;
    }

    fn combine(a1: &mut bool, a2: &bool) {
        Self::add0(a1, *a2);
    }

    fn finish(a: &bool) -> bool {
        *a
    }
}

#[derive(Clone, Debug, Copy)]
pub struct OrDef();

impl Accumulator<bool, bool, bool> for OrDef {
    fn zero() -> bool {
        false
    }

    fn add0(a1: &mut bool, a: bool) {
        *a1 = *a1 || a;
    }

    fn combine(a1: &mut bool, a2: &bool) {
        Self::add0(a1, *a2);
    }

    fn finish(a: &bool) -> bool {
        *a
    }
}

#[derive(Clone, Debug, Copy)]
pub struct MinDef<A: StateType + Bounded + PartialOrd> {
    _marker: PhantomData<A>,
}

impl<A> Accumulator<A, A, A> for MinDef<A>
where
    A: StateType + Bounded + PartialOrd,
{
    fn zero() -> A {
        A::max_value()
    }

    fn add0(a1: &mut A, a: A) {
        if a < *a1 {
            *a1 = a;
        }
    }

    fn combine(a1: &mut A, a2: &A) {
        Self::add0(a1, a2.clone());
    }

    fn finish(a: &A) -> A {
        a.clone()
    }
}

#[derive(Clone, Debug, Copy)]
pub struct MaxDef<A: StateType + Bounded + PartialOrd> {
    _marker: PhantomData<A>,
}

impl<A> Accumulator<A, A, A> for MaxDef<A>
where
    A: StateType + Bounded + PartialOrd,
{
    fn zero() -> A {
        A::min_value()
    }

    fn add0(a1: &mut A, a: A) {
        if a > *a1 {
            *a1 = a.clone();
        }
    }

    fn combine(a1: &mut A, a2: &A) {
        Self::add0(a1, a2.clone());
    }

    fn finish(a: &A) -> A {
        a.clone()
    }
}

#[derive(Clone, Debug, Copy)]
pub struct SumDef<A: StateType + Zero + AddAssign<A>> {
    _marker: PhantomData<A>,
}

impl<A> Accumulator<A, A, A> for SumDef<A>
where
    A: StateType + Zero + AddAssign<A>,
{
    fn zero() -> A {
        A::zero()
    }

    fn add0(a1: &mut A, a: A) {
        *a1 += a;
    }

    fn combine(a1: &mut A, a2: &A) {
        Self::add0(a1, a2.clone());
    }

    fn finish(a: &A) -> A {
        a.clone()
    }
}

#[derive(Clone, Debug, Copy)]
pub struct ValDef<A: StateType + Zero> {
    _marker: PhantomData<A>,
}

impl<A> Accumulator<A, A, A> for ValDef<A>
where
    A: StateType + Zero,
{
    fn zero() -> A {
        A::zero()
    }

    fn add0(a1: &mut A, a: A) {
        *a1 = a;
    }

    fn combine(a1: &mut A, a2: &A) {
        Self::add0(a1, a2.clone());
    }

    fn finish(a: &A) -> A {
        a.clone()
    }
}

#[derive(Clone, Debug, Copy)]
pub struct ArrConst<A, const N: usize>(pub [A; N]);

impl<A: Accumulator<A, A, A>, const N: usize> Accumulator<[A; N], (usize, A), [A; N]>
    for ArrConst<A, N>
where
    A: StateType + Copy,
{
    fn zero() -> [A; N] {
        [A::zero(); N]
    }

    fn add0(a1: &mut [A; N], a: (usize, A)) {
        let (i, a) = a;
        if i < N {
            A::add0(&mut a1[i], a);
        }
    }

    fn combine(a1: &mut [A; N], a2: &[A; N]) {
        for (into, from) in a1.iter_mut().zip(a2.iter()) {
            A::combine(into, from)
        }
    }

    fn finish(a: &[A; N]) -> [A; N] {
        a.clone()
    }
}

#[derive(Clone, Debug, Copy)]
pub struct AvgDef<A: StateType + Zero + AddAssign<A> + TryFrom<usize> + Div<A, Output = A>> {
    _marker: PhantomData<A>,
}

impl<A> Accumulator<(A, usize), A, A> for AvgDef<A>
where
    A: StateType + Zero + AddAssign<A> + TryFrom<usize> + Div<A, Output = A>,
    <A as TryFrom<usize>>::Error: std::fmt::Debug,
{
    fn zero() -> (A, usize) {
        (A::zero(), 0)
    }

    fn add0(a1: &mut (A, usize), a: A) {
        a1.0 += a;
        a1.1 += 1;
    }

    fn combine(a1: &mut (A, usize), a2: &(A, usize)) {
        a1.0 += a2.0.clone();
        a1.1 += a2.1;
    }

    fn finish(a: &(A, usize)) -> A {
        let count: A = A::try_from(a.1).expect("failed to convert usize to A");
        a.0.clone() / count
    }
}
// use to replace the default zero for A
pub trait Init<A> {
    fn init() -> A;
}

pub struct InitAcc<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, I: Init<A>> {
    _marker: PhantomData<(A, IN, OUT, ACC, I)>,
}

pub type InitAcc1<A, ACC, I> = InitAcc<A, A, A, ACC, I>;

// these are safe as long as InitAcc does not have ANY internal state
unsafe impl<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, I: Init<A>> Sync
    for InitAcc<A, IN, OUT, ACC, I>
{
}
unsafe impl<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, I: Init<A>> Send
    for InitAcc<A, IN, OUT, ACC, I>
{
}

impl<A: 'static, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>, I: Init<A> + 'static>
    Accumulator<A, IN, OUT> for InitAcc<A, IN, OUT, ACC, I>
{
    fn zero() -> A {
        I::init()
    }

    fn add0(a1: &mut A, a: IN) {
        ACC::add0(a1, a);
    }

    fn combine(a1: &mut A, a2: &A) {
        ACC::combine(a1, a2);
    }

    fn finish(a: &A) -> OUT {
        ACC::finish(a)
    }
}

pub mod set {
    use super::*;
    use crate::core::state::StateType;
    use roaring::{RoaringBitmap, RoaringTreemap};
    use rustc_hash::FxHashSet;
    use std::hash::Hash;

    pub struct Set<A: StateType + Hash + Eq> {
        _marker: PhantomData<A>,
    }

    impl<A> Accumulator<FxHashSet<A>, A, FxHashSet<A>> for Set<A>
    where
        A: StateType + Hash + Eq,
    {
        fn zero() -> FxHashSet<A> {
            FxHashSet::default()
        }

        fn add0(a1: &mut FxHashSet<A>, a: A) {
            a1.insert(a);
        }

        fn combine(a1: &mut FxHashSet<A>, a2: &FxHashSet<A>) {
            a1.extend(a2.iter().cloned())
        }

        fn finish(a: &FxHashSet<A>) -> FxHashSet<A> {
            a.clone()
        }
    }

    pub trait BitSetSupport {}
    impl BitSetSupport for u32 {}
    impl BitSetSupport for u64 {}
    pub struct BitSet<A: StateType + Hash + Eq + BitSetSupport> {
        _marker: PhantomData<A>,
    }

    impl Accumulator<RoaringBitmap, u32, RoaringBitmap> for BitSet<u32> {
        fn zero() -> RoaringBitmap {
            RoaringBitmap::new()
        }

        fn add0(a1: &mut RoaringBitmap, a: u32) {
            a1.insert(a);
        }

        fn combine(a1: &mut RoaringBitmap, a2: &RoaringBitmap) {
            a1.extend(a2.iter());
        }

        fn finish(a: &RoaringBitmap) -> RoaringBitmap {
            a.clone()
        }
    }

    impl Accumulator<RoaringTreemap, u64, RoaringTreemap> for BitSet<u64> {
        fn zero() -> RoaringTreemap {
            RoaringTreemap::new()
        }

        fn add0(a1: &mut RoaringTreemap, a: u64) {
            a1.insert(a);
        }

        fn combine(a1: &mut RoaringTreemap, a2: &RoaringTreemap) {
            a1.extend(a2.iter());
        }

        fn finish(a: &RoaringTreemap) -> RoaringTreemap {
            a.clone()
        }
    }
}

pub mod topk {
    use super::*;
    use crate::core::state::StateType;
    use itertools::Itertools;
    use std::{cmp::Reverse, collections::BTreeSet, marker::PhantomData};

    pub struct TopK<A: StateType + Ord, const N: usize> {
        _marker: PhantomData<A>,
    }

    pub type TopKHeap<A> = BTreeSet<Reverse<A>>;

    impl<A, const N: usize> Accumulator<TopKHeap<A>, A, Vec<A>> for TopK<A, N>
    where
        A: StateType + Ord,
    {
        fn zero() -> TopKHeap<A> {
            TopKHeap::new()
        }

        fn add0(a1: &mut TopKHeap<A>, a: A) {
            a1.insert(Reverse(a));
            if a1.len() > N {
                a1.pop_last();
            }
        }

        fn combine(a1: &mut TopKHeap<A>, a2: &TopKHeap<A>) {
            a1.extend(a2.iter().cloned());
            while a1.len() > N {
                a1.pop_last();
            }
        }

        fn finish(a: &TopKHeap<A>) -> Vec<A> {
            a.iter().sorted().map(|Reverse(a)| a.clone()).collect()
        }
    }
}

#[cfg(test)]
mod agg_test {

    #[test]
    fn avg_def() {
        use super::{topk::TopK, topk::TopKHeap, Accumulator, AvgDef, MaxDef, MinDef, SumDef};

        let mut avg = AvgDef::<i32>::zero();
        let mut sum = SumDef::<i32>::zero();
        let mut min = MinDef::<i32>::zero();
        let mut max = MaxDef::<i32>::zero();
        let mut top3 = TopK::<i32, 3>::zero();
        let mut top5 = TopK::<i32, 5>::zero();
        // let mut arr = ArrConst::<u32, 5>::zero();

        for i in 0..100 {
            <AvgDef<i32> as Accumulator<(i32, usize), i32, i32>>::add0(&mut avg, i);
            <SumDef<i32> as Accumulator<i32, i32, i32>>::add0(&mut sum, i);
            <MinDef<i32> as Accumulator<i32, i32, i32>>::add0(&mut min, i);
            <MaxDef<i32> as Accumulator<i32, i32, i32>>::add0(&mut max, i);
            <TopK<i32, 3> as Accumulator<TopKHeap<i32>, i32, Vec<i32>>>::add0(&mut top3, i);
            <TopK<i32, 5> as Accumulator<TopKHeap<i32>, i32, Vec<i32>>>::add0(&mut top5, i);
        }

        assert_eq!(
            <AvgDef<i32> as Accumulator<(i32, usize), i32, i32>>::finish(&avg),
            49
        );
        assert_eq!(
            <SumDef<i32> as Accumulator<i32, i32, i32>>::finish(&sum),
            4950
        );
        assert_eq!(<MinDef<i32> as Accumulator<i32, i32, i32>>::finish(&min), 0);
        assert_eq!(
            <MaxDef<i32> as Accumulator<i32, i32, i32>>::finish(&max),
            99
        );
        assert_eq!(
            <TopK<i32, 3> as Accumulator<TopKHeap<i32>, i32, Vec<i32>>>::finish(&top3),
            vec![99, 98, 97]
        );
        assert_eq!(
            <TopK<i32, 5> as Accumulator<TopKHeap<i32>, i32, Vec<i32>>>::finish(&top5),
            vec![99, 98, 97, 96, 95]
        );
    }
}
