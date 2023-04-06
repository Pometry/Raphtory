//! Computing aggregates over temporal graph data.

use std::{
    marker::PhantomData,
    ops::{AddAssign, Div},
};

use num_traits::{Bounded, Zero};

use crate::state::StateType;

pub trait Accumulator<A, IN, OUT>: Send + Sync {
    fn zero() -> A;

    fn add0(a1: &mut A, a: IN);

    fn combine(a1: &mut A, a2: &A);

    fn finish(a: &A) -> OUT;
}

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
        println!("BEFORE: a1 = {:?}, a = {:?}", a1, a.clone());
        if a > *a1 {
            *a1 = a.clone();
        }
        println!("AFTER: a1 = {:?}, a = {:?}", a1, a);
    }

    fn combine(a1: &mut A, a2: &A) {
        Self::add0(a1, a2.clone());
    }

    fn finish(a: &A) -> A {
        a.clone()
    }
}

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

pub mod set {
    use super::*;
    use crate::state::StateType;
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
    use crate::state::StateType;
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
        use crate::agg::topk::TopK;
        use crate::agg::topk::TopKHeap;
        use crate::agg::Accumulator;
        use crate::agg::AvgDef;
        use crate::agg::MaxDef;
        use crate::agg::MinDef;
        use crate::agg::SumDef;

        let mut avg = AvgDef::<i32>::zero();
        let mut sum = SumDef::<i32>::zero();
        let mut min = MinDef::<i32>::zero();
        let mut max = MaxDef::<i32>::zero();
        let mut top3 = TopK::<i32, 3>::zero();
        let mut top5 = TopK::<i32, 5>::zero();

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
