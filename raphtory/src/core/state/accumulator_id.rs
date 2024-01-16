use crate::core::state::agg::{Accumulator, Init, InitAcc};

#[derive(Debug)]
pub struct AccId<A, IN, OUT, ACC: Accumulator<A, IN, OUT>> {
    id: u32,
    _a: std::marker::PhantomData<(A, IN, OUT, ACC)>,
}

pub type AccId1<A, ACC> = AccId<A, A, A, ACC>;

impl<A, IN, OUT, ACC: Accumulator<A, IN, OUT>> Copy for AccId<A, IN, OUT, ACC> {}

impl<A, IN, OUT, ACC: Accumulator<A, IN, OUT>> AccId<A, IN, OUT, ACC> {
    pub fn id(&self) -> u32 {
        self.id
    }
}
impl<A: 'static, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>> AccId<A, IN, OUT, ACC> {
    pub fn init<I: Init<A> + 'static>(self) -> AccId<A, IN, OUT, InitAcc<A, IN, OUT, ACC, I>> {
        AccId {
            id: self.id,
            _a: std::marker::PhantomData,
        }
    }
}

impl<A, IN, OUT, ACC: Accumulator<A, IN, OUT>> Clone for AccId<A, IN, OUT, ACC> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            _a: std::marker::PhantomData,
        }
    }
}

unsafe impl<A, IN, OUT, ACC: Accumulator<A, IN, OUT>> Send for AccId<A, IN, OUT, ACC> {}

unsafe impl<A, IN, OUT, ACC: Accumulator<A, IN, OUT>> Sync for AccId<A, IN, OUT, ACC> {}

pub mod accumulators {
    use super::AccId;
    use crate::core::state::{
        agg::{
            set::Set,
            topk::{TopK, TopKHeap},
            Accumulator, AndDef, ArrConst, AvgDef, MaxDef, MinDef, OrDef, SumDef, ValDef,
        },
        StateType,
    };
    use num_traits::{Bounded, Zero};
    use rustc_hash::FxHashSet;
    use std::{
        cmp::Eq,
        hash::Hash,
        ops::{AddAssign, Div},
    };

    pub fn and(id: u32) -> AccId<bool, bool, bool, AndDef> {
        AccId {
            id,
            _a: std::marker::PhantomData,
        }
    }

    pub fn or(id: u32) -> AccId<bool, bool, bool, OrDef> {
        AccId {
            id,
            _a: std::marker::PhantomData,
        }
    }

    pub fn min<A: StateType + Bounded + PartialOrd>(id: u32) -> AccId<A, A, A, MinDef<A>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
        }
    }

    pub fn max<A: StateType + Bounded + PartialOrd>(id: u32) -> AccId<A, A, A, MaxDef<A>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
        }
    }

    pub fn sum<A: StateType + Zero + AddAssign<A>>(id: u32) -> AccId<A, A, A, SumDef<A>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
        }
    }

    pub fn val<A: StateType + Zero>(id: u32) -> AccId<A, A, A, ValDef<A>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
        }
    }

    pub fn avg<A>(id: u32) -> AccId<(A, usize), A, A, AvgDef<A>>
    where
        A: StateType + Zero + AddAssign<A> + TryFrom<usize> + Div<A, Output = A>,
        <A as TryFrom<usize>>::Error: std::fmt::Debug,
    {
        AccId {
            id,
            _a: std::marker::PhantomData,
        }
    }

    pub fn topk<A: StateType + Ord, const N: usize>(
        id: u32,
    ) -> AccId<TopKHeap<A>, A, Vec<A>, TopK<A, N>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
        }
    }

    pub fn arr<A: StateType, ACC: Accumulator<A, A, A>, const N: usize>(
        id: u32,
    ) -> AccId<[A; N], [A; N], [A; N], ArrConst<A, ACC, N>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
        }
    }

    pub fn hash_set<A: StateType + Hash + Eq>(
        id: u32,
    ) -> AccId<FxHashSet<A>, A, FxHashSet<A>, Set<A>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
        }
    }
}
