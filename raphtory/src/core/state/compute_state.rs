use super::{
    container::{merge_2_vecs, DynArray, VecArray},
    StateType,
};
use crate::{core::state::agg::Accumulator, db::api::view::StaticGraphViewOps};
use std::collections::HashMap;

pub trait ComputeState: std::fmt::Debug + Clone + Send + Sync {
    fn clone_current_into_other(&mut self, ss: usize);

    fn reset_resetable_states(&mut self, ss: usize);

    fn new_mutable_primitive<T: StateType>(zero: T) -> Self;

    fn read<A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        i: usize,
    ) -> Option<OUT>
    where
        OUT: std::fmt::Debug;

    fn read_ref<A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        i: usize,
    ) -> Option<&A>;

    fn iter<A: StateType>(&self, ss: usize, extend_to: usize) -> Box<dyn Iterator<Item = &A> + '_>;

    fn agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, ss: usize, a: IN, ki: usize)
    where
        A: StateType;

    fn combine<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, ss: usize, a: &A, ki: usize)
    where
        A: StateType;

    fn merge<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, other: &Self, ss: usize)
    where
        A: StateType;

    fn finalize<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, G: StaticGraphViewOps>(
        &self,
        ss: usize,
        g: &G,
    ) -> HashMap<usize, OUT>
    where
        OUT: StateType,
        A: 'static;

    fn fold<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, F, B>(&self, ss: usize, b: B, f: F) -> B
    where
        F: FnOnce(B, &u64, OUT) -> B + Copy,
        A: 'static,
        B: std::fmt::Debug,
        OUT: StateType;
}

#[derive(Debug)]
pub struct ComputeStateVec(Box<dyn DynArray + 'static>);

impl ComputeStateVec {
    fn current_mut(&mut self) -> &mut dyn DynArray {
        self.0.as_mut()
    }

    fn current(&self) -> &dyn DynArray {
        self.0.as_ref()
    }
}

impl Clone for ComputeStateVec {
    fn clone(&self) -> Self {
        ComputeStateVec(self.0.clone_array())
    }
}

impl ComputeState for ComputeStateVec {
    fn clone_current_into_other(&mut self, ss: usize) {
        self.0.copy_over(ss);
    }

    fn reset_resetable_states(&mut self, ss: usize) {
        self.0.reset(ss);
    }

    fn new_mutable_primitive<T: StateType>(zero: T) -> Self {
        ComputeStateVec(Box::new(VecArray::new(zero)))
    }

    fn read<A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        i: usize,
    ) -> Option<OUT>
    where
        OUT: std::fmt::Debug,
    {
        let vec = self
            .current()
            .as_any()
            .downcast_ref::<VecArray<A>>()
            .unwrap();
        vec.current(ss).get(i).map(|a| ACC::finish(a))
    }

    fn read_ref<A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        i: usize,
    ) -> Option<&A> {
        let vec = self
            .current()
            .as_any()
            .downcast_ref::<VecArray<A>>()
            .unwrap();
        vec.current(ss).get(i)
    }

    fn iter<A: StateType>(&self, ss: usize, extend_to: usize) -> Box<dyn Iterator<Item = &A> + '_> {
        let vec = self
            .current()
            .as_any()
            .downcast_ref::<VecArray<A>>()
            .unwrap();
        let zero = vec.zero();
        let inner_vec = vec.current(ss);
        let vec_len = inner_vec.len();
        let extend_iter = std::iter::repeat_n(zero, extend_to - vec_len);
        let iter = inner_vec.iter().chain(extend_iter);
        Box::new(iter)
    }

    fn agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, ss: usize, a: IN, ki: usize)
    where
        A: StateType,
    {
        let vec = self
            .current_mut()
            .as_mut_any()
            .downcast_mut::<VecArray<A>>()
            .unwrap();

        let v = vec.current_mut(ss);
        if v.len() <= ki {
            v.resize(ki + 1, ACC::zero());
        }

        ACC::add0(&mut v[ki], a);
    }

    fn combine<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, ss: usize, a: &A, ki: usize)
    where
        A: StateType,
    {
        let vec = self
            .current_mut()
            .as_mut_any()
            .downcast_mut::<VecArray<A>>()
            .unwrap();

        let v = vec.current_mut(ss);
        if v.len() <= ki {
            v.resize(ki + 1, ACC::zero());
        }
        ACC::combine(&mut v[ki], a);
    }

    fn merge<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, other: &Self, ss: usize)
    where
        A: StateType,
    {
        let vec = self
            .current_mut()
            .as_mut_any()
            .downcast_mut::<VecArray<A>>()
            .unwrap();

        let other_vec = other
            .current()
            .as_any()
            .downcast_ref::<VecArray<A>>()
            .unwrap();

        let v = vec.current_mut(ss);
        let v_other = other_vec.current(ss);

        merge_2_vecs(v, v_other, |a, b| ACC::combine(a, b));
    }

    fn finalize<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, G: StaticGraphViewOps>(
        &self,
        ss: usize,
        _g: &G,
    ) -> HashMap<usize, OUT>
    where
        OUT: StateType,
        A: 'static,
    {
        let current = self
            .current()
            .as_any()
            .downcast_ref::<VecArray<A>>()
            .unwrap()
            .current(ss);

        current
            .iter()
            .enumerate()
            .map(|(p_id, a)| {
                let out = ACC::finish(a);
                (p_id, out)
            })
            .collect()
    }

    fn fold<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, F, B>(&self, ss: usize, b: B, f: F) -> B
    where
        F: FnOnce(B, &u64, OUT) -> B + Copy,
        A: 'static,
        B: std::fmt::Debug,
        OUT: StateType,
    {
        let current = self
            .current()
            .as_any()
            .downcast_ref::<VecArray<A>>()
            .unwrap()
            .current(ss);

        current.iter().enumerate().fold(b, |b, (i, a)| {
            let out = ACC::finish(a);
            let i = i as u64;
            f(b, &i, out)
        })
    }
}
