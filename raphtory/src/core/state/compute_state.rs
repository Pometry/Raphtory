use rustc_hash::FxHashMap;

use crate::core::agg::Accumulator;

use super::{StateType, container::{DynArray, MapArray, VecArray, merge_2_vecs}};

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

    fn iter<A: StateType>(&self, ss: usize) -> Box<dyn Iterator<Item = (usize, &A)> + '_>;

    fn iter_keys(&self) -> Box<dyn Iterator<Item = u64> + '_>;
    fn iter_keys_changed(&self, ss: usize) -> Box<dyn Iterator<Item = u64> + '_>;

    fn agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, ss: usize, a: IN, ki: usize)
    where
        A: StateType;

    fn combine<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, ss: usize, a: &A, ki: usize)
    where
        A: StateType;

    fn merge<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, other: &Self, ss: usize)
    where
        A: StateType;

    fn finalize<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&self, ss: usize) -> Vec<OUT>
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
pub struct ComputeStateMap(Box<dyn DynArray + 'static>);

impl ComputeStateMap {
    fn current_mut(&mut self) -> &mut dyn DynArray {
        self.0.as_mut()
    }

    fn current(&self) -> &dyn DynArray {
        self.0.as_ref()
    }
}

impl Clone for ComputeStateMap {
    fn clone(&self) -> Self {
        ComputeStateMap(self.0.clone_array())
    }
}

impl ComputeState for ComputeStateMap {
    fn clone_current_into_other(&mut self, ss: usize) {
        self.0.copy_over(ss);
    }

    fn reset_resetable_states(&mut self, ss: usize) {
        self.0.reset(ss);
    }

    fn new_mutable_primitive<T: StateType>(zero: T) -> Self {
        ComputeStateMap(Box::new(MapArray::<T> {
            map: FxHashMap::default(),
            zero,
        }))
    }

    fn read<A: 'static, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        i: usize,
    ) -> Option<OUT>
    where
        OUT: std::fmt::Debug,
    {
        let current = self
            .current()
            .as_any()
            .downcast_ref::<MapArray<A>>()
            .unwrap();

        current
            .map
            .get(&(i as u64))
            .map(|v| ACC::finish(&v[ss % 2]))
    }

    fn read_ref<A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        i: usize,
    ) -> Option<&A> {
        let current = self
            .current()
            .as_any()
            .downcast_ref::<MapArray<A>>()
            .unwrap();
        current.map.get(&(i as u64)).map(|v| &v[ss % 2])
    }

    fn iter<A: StateType>(&self, ss: usize) -> Box<dyn Iterator<Item = (usize, &A)> + '_> {
        let current = self
            .current()
            .as_any()
            .downcast_ref::<MapArray<A>>()
            .unwrap();
        Box::new(
            current
                .map
                .iter()
                .map(move |(k, v)| (*k as usize, &v[ss % 2])),
        )
    }

    fn iter_keys(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        self.current().iter_keys()
    }

    fn iter_keys_changed(&self, ss: usize) -> Box<dyn Iterator<Item = u64> + '_> {
        self.current().iter_keys_changed(ss)
    }

    fn agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, ss: usize, a: IN, i: usize)
    where
        A: StateType,
    {
        let current = self
            .current_mut()
            .as_mut_any()
            .downcast_mut::<MapArray<A>>()
            .unwrap();
        let entry = current
            .map
            .entry(i as u64)
            .or_insert_with(|| [current.zero.clone(), current.zero.clone()]);
        ACC::add0(&mut entry[ss % 2], a);
    }

    fn combine<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, ss: usize, a: &A, i: usize)
    where
        A: StateType,
    {
        let current = self
            .current_mut()
            .as_mut_any()
            .downcast_mut::<MapArray<A>>()
            .unwrap();
        let zero = current.zero.clone();
        let entry = current
            .map
            .entry(i as u64)
            .or_insert_with(|| [zero.clone(), zero.clone()]);
        ACC::combine(&mut entry[ss % 2], a);
    }

    fn merge<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, other: &Self, ss: usize)
    where
        A: StateType,
    {
        other.iter::<A>(ss).for_each(|(i, a)| {
            self.combine::<A, IN, OUT, ACC>(ss, a, i);
        });
    }

    fn finalize<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&self, ss: usize) -> Vec<OUT>
    where
        OUT: StateType,
        A: 'static,
    {
        let current = self
            .current()
            .as_any()
            .downcast_ref::<MapArray<A>>()
            .unwrap();
        current
            .map
            .values()
            .map(|v| ACC::finish(&v[ss % 2]))
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
            .downcast_ref::<MapArray<A>>()
            .unwrap();
        current
            .map
            .iter()
            .map(|(k, v)| (k, ACC::finish(&v[ss % 2])))
            .fold(b, |b, (k, out)| f(b, k, out))
    }
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
        OUT:std::fmt::Debug,
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

    fn iter<A: StateType>(&self, ss: usize) -> Box<dyn Iterator<Item = (usize, &A)> + '_> {
        let vec = self
            .current()
            .as_any()
            .downcast_ref::<VecArray<A>>()
            .unwrap();
        let iter = vec.current(ss).iter().enumerate();
        Box::new(iter)
    }

    fn iter_keys(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        todo!()
    }

    fn iter_keys_changed(&self, ss: usize) -> Box<dyn Iterator<Item = u64> + '_> {
        todo!()
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

    fn finalize<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&self, ss: usize) -> Vec<OUT>
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

        current.iter().map(|a| ACC::finish(a)).collect()
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