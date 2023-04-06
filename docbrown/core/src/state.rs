//! A data structure for storing stateful data for temporal graphs and their shards.

use crate::agg::Accumulator;
use crate::utils::get_shard_id_from_global_vid;
use rustc_hash::FxHashMap;
use std::{any::Any, fmt::Debug};

#[derive(Debug)]
pub struct AccId<A, IN, OUT, ACC: Accumulator<A, IN, OUT>> {
    id: u32,
    _a: std::marker::PhantomData<A>,
    _acc: std::marker::PhantomData<ACC>,
    _in: std::marker::PhantomData<IN>,
    _out: std::marker::PhantomData<OUT>,
}

impl<A, IN, OUT, ACC: Accumulator<A, IN, OUT>> Clone for AccId<A, IN, OUT, ACC> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            _a: std::marker::PhantomData,
            _acc: std::marker::PhantomData,
            _in: std::marker::PhantomData,
            _out: std::marker::PhantomData,
        }
    }
}

unsafe impl<A, IN, OUT, ACC: Accumulator<A, IN, OUT>> Send for AccId<A, IN, OUT, ACC> {}

unsafe impl<A, IN, OUT, ACC: Accumulator<A, IN, OUT>> Sync for AccId<A, IN, OUT, ACC> {}

pub mod def {
    use super::{AccId, StateType};
    use crate::agg::{
        set::{BitSet, Set},
        topk::{TopK, TopKHeap},
        AvgDef, MaxDef, MinDef, SumDef, ValDef,
    };
    use num_traits::{Bounded, Zero};
    use roaring::{RoaringBitmap, RoaringTreemap};
    use rustc_hash::FxHashSet;
    use std::{
        cmp::Eq,
        hash::Hash,
        ops::{AddAssign, Div},
    };

    pub fn min<A: StateType + Bounded + PartialOrd>(id: u32) -> AccId<A, A, A, MinDef<A>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
            _acc: std::marker::PhantomData,
            _in: std::marker::PhantomData,
            _out: std::marker::PhantomData,
        }
    }

    pub fn max<A: StateType + Bounded + PartialOrd>(id: u32) -> AccId<A, A, A, MaxDef<A>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
            _acc: std::marker::PhantomData,
            _in: std::marker::PhantomData,
            _out: std::marker::PhantomData,
        }
    }

    pub fn sum<A: StateType + Zero + AddAssign<A>>(id: u32) -> AccId<A, A, A, SumDef<A>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
            _acc: std::marker::PhantomData,
            _in: std::marker::PhantomData,
            _out: std::marker::PhantomData,
        }
    }

    pub fn val<A: StateType + Zero>(id: u32) -> AccId<A, A, A, ValDef<A>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
            _acc: std::marker::PhantomData,
            _in: std::marker::PhantomData,
            _out: std::marker::PhantomData,
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
            _acc: std::marker::PhantomData,
            _in: std::marker::PhantomData,
            _out: std::marker::PhantomData,
        }
    }

    pub fn topk<A: StateType + Ord, const N: usize>(
        id: u32,
    ) -> AccId<TopKHeap<A>, A, Vec<A>, TopK<A, N>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
            _acc: std::marker::PhantomData,
            _in: std::marker::PhantomData,
            _out: std::marker::PhantomData,
        }
    }

    pub fn hash_set<A: StateType + Hash + Eq>(
        id: u32,
    ) -> AccId<FxHashSet<A>, A, FxHashSet<A>, Set<A>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
            _acc: std::marker::PhantomData,
            _in: std::marker::PhantomData,
            _out: std::marker::PhantomData,
        }
    }

    pub fn bit_set_32(id: u32) -> AccId<RoaringBitmap, u32, RoaringBitmap, BitSet<u32>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
            _acc: std::marker::PhantomData,
            _in: std::marker::PhantomData,
            _out: std::marker::PhantomData,
        }
    }

    pub fn bit_set_64(id: u32) -> AccId<RoaringTreemap, u64, RoaringTreemap, BitSet<u64>> {
        AccId {
            id,
            _a: std::marker::PhantomData,
            _acc: std::marker::PhantomData,
            _in: std::marker::PhantomData,
            _out: std::marker::PhantomData,
        }
    }
}

pub trait DynArray: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
    fn clone_array(&self) -> Box<dyn DynArray>;
    fn len(&self) -> usize;
    fn copy_from(&mut self, other: &dyn DynArray);
    // used for tricks
    fn empty(&self) -> Box<dyn DynArray>;
    // used for map array
    fn copy_over(&mut self, ss: usize);
    fn iter_keys(&self) -> Box<dyn Iterator<Item = u64> + '_>;
    fn iter_keys_changed(&self, ss: usize) -> Box<dyn Iterator<Item = u64> + '_>;
}

#[derive(Debug, Clone, PartialEq)]
struct MapArray<T> {
    map: FxHashMap<u64, [T; 2]>,
    zero: T,
}

impl<T> DynArray for MapArray<T>
where
    T: StateType,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_array(&self) -> Box<dyn DynArray> {
        Box::new(self.clone())
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn copy_from(&mut self, other: &dyn DynArray) {
        let other = other.as_any().downcast_ref::<MapArray<T>>().unwrap();
        self.map = other.map.clone();
    }

    fn empty(&self) -> Box<dyn DynArray> {
        panic!("not implemented");
    }

    fn copy_over(&mut self, ss: usize) {
        for val in self.map.values_mut() {
            let i = ss % 2;
            let j = (ss + 1) % 2;
            val[j] = val[i].clone();
        }
    }

    fn iter_keys(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        Box::new(self.map.keys().copied())
    }

    fn iter_keys_changed(&self, ss: usize) -> Box<dyn Iterator<Item = u64> + '_> {
        Box::new(self.map.iter().filter_map(move |(k, v)| {
            let i = ss % 2;
            let j = (ss + 1) % 2;
            if v[i] != v[j] {
                Some(*k)
            } else {
                None
            }
        }))
    }
}

pub trait StateType: PartialEq + Clone + Debug + Send + Sync + 'static {}

impl<T: PartialEq + Clone + Debug + Send + Sync + 'static> StateType for T {}

pub trait ComputeState: Debug + Clone {
    fn clone_current_into_other(&mut self, ss: usize);

    fn new_mutable_primitive<T: StateType>(zero: T) -> Self;

    fn read<A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        i: usize,
    ) -> Option<OUT>
    where
        OUT: Debug;

    fn read_ref<A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        i: usize,
    ) -> Option<&A>;

    fn iter<A: StateType>(&self, ss: usize) -> Box<dyn Iterator<Item = (usize, &A)> + '_>;

    fn iter_keys(&self) -> Box<dyn Iterator<Item = u64> + '_>;
    fn iter_keys_changed(&self, ss: usize) -> Box<dyn Iterator<Item = u64> + '_>;

    fn reset<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, ki: usize)
    where
        A: StateType;

    fn agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, ss: usize, a: IN, ki: usize)
    where
        A: StateType;

    fn combine<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, ss: usize, a: &A, ki: usize)
    where
        A: StateType;

    fn merge<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, CS2: ComputeState>(
        &mut self,
        other: &CS2,
        ss: usize,
    ) where
        A: StateType;

    fn finalize<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&self, ss: usize) -> Vec<OUT>
    where
        OUT: StateType,
        A: 'static;

    fn fold<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, F, B>(&self, ss: usize, b: B, f: F) -> B
    where
        F: FnOnce(B, &u64, OUT) -> B + Copy,
        A: 'static,
        B: Debug,
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
        OUT: Debug,
    {
        let current = self
            .current()
            .as_any()
            .downcast_ref::<MapArray<A>>()
            .unwrap();

        current.map.get(&(i as u64)).map(|v| {
            println!("0 = {:?}, 1 = {:?}", ACC::finish(&v[0]), ACC::finish(&v[1]));
            ACC::finish(&v[ss % 2])
        })
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

    fn reset<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(&mut self, i: usize)
    where
        A: StateType,
    {
        let current = self
            .current_mut()
            .as_mut_any()
            .downcast_mut::<MapArray<A>>()
            .unwrap();
        current.map.remove_entry(&(i as u64));
        current
            .map
            .insert(i as u64, [current.zero.clone(), current.zero.clone()]);
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

    fn merge<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, CS2: ComputeState>(
        &mut self,
        other: &CS2,
        ss: usize,
    ) where
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
        B: Debug,
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

const GLOBAL_STATE_KEY: usize = 0;
#[derive(Debug, Clone)]
pub struct ShardComputeState<CS: ComputeState + Send> {
    states: FxHashMap<u32, CS>,
}

impl<CS: ComputeState + Send + Clone> ShardComputeState<CS> {
    fn copy_over_next_ss(&mut self, ss: usize) {
        for (_, state) in self.states.iter_mut() {
            state.clone_current_into_other(ss);
        }
    }

    fn fold<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, F, B>(
        &self,
        ss: usize,
        b: B,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        f: F,
    ) -> B
    where
        F: FnOnce(B, &u64, OUT) -> B + Copy,
        A: 'static,
        B: Debug,
        OUT: StateType,
    {
        if let Some(state) = self.states.get(&agg_ref.id) {
            state.fold::<A, IN, OUT, ACC, F, B>(ss, b, f)
        } else {
            b
        }
    }

    fn read_vec<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<Vec<OUT>>
    where
        OUT: StateType,
        A: 'static,
    {
        let cs = self.states.get(&agg_ref.id)?;
        Some(cs.finalize::<A, IN, OUT, ACC>(ss))
    }

    fn merge<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        other: &Self,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        ss: usize,
    ) where
        A: StateType,
    {
        match (
            self.states.get_mut(&agg_ref.id),
            other.states.get(&agg_ref.id),
        ) {
            (Some(self_cs), Some(other_cs)) => {
                self_cs.merge::<A, IN, OUT, ACC, CS>(other_cs, ss);
            }
            (None, Some(other_cs)) => {
                self.states.insert(agg_ref.id, other_cs.clone());
            }
            _ => {}
        }
    }

    fn read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        i: usize,
        id: u32,
        ss: usize,
    ) -> Option<OUT>
    where
        A: StateType,
        OUT: Debug,
    {
        let state = self.states.get(&id)?;
        state.read::<A, IN, OUT, ACC>(ss, i)
    }

    fn read_ref<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        i: usize,
        id: u32,
        ss: usize,
    ) -> Option<&A>
    where
        A: StateType,
    {
        let state = self.states.get(&id)?;
        state.read_ref::<A, IN, OUT, ACC>(ss, i)
    }

    fn new() -> Self {
        ShardComputeState {
            states: FxHashMap::default(),
        }
    }

    fn reset<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        into: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) where
        A: StateType,
    {
        let state = self
            .states
            .entry(agg_ref.id)
            .or_insert_with(|| CS::new_mutable_primitive(ACC::zero()));
        state.reset::<A, IN, OUT, ACC>(into);
    }

    fn accumulate_into<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        ss: usize,
        into: usize,
        a: IN,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) where
        A: StateType,
    {
        let state = self
            .states
            .entry(agg_ref.id)
            .or_insert_with(|| CS::new_mutable_primitive(ACC::zero()));
        state.agg::<A, IN, OUT, ACC>(ss, a, into);
    }
}

#[cfg(test)]
impl<CS: ComputeState + Send> ShardComputeState<CS> {
    fn finalize<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        ss: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<Vec<OUT>>
    where
        OUT: StateType,
        A: 'static,
    {
        // finalize the accumulator
        // print the states
        let state = self.states.get(&agg_ref.id)?;
        let state_arr = state.finalize::<A, IN, OUT, ACC>(ss);
        Some(state_arr)
    }
}

#[derive(Debug, Clone)]
pub struct ShuffleComputeState<CS: ComputeState + Send> {
    pub global: ShardComputeState<CS>,
    pub parts: Vec<ShardComputeState<CS>>,
}

// every partition has a struct as such
impl<CS: ComputeState + Send + Sync> ShuffleComputeState<CS> {
    pub fn fold_state<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, B, F>(
        &self,
        ss: usize,
        b: B,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        f: F,
    ) -> B
    where
        A: StateType,
        B: Debug,
        OUT: StateType,
        F: Fn(B, &u64, OUT) -> B + Copy,
    {
        let out_b = self
            .parts
            .iter()
            .fold(b, |b, part| part.fold(ss, b, agg_ref, f));
        out_b
    }

    pub fn merge_mut<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        other: &Self,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        ss: usize,
    ) where
        A: StateType,
    {
        // zip the two partitions
        // merge each shard
        assert_eq!(self.parts.len(), other.parts.len());
        self.parts
            .iter_mut()
            .zip(other.parts.iter())
            .for_each(|(s, o)| s.merge(o, agg_ref, ss));
    }

    pub fn merge_mut_global<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        other: &Self,
        agg_ref: &AccId<A, IN, OUT, ACC>,
        ss: usize,
    ) where
        A: StateType,
    {
        self.global.merge(&other.global, agg_ref, ss);
    }

    pub fn copy_over_next_ss(&mut self, ss: usize) {
        self.parts.iter_mut().for_each(|p| p.copy_over_next_ss(ss));
    }

    pub fn new(n_parts: usize) -> Self {
        Self {
            parts: (0..n_parts)
                .into_iter()
                .map(|_| ShardComputeState::new())
                .collect(),
            global: ShardComputeState::new(),
        }
    }

    pub fn keys(&self, part_num: usize) -> impl Iterator<Item = u64> + '_ {
        self.parts[part_num]
            .states
            .iter()
            .flat_map(|(_, cs)| cs.iter_keys())
    }

    pub fn changed_keys(&self, part_num: usize, ss: usize) -> impl Iterator<Item = u64> + '_ {
        self.parts[part_num]
            .states
            .iter()
            .flat_map(move |(_, cs)| cs.iter_keys_changed(ss))
    }

    pub fn reset<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        into: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) where
        A: StateType,
    {
        let part = get_shard_id_from_global_vid(into, self.parts.len());
        self.parts[part].reset(into, agg_ref)
    }

    pub fn reset_global<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) where
        A: StateType,
    {
        self.global.reset(GLOBAL_STATE_KEY, agg_ref)
    }

    pub fn accumulate_into<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        ss: usize,
        into: usize,
        a: IN,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) where
        A: StateType,
    {
        let part = get_shard_id_from_global_vid(into, self.parts.len());
        self.parts[part].accumulate_into(ss, into, a, agg_ref)
    }

    pub fn accumulate_global<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        ss: usize,
        a: IN,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) where
        A: StateType,
    {
        self.global
            .accumulate_into(ss, GLOBAL_STATE_KEY, a, agg_ref)
    }
    // reads the value from K if it's set we return Ok(a) else we return Err(zero) from the monoid
    pub fn read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        into: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<OUT>
    where
        A: StateType,
        OUT: Debug,
    {
        let part = get_shard_id_from_global_vid(into, self.parts.len());
        self.parts[part].read::<A, IN, OUT, ACC>(into, agg_ref.id, ss)
    }

    pub fn read_ref<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        into: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<&A>
    where
        A: StateType,
    {
        let part = get_shard_id_from_global_vid(into, self.parts.len());
        self.parts[part].read_ref::<A, IN, OUT, ACC>(into, agg_ref.id, ss)
    }

    pub fn read_global<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        agg_ref: &AccId<A, IN, OUT, ACC>,
    ) -> Option<OUT>
    where
        A: StateType,
        OUT: Debug,
    {
        self.global
            .read::<A, IN, OUT, ACC>(GLOBAL_STATE_KEY, agg_ref.id, ss)
    }

    pub fn read_vec_partition<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        ss: usize,
        agg_def: &AccId<A, IN, OUT, ACC>,
    ) -> Vec<Vec<OUT>>
    where
        OUT: StateType,
        A: 'static,
    {
        self.parts
            .iter()
            .flat_map(|part| part.read_vec(ss, agg_def))
            .collect()
    }
}

#[cfg(test)]
impl<CS: ComputeState + Send> ShuffleComputeState<CS> {
    fn finalize<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        ss: usize,
        agg_def: &AccId<A, IN, OUT, ACC>,
    ) -> Vec<Option<Vec<OUT>>>
    where
        OUT: StateType,
        A: 'static,
    {
        self.parts
            .iter_mut()
            .map(|part| part.finalize(ss, &agg_def))
            .collect()
    }
}

#[cfg(test)]
mod state_test {

    // take two monoids in 2 vectors, zip them with 2 graphs
    // merge the monoids in parallel

    use super::*;
    use rand::Rng;

    #[test]
    fn min_aggregates_for_3_keys() {
        let min = def::min(0);

        let mut state_map: ShardComputeState<ComputeStateMap> = ShardComputeState::new();

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec = vec![];
        let mut actual_min = i32::MAX;
        for _ in 0..100 {
            let i = rng.gen_range(0..100);
            actual_min = actual_min.min(i);
            vec.push(i);
        }

        for a in vec {
            state_map.accumulate_into(0, 0, a, &min);
            state_map.accumulate_into(0, 1, a, &min);
            state_map.accumulate_into(0, 2, a, &min);
        }

        let actual = state_map.finalize(0, &min);
        assert_eq!(actual, Some(vec![actual_min, actual_min, actual_min]));
    }

    #[test]
    fn avg_aggregates_for_3_keys() {
        let avg = def::avg(0);

        let mut state_map: ShardComputeState<ComputeStateMap> = ShardComputeState::new();

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec = vec![];
        let mut sum = 0;
        for _ in 0..100 {
            let i = rng.gen_range(0..100);
            sum += i;
            vec.push(i);
        }

        for a in vec {
            state_map.accumulate_into(0, 0, a, &avg);
            state_map.accumulate_into(0, 1, a, &avg);
            state_map.accumulate_into(0, 2, a, &avg);
        }

        let actual_avg = sum / 100;
        let actual = state_map.finalize(0, &avg);
        assert_eq!(actual, Some(vec![actual_avg, actual_avg, actual_avg]));
    }

    #[test]
    fn top3_aggregates_for_3_keys() {
        let avg = def::topk::<i32, 3>(0);

        let mut state_map: ShardComputeState<ComputeStateMap> = ShardComputeState::new();

        for a in 0..100 {
            state_map.accumulate_into(0, 0, a, &avg);
            state_map.accumulate_into(0, 1, a, &avg);
            state_map.accumulate_into(0, 2, a, &avg);
        }
        let expected = vec![99, 98, 97];

        let actual = state_map.finalize(0, &avg);
        assert_eq!(
            actual,
            Some(vec![expected.clone(), expected.clone(), expected.clone()])
        );
    }

    #[test]
    fn sum_aggregates_for_3_keys() {
        let sum = def::sum(0);

        let mut state: ShardComputeState<ComputeStateMap> = ShardComputeState::new();

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec = vec![];
        let mut actual_sum = 0;
        for _ in 0..100 {
            let i = rng.gen_range(0..100);
            actual_sum += i;
            vec.push(i);
        }

        for a in vec {
            state.accumulate_into(0, 0, a, &sum);
            state.accumulate_into(0, 1, a, &sum);
            state.accumulate_into(0, 2, a, &sum);
        }

        let actual = state.finalize(0, &sum);

        assert_eq!(actual, Some(vec![actual_sum, actual_sum, actual_sum]));
    }

    #[test]
    fn sum_aggregates_for_3_keys_2_parts() {
        let sum = def::sum(0);

        let mut part1_state: ShuffleComputeState<ComputeStateMap> = ShuffleComputeState::new(2);
        let mut part2_state: ShuffleComputeState<ComputeStateMap> = ShuffleComputeState::new(2);

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec1 = vec![];
        let mut vec2 = vec![];
        let mut actual_sum_1 = 0;
        let mut actual_sum_2 = 0;
        for _ in 0..100 {
            // data for first partition
            let i = rng.gen_range(0..100);
            actual_sum_1 += i;
            vec1.push(i);

            // data for second partition
            let i = rng.gen_range(0..100);
            actual_sum_2 += i;
            vec2.push(i);
        }

        // 1 gets all the numbers
        // 2 gets the numbers from part1
        // 3 gets the numbers from part2
        for a in vec1 {
            part1_state.accumulate_into(0, 1, a, &sum);
            part1_state.accumulate_into(0, 2, a, &sum);
        }

        for a in vec2 {
            part2_state.accumulate_into(0, 1, a, &sum);
            part2_state.accumulate_into(0, 3, a, &sum);
        }

        println!("part1_state: {:?}", part1_state);
        println!("part2_state: {:?}", part2_state);

        let actual = part1_state.finalize(0, &sum);

        assert_eq!(
            actual,
            vec![Some(vec![actual_sum_1]), Some(vec![actual_sum_1])]
        );

        let actual = part2_state.finalize(0, &sum);

        assert_eq!(actual, vec![None, Some(vec![actual_sum_2, actual_sum_2])]);

        ShuffleComputeState::merge_mut(&mut part1_state, &part2_state, &sum, 0);
        let actual = part1_state.finalize(0, &sum);

        assert_eq!(
            actual,
            vec![
                Some(vec![(actual_sum_1)]),
                Some(vec![(actual_sum_1 + actual_sum_2), (actual_sum_2)]),
            ]
        );
    }

    #[test]
    fn min_sum_aggregates_for_3_keys_2_parts() {
        let sum = def::sum(0);
        let min = def::min(1);

        let mut part1_state: ShuffleComputeState<ComputeStateMap> = ShuffleComputeState::new(2);
        let mut part2_state: ShuffleComputeState<ComputeStateMap> = ShuffleComputeState::new(2);

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec1 = vec![];
        let mut vec2 = vec![];
        let mut actual_sum_1 = 0;
        let mut actual_sum_2 = 0;
        let mut actual_min_1 = 100;
        let mut actual_min_2 = 100;
        for _ in 0..100 {
            // data for first partition
            let i = rng.gen_range(0..100);
            actual_sum_1 += i;
            actual_min_1 = actual_min_1.min(i);
            vec1.push(i);

            // data for second partition
            let i = rng.gen_range(0..100);
            actual_sum_2 += i;
            actual_min_2 = actual_min_2.min(i);
            vec2.push(i);
        }

        // 1 gets all the numbers
        // 2 gets the numbers from part1
        // 3 gets the numbers from part2
        for a in vec1 {
            part1_state.accumulate_into(0, 1, a, &sum);
            part1_state.accumulate_into(0, 2, a, &sum);
            part1_state.accumulate_into(0, 1, a, &min);
            part1_state.accumulate_into(0, 2, a, &min);
        }

        for a in vec2 {
            part2_state.accumulate_into(0, 1, a, &sum);
            part2_state.accumulate_into(0, 3, a, &sum);
            part2_state.accumulate_into(0, 1, a, &min);
            part2_state.accumulate_into(0, 3, a, &min);
        }

        let actual = part1_state.finalize(0, &sum);
        assert_eq!(
            actual,
            vec![Some(vec![actual_sum_1]), Some(vec![actual_sum_1])]
        );

        let actual = part1_state.finalize(0, &min);
        assert_eq!(
            actual,
            vec![Some(vec![actual_min_1]), Some(vec![actual_min_1])]
        );

        let actual = part2_state.finalize(0, &sum);
        assert_eq!(actual, vec![None, Some(vec![actual_sum_2, actual_sum_2])]);

        let actual = part2_state.finalize(0, &min);
        assert_eq!(actual, vec![None, Some(vec![actual_min_2, actual_min_2])]);

        ShuffleComputeState::merge_mut(&mut part1_state, &part2_state, &sum, 0);
        let actual = part1_state.finalize(0, &sum);

        assert_eq!(
            actual,
            vec![
                Some(vec![(actual_sum_1)]),
                Some(vec![(actual_sum_1 + actual_sum_2), (actual_sum_2)]),
            ]
        );

        ShuffleComputeState::merge_mut(&mut part1_state, &part2_state, &min, 0);
        let actual = part1_state.finalize(0, &min);
        assert_eq!(
            actual,
            vec![
                Some(vec![(actual_min_1)]),
                Some(vec![(actual_min_1.min(actual_min_2)), (actual_min_2)]),
            ]
        );
    }
}
