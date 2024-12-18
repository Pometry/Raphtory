use crate::core::utils::errors::GraphError;
use raphtory_api::iter::BoxedLIter;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, iter};

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("cannot set previous value '{previous_value:?}' to '{new_value:?}' in position '{index}'")]
pub struct IllegalSet<A: Debug> {
    pub index: usize,
    pub previous_value: A,
    pub new_value: A,
}

impl<A: Debug> IllegalSet<A> {
    fn new(index: usize, previous_value: A, new_value: A) -> IllegalSet<A> {
        IllegalSet {
            index,
            previous_value,
            new_value,
        }
    }
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct MaskedCol<T> {
    ts: Vec<T>,
    mask: Vec<bool>,
}

impl<T: Default> MaskedCol<T> {
    pub fn push(&mut self, t: Option<T>) {
        let is_some = t.is_some();
        if let Some(t) = t {
            self.ts.resize_with(self.mask.len(), || Default::default());
            self.ts.push(t);
        }
        self.mask.push(is_some);
    }

    pub fn upsert(&mut self, index: usize, t: Option<T>) {
        let is_some = t.is_some();
        if let Some(t) = t {
            if index >= self.ts.len() {
                self.ts.resize_with(index + 1, || Default::default());
            }
            self.ts[index] = t;
        }
        if index >= self.mask.len() {
            self.mask.resize(index + 1, false);
        }
        self.mask[index] = is_some;
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.mask
            .get(index)
            .and_then(|&is_some| (is_some).then(|| &self.ts[index]))
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.mask
            .get(index)
            .and_then(|&is_some| (is_some).then(|| &mut self.ts[index]))
    }

    pub fn len(&self) -> usize {
        self.mask.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&T>> {
        let ts_len = self.ts.len();
        self.ts
            .iter()
            .zip(&self.mask[0..ts_len])
            .map(|(t, &is_some)| is_some.then(|| t))
            .chain(self.mask[ts_len..].iter().map(|_| None))
    }
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct TupleCol<T> {
    size: usize,
    tuples: Vec<(usize, T)>,
}

impl<T> TupleCol<T> {
    pub fn with_len(len: usize) -> TupleCol<T> {
        TupleCol {
            size: len,
            tuples: Vec::with_capacity(1),
        }
    }

    pub fn from(mut tuples: Vec<(usize, T)>) -> TupleCol<T> {
        tuples.sort_by_key(|(id, _)| *id);
        let size = tuples.iter().map(|(id, _)| id + 1).max().unwrap_or(0);
        TupleCol { size, tuples }
    }

    pub fn push(&mut self, t: Option<T>) {
        let id = self.size;
        if let Some(t) = t {
            self.tuples.push((id, t));
        }
        self.size += 1;
    }

    pub fn upsert(&mut self, id: usize, t: Option<T>) {
        if let Some(t) = t {
            if let Some(value) = self.get_mut(id) {
                *value = t;
            } else {
                self.tuples.push((id, t));
            }
        }
        self.size = self.size.max(id + 1);
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn get(&self, id: usize) -> Option<&T> {
        self.tuples.iter().find(|(i, _)| *i == id).map(|(_, t)| t)
    }

    pub fn get_mut(&mut self, id: usize) -> Option<&mut T> {
        self.tuples
            .iter_mut()
            .find(|(i, _)| *i == id)
            .map(|(_, t)| t)
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&T>> {
        (0..self.size).map(move |id| self.get(id))
    }

    pub fn into_iter(mut self) -> impl Iterator<Item = Option<T>>
    where
        T: Default,
    {
        (0..self.size).map(move |id| {
            self.get_mut(id)
                .map(|t| std::mem::replace(t, Default::default()))
        })
    }
}

impl<T: Default> From<TupleCol<T>> for MaskedCol<T> {
    fn from(tuples: TupleCol<T>) -> MaskedCol<T> {
        let mut mask_col = MaskedCol::default();
        for (id, t) in tuples.into_iter().enumerate() {
            mask_col.upsert(id, t);
        }
        mask_col
    }
}

const LAZY_VEC_1_MAX_SIZE: usize = 8;

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum LazyVec<A> {
    #[default]
    Empty,
    // First value in "LazyVec1" and indices in "LazyVecN" vector denote the indices of this vec
    LazyVec1(A, TupleCol<A>),
    LazyVecN(A, MaskedCol<A>),
}

impl<A> LazyVec<A>
where
    A: PartialEq + Default + Debug + Sync + Send + Clone,
{
    // fails if there is already a value set for the given id to a different value
    pub(crate) fn set(&mut self, id: usize, value: A) -> Result<(), IllegalSet<A>> {
        match self {
            LazyVec::Empty => {
                *self = Self::from(id, value);
                Ok(())
            }
            LazyVec::LazyVec1(_, tuples) => {
                if let Some(only_value) = tuples.get(id) {
                    if only_value != &value {
                        return Err(IllegalSet::new(id, only_value.clone(), value));
                    }
                } else {
                    tuples.upsert(id, Some(value));

                    self.swap_lazy_types();
                }
                Ok(())
            }
            LazyVec::LazyVecN(_, vector) => {
                if let Some(only_value) = vector.get(id) {
                    if only_value != &value {
                        return Err(IllegalSet::new(id, only_value.clone(), value));
                    }
                } else {
                    vector.upsert(id, Some(value));
                }
                Ok(())
            }
        }
    }

    pub(crate) fn update<F, B>(&mut self, id: usize, updater: F) -> Result<B, GraphError>
    where
        F: FnOnce(&mut A) -> Result<B, GraphError>,
    {
        let b = match self.get_mut(id) {
            Some(value) => updater(value)?,
            None => {
                let mut value = A::default();
                let b = updater(&mut value)?;
                self.set(id, value)?;
                b
            }
        };
        Ok(b)
    }
}

impl<A> LazyVec<A>
where
    A: PartialEq + Default + Debug + Send + Sync,
{
    pub(crate) fn with_len(len: usize) -> Self {
        LazyVec::LazyVec1(A::default(), TupleCol::with_len(len))
    }

    fn swap_lazy_types(&mut self) {
        if let LazyVec::LazyVec1(_, tuples) = self {
            if tuples.len() >= LAZY_VEC_1_MAX_SIZE {
                let mut take = TupleCol::default();
                std::mem::swap(&mut take, tuples);
                let masked_col: MaskedCol<A> = take.into();
                *self = LazyVec::LazyVecN(A::default(), masked_col);
            }
        }
    }

    pub(crate) fn from(id: usize, value: A) -> Self {
        let mut inner = Vec::with_capacity(1);
        inner.push((id, value));
        LazyVec::LazyVec1(A::default(), TupleCol::from(inner))
    }

    pub(crate) fn filled_ids(&self) -> BoxedLIter<usize> {
        match self {
            LazyVec::Empty => Box::new(iter::empty()),
            LazyVec::LazyVec1(_, tuples) => Box::new(
                tuples
                    .iter()
                    .enumerate()
                    .filter_map(|(id, value)| value.map(|_| id)),
            ),
            LazyVec::LazyVecN(_, vector) => Box::new(
                vector
                    .iter()
                    .enumerate()
                    .filter_map(|(id, value)| value.map(|_| id)),
            ),
        }
    }

    #[cfg(test)]
    fn iter(&self) -> Box<dyn Iterator<Item = &A> + Send + '_> {
        match self {
            LazyVec::Empty => Box::new(iter::empty()),
            LazyVec::LazyVec1(default, tuples) => {
                Box::new(tuples.iter().map(|value| value.unwrap_or(default)))
            }
            LazyVec::LazyVecN(default, vector) => {
                Box::new(vector.iter().map(|value| value.unwrap_or(default)))
            }
        }
    }

    #[cfg(test)]
    fn iter_opt(&self) -> Box<dyn Iterator<Item = Option<&A>> + Send + '_> {
        match self {
            LazyVec::Empty => Box::new(iter::empty()),
            LazyVec::LazyVec1(_, tuples) => Box::new(tuples.iter()),
            LazyVec::LazyVecN(_, vector) => Box::new(vector.iter()),
        }
    }

    pub(crate) fn get(&self, id: usize) -> Option<&A> {
        match self {
            LazyVec::LazyVec1(default, tuples) => tuples
                .get(id)
                .or_else(|| (id < self.len()).then(|| default)),
            LazyVec::LazyVecN(default, vec) => {
                vec.get(id).or_else(|| (id < self.len()).then(|| default))
            }
            _ => None,
        }
    }

    pub(crate) fn get_opt(&self, id: usize) -> Option<&A> {
        match self {
            LazyVec::LazyVec1(_, tuples) => tuples.get(id),
            LazyVec::LazyVecN(_, vec) => vec.get(id),
            _ => None,
        }
    }

    pub(crate) fn get_mut(&mut self, id: usize) -> Option<&mut A> {
        match self {
            LazyVec::LazyVec1(_, tuples) => tuples.get_mut(id),
            LazyVec::LazyVecN(_, vec) => vec.get_mut(id).filter(|a| **a != A::default()),
            _ => None,
        }
    }

    pub(crate) fn push(&mut self, value: Option<A>) {
        match self {
            LazyVec::Empty => {
                if let Some(value) = value {
                    *self = LazyVec::from(0, value);
                } else {
                    *self = LazyVec::LazyVec1(A::default(), TupleCol::default());
                    self.push(value);
                }
            }
            LazyVec::LazyVec1(_, tuples) => {
                tuples.push(value);
                self.swap_lazy_types();
            }
            LazyVec::LazyVecN(_, vector) => {
                vector.push(value);
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            LazyVec::Empty => 0,
            LazyVec::LazyVec1(_, tuples) => tuples.len(),
            LazyVec::LazyVecN(_, vector) => vector.len(),
        }
    }
}

#[cfg(test)]
mod lazy_vec_tests {
    use super::*;
    use itertools::Itertools;
    use proptest::{arbitrary::Arbitrary, proptest};

    fn check_lazy_vec(lazy_vec: &LazyVec<u32>, v: Vec<Option<u32>>) {
        assert_eq!(lazy_vec.len(), v.len());
        for (i, value) in v.iter().enumerate() {
            assert_eq!(
                lazy_vec.get(i),
                Some(value.clone().unwrap_or_default()).as_ref()
            );
        }

        for (actual, expected) in lazy_vec.iter().zip(v.iter()) {
            assert_eq!(*actual, expected.as_ref().copied().unwrap_or_default());
        }

        for (actual, expected) in lazy_vec.iter_opt().zip(v.iter()) {
            assert_eq!(actual, expected.as_ref());
        }
    }

    #[test]
    fn push_null_changes_len() {
        let mut vec = LazyVec::<u32>::Empty;
        vec.push(None);
        assert_eq!(vec.len(), 1);

        let mut vec = LazyVec::<u32>::Empty;
        vec.push(None);
        vec.push(Some(1));
        assert_eq!(vec.len(), 2);

        let mut vec = LazyVec::<u32>::Empty;
        vec.push(Some(1));
        vec.push(None);
        assert_eq!(vec.len(), 2);

        let mut vec = LazyVec::<u32>::Empty;
        vec.push(None);
        vec.push(None);
        assert_eq!(vec.len(), 2);

        let mut vec = LazyVec::<u32>::Empty;
        vec.push(None);
        vec.push(Some(1));
        vec.push(None);
        assert_eq!(vec.len(), 3);
    }

    #[test]
    fn lazy_vec_is_opt_vec_push() {
        proptest!(|(
            v in Vec::<Option<u32>>::arbitrary(),
        )| {
            let mut lazy_vec = LazyVec::<u32>::Empty;
            for value in &v {
                lazy_vec.push(*value);
            }
            check_lazy_vec(&lazy_vec, v);
        });
    }

    #[test]
    fn normal_operation() {
        let mut vec = LazyVec::<u32>::Empty;

        vec.set(5, 55).unwrap();
        vec.set(1, 11).unwrap();
        vec.set(8, 88).unwrap();
        assert_eq!(vec.get(5), Some(&55));
        assert_eq!(vec.get(1), Some(&11));
        assert_eq!(vec.get(0), Some(&0));
        assert_eq!(vec.get(10), None);

        vec.update(5, |n| {
            *n = 100;
            Ok(())
        })
        .unwrap();
        assert_eq!(vec.get(5), Some(&100));

        vec.update(6, |n| {
            *n += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(vec.get(6), Some(&1));
        vec.update(9, |n| {
            *n += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(vec.get(9), Some(&1));

        assert_eq!(vec.filled_ids().collect_vec(), vec![1, 5, 6, 8, 9]);
    }

    #[test]
    fn set_fails_if_present() {
        let mut vec = LazyVec::from(5, 55);
        let result = vec.set(5, 555);
        assert_eq!(result, Err(IllegalSet::new(5, 55, 555)))
    }
}
