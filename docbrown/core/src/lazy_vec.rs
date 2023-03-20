use crate::tprop::TProp;
use crate::Prop;
use serde::{Deserialize, Serialize};
use std::ops::Range;

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum LazyVec<A> {
    #[default]
    Empty,
    // First value in "LazyVec1" and indices in "LazyVecN" vector denote the indices of this vec
    LazyVec1(usize, A),
    LazyVecN(Vec<A>),
}

impl<A> LazyVec<A>
where
    A: PartialEq + Default + Clone
{
    pub(crate) fn from(id: usize, value: A) -> Self {
        LazyVec::LazyVec1(id, value)
    }

    pub(crate) fn filled_ids(&self) -> Vec<usize> {
        match self {
            LazyVec::Empty => Default::default(),
            LazyVec::LazyVec1(id, _) => vec![*id],
            LazyVec::LazyVecN(vector) => {
                vector.iter().enumerate()
                    .filter(|&(id, value)| *value != Default::default())
                    .map(|(id, _)| id)
                    .collect()
            }
        }
    }

    pub(crate) fn get(&self, id: usize) -> Option<&A> {
        match self {
            LazyVec::LazyVec1(only_id, value) if *only_id == id => Some(value),
            LazyVec::LazyVecN(vec) => vec.get(id),
            _ => None,
        }
    }

    // fails if there is already a value set for the given id
    pub(crate) fn set(&mut self, id: usize, value: A) {
        match self {
            LazyVec::Empty => {
                *self = Self::from(id, value);
            }
            LazyVec::LazyVec1(only_id, only_value) => {
                if *only_id == id {
                    panic!("cannot set value in position '{id}' because it was perviously set");
                } else {
                    let mut vector = vec![Default::default(); usize::max(id, *only_id) + 1];
                    vector[id] = value;
                    vector[*only_id] = only_value.clone();
                    *self = LazyVec::LazyVecN(vector);
                }
            }
            LazyVec::LazyVecN(vector) => {
                if vector.len() <= id {
                    vector.resize(id + 1, Default::default())
                }
                if vector[id] == Default::default() {
                    vector[id] = value;
                } else {
                    panic!("cannot set value in position '{id}' because it was perviously set");
                }
            }
        }
    }

    fn get_mut(&mut self, id: usize) -> Option<&mut A> {
        match self {
            LazyVec::LazyVec1(only_id, value) if *only_id == id => Some(value),
            LazyVec::LazyVecN(vec) => vec.get_mut(id),
            _ => None,
        }
    }

    pub(crate) fn update_or_set<F>(&mut self, id: usize, mut updater: F, default: A)
    where
        F: FnOnce(&mut A)
    {
        match self.get_mut(id) {
            Some(value) => updater(value),
            None => self.set(id, default),
        }
    }
}

#[cfg(test)]
mod lazy_vec_tests {
    use super::*;

    #[test]
    fn normal_operation() {
        let mut vec = LazyVec::<u32>::Empty;

        vec.set(5, 55);
        vec.set(1, 11);
        vec.set(8, 88);
        assert_eq!(vec.get(5), Some(&55));
        assert_eq!(vec.get(1), Some(&11));
        assert_eq!(vec.get(0), Some(&0));
        assert_eq!(vec.get(10), None); // FIXME: this should return the default, 0, as well, there is no need to return Option from get()

        // FIXME: replace update_or_set() with update()
        // the behavior should be the same for both cases, because we should be able to assume that
        // any cell is prefilled with default values and can therefore be safely updated
        vec.update_or_set(6, |n| { *n += 1 }, 66);
        assert_eq!(vec.get(6), Some(&1));
        vec.update_or_set(9, |n| { *n += 1 }, 99);
        assert_eq!(vec.get(9), Some(&99));

        assert_eq!(vec.filled_ids(), vec![1, 5, 6, 8, 9]);
    }

    #[test]
    #[should_panic]
    fn set_fails_if_present() {
        let mut vec = LazyVec::from(5, 55);
        vec.set(5, 555);
    }
}
