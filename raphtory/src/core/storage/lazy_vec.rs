use serde::{Deserialize, Serialize};
use std::fmt::Debug;

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
pub(crate) enum LazyVec<A> {
    #[default]
    Empty,
    // First value in "LazyVec1" and indices in "LazyVecN" vector denote the indices of this vec
    LazyVec1(usize, A),
    LazyVecN(Vec<A>),
}

impl<A> LazyVec<A>
where
    A: PartialEq + Default + Clone + Debug,
{
    pub(crate) fn from(id: usize, value: A) -> Self {
        LazyVec::LazyVec1(id, value)
    }

    pub(crate) fn filled_ids(&self) -> Vec<usize> {
        match self {
            LazyVec::Empty => Default::default(),
            LazyVec::LazyVec1(id, _) => vec![*id],
            LazyVec::LazyVecN(vector) => vector
                .iter()
                .enumerate()
                .filter(|&(_, value)| *value != Default::default())
                .map(|(id, _)| id)
                .collect(),
        }
    }

    pub(crate) fn get(&self, id: usize) -> Option<&A> {
        match self {
            LazyVec::LazyVec1(only_id, value) if *only_id == id => Some(value),
            LazyVec::LazyVecN(vec) => vec.get(id),
            _ => None,
        }
    }

    // fails if there is already a value set for the given id to a different value
    pub(crate) fn set(&mut self, id: usize, value: A) -> Result<(), IllegalSet<A>> {
        match self {
            LazyVec::Empty => {
                *self = Self::from(id, value);
                Ok(())
            }
            LazyVec::LazyVec1(only_id, only_value) => {
                if *only_id == id {
                    if *only_value != Default::default() && *only_value != value {
                        return Err(IllegalSet::new(id, only_value.clone(), value));
                    }
                } else {
                    let len = usize::max(id, *only_id) + 1;
                    let mut vector = Vec::with_capacity(len + 1);
                    vector.resize(len, Default::default());
                    vector[id] = value;
                    vector[*only_id] = only_value.clone();
                    *self = LazyVec::LazyVecN(vector)
                }
                Ok(())
            }
            LazyVec::LazyVecN(vector) => {
                if vector.len() <= id {
                    vector.resize(id + 1, Default::default())
                }
                if vector[id] == Default::default() {
                    vector[id] = value
                } else if vector[id] != value {
                    return Err(IllegalSet::new(id, vector[id].clone(), value));
                }
                Ok(())
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

    pub(crate) fn update_or_set<F>(&mut self, id: usize, updater: F, default: A)
    where
        F: FnOnce(&mut A),
    {
        match self.get_mut(id) {
            Some(value) => updater(value),
            None => self
                .set(id, default)
                .expect("Set failed over a non existing value"),
        }
    }
}

#[cfg(test)]
mod lazy_vec_tests {
    use super::*;

    #[test]
    fn normal_operation() {
        let mut vec = LazyVec::<u32>::Empty;

        vec.set(5, 55).unwrap();
        vec.set(1, 11).unwrap();
        vec.set(8, 88).unwrap();
        assert_eq!(vec.get(5), Some(&55));
        assert_eq!(vec.get(1), Some(&11));
        assert_eq!(vec.get(0), Some(&0));
        assert_eq!(vec.get(10), None); // FIXME: this should return the default, 0, as well, there is no need to return Option from get()

        // FIXME: replace update_or_set() with update()
        // the behavior should be the same for both cases, because we should be able to assume that
        // any cell is prefilled with default values and can therefore be safely updated
        vec.update_or_set(6, |n| *n += 1, 66);
        assert_eq!(vec.get(6), Some(&1));
        vec.update_or_set(9, |n| *n += 1, 99);
        assert_eq!(vec.get(9), Some(&99));

        assert_eq!(vec.filled_ids(), vec![1, 5, 6, 8, 9]);
    }

    #[test]
    fn set_fails_if_present() {
        let mut vec = LazyVec::from(5, 55);
        let result = vec.set(5, 555);
        assert_eq!(result, Err(IllegalSet::new(5, 55, 555)))
    }
}
