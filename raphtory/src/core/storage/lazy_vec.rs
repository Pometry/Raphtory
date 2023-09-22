use crate::core::utils::errors::GraphError;
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

    pub(crate) fn filled_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        match self {
            LazyVec::Empty => Box::new(iter::empty()),
            LazyVec::LazyVec1(id, _) => Box::new(iter::once(*id)),
            LazyVec::LazyVecN(vector) => Box::new(
                vector
                    .iter()
                    .enumerate()
                    .filter(|&(_, value)| *value != Default::default())
                    .map(|(id, _)| id),
            ),
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

    pub(crate) fn update<F>(&mut self, id: usize, updater: F) -> Result<(), GraphError>
    where
        F: FnOnce(&mut A) -> Result<(), GraphError>,
    {
        match self.get_mut(id) {
            Some(value) => updater(value)?,
            None => {
                let mut value = A::default();
                updater(&mut value)?;
                self.set(id, value)
                    .expect("Set failed over a non existing value")
            }
        };
        Ok(())
    }
}

#[cfg(test)]
mod lazy_vec_tests {
    use super::*;
    use itertools::Itertools;

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

        vec.update(6, |n| Ok(*n += 1));
        assert_eq!(vec.get(6), Some(&1));
        vec.update(9, |n| Ok(*n += 1));
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
