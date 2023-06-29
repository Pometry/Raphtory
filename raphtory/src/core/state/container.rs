use std::any::Any;

use super::StateType;

pub trait DynArray: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
    fn clone_array(&self) -> Box<dyn DynArray>;
    fn copy_from(&mut self, other: &dyn DynArray);
    // used for map array
    fn copy_over(&mut self, ss: usize);
    fn reset(&mut self, ss: usize);
    fn iter_keys(&self) -> Box<dyn Iterator<Item = u64> + '_>;
    fn iter_keys_changed(&self, ss: usize) -> Box<dyn Iterator<Item = u64> + '_>;
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VecArray<T> {
    odd: Vec<T>,
    even: Vec<T>,
    zero: T,
}

impl<T> VecArray<T> {
    pub(crate) fn new(zero: T) -> Self {
        VecArray {
            odd: Vec::new(),
            even: Vec::new(),
            zero,
        }
    }

    pub(crate) fn current_mut(&mut self, ss: usize) -> &mut Vec<T> {
        if ss % 2 == 0 {
            &mut self.even
        } else {
            &mut self.odd
        }
    }

    pub(crate) fn current(&self, ss: usize) -> &Vec<T> {
        if ss % 2 == 0 {
            &self.even
        } else {
            &self.odd
        }
    }

    fn previous_mut(&mut self, ss: usize) -> &mut Vec<T> {
        if ss % 2 == 0 {
            &mut self.odd
        } else {
            &mut self.even
        }
    }

    pub(crate) fn zero(&self) -> &T {
        &self.zero
    }
}

#[inline]
pub fn merge_2_vecs<T: Clone, F: Fn(&mut T, &T)>(v1: &mut Vec<T>, v2: &Vec<T>, f: F) {
    let v1_len = v1.len();
    let v2_len = v2.len();

    if v2_len < v1_len {
        let v1_slice = &mut v1[0..v2_len];
        v1_slice
            .iter_mut()
            .zip(v2.iter())
            .for_each(|(v1, v2)| f(v1, v2));
    } else {
        let v2_slice = &v2[0..v1_len];
        v1.iter_mut()
            .zip(v2_slice.iter())
            .for_each(|(v1, v2)| f(v1, v2));

        v1.extend_from_slice(&v2[v1_len..]);
    }
}

impl<T: StateType> DynArray for VecArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_array(&self) -> Box<dyn DynArray> {
        Box::new(self.clone())
    }

    fn copy_from(&mut self, other: &dyn DynArray) {
        let other = other.as_any().downcast_ref::<VecArray<T>>().unwrap();

        merge_2_vecs(&mut self.even, &other.even, |a, b| *a = b.clone())
    }

    fn copy_over(&mut self, ss: usize) {
        let mut previous = vec![];

        std::mem::swap(self.previous_mut(ss), &mut previous);

        // put current into previous using the merge function
        merge_2_vecs(&mut previous, self.current(ss), |a, b| *a = b.clone());

        // put previous back into previous_mut
        std::mem::swap(self.previous_mut(ss), &mut previous);
    }

    fn reset(&mut self, ss: usize) {
        let zero = self.zero.clone();
        for v in self.previous_mut(ss).iter_mut() {
            *v = zero.clone();
        }
    }

    fn iter_keys(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        todo!()
    }

    fn iter_keys_changed(&self, _ss: usize) -> Box<dyn Iterator<Item = u64> + '_> {
        todo!()
    }
}
