use std::ops::Range;
use serde::{Deserialize, Serialize};
use crate::Prop;
use crate::tprop::TProp;

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum TPropVec {
    #[default]
    Empty,
    // First tuple value in "TPropVec1" and indices in "TPropVecN" vector denote property id
    // values from "Props::prop_ids" hashmap
    TPropVec1(usize, TProp),
    TPropVecN(Vec<TProp>),
}

impl TPropVec {
    pub(crate) fn from(prop_id: usize, time: i64, prop: &Prop) -> Self {
        TPropVec::TPropVec1(prop_id, TProp::from(time, prop))
    }

    pub(crate) fn set(&mut self, prop_id: usize, time: i64, prop: &Prop) {
        match self {
            TPropVec::Empty => {
                *self = Self::from(prop_id, time, prop);
            }
            TPropVec::TPropVec1(prop_id0, prop0) => {
                if *prop_id0 == prop_id {
                    prop0.set(time, prop);
                } else {
                    let mut props = vec![TProp::Empty; usize::max(prop_id, *prop_id0) + 1];
                    props[prop_id] = TProp::from(time, prop);
                    props[*prop_id0] = prop0.clone();
                    *self = TPropVec::TPropVecN(props);
                }
            }
            TPropVec::TPropVecN(props) => {
                if props.len() <= prop_id {
                    props.resize(prop_id + 1, TProp::Empty)
                }
                props[prop_id].set(time, prop);
            }
        }
    }

    pub(crate) fn iter(&self, prop_id: usize) -> Box<dyn Iterator<Item = (&i64, Prop)> + '_> {
        match self {
            TPropVec::TPropVec1(prop_id0, prop0) if *prop_id0 == prop_id => prop0.iter(),
            TPropVec::TPropVecN(props) if props.len() > prop_id => props[prop_id].iter(),
            _ => Box::new(std::iter::empty()),
        }
    }

    pub(crate) fn iter_window(
        &self,
        prop_id: usize,
        r: Range<i64>,
    ) -> Box<dyn Iterator<Item = (&i64, Prop)> + '_> {
        match self {
            TPropVec::TPropVec1(prop_id0, prop0) if *prop_id0 == prop_id => prop0.iter_window(r),
            TPropVec::TPropVecN(props) if props.len() >= prop_id => props[prop_id].iter_window(r),
            _ => Box::new(std::iter::empty()),
        }
    }
}
