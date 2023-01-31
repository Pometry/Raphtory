use crate::tprop::TProp;
use crate::Prop;
use serde::{Deserialize, Serialize};
use std::ops::Range;

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

#[cfg(test)]
mod tpropvec_tests {
    use super::*;

    #[test]
    fn set_new_prop_for_tpropvec_initialized_as_empty() {
        let mut tpropvec = TPropVec::Empty;
        let prop_id = 1;
        tpropvec.set(prop_id, 1, &Prop::I32(10));

        assert_eq!(
            tpropvec.iter(prop_id).collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        );
    }

    #[test]
    fn set_multiple_props() {
        let mut tpropvec = TPropVec::from(1, 1, &Prop::Str("Pometry".into()));
        tpropvec.set(2, 2, &Prop::I32(2022));
        tpropvec.set(3, 3, &Prop::Str("Graph".into()));

        assert_eq!(
            tpropvec.iter(1).collect::<Vec<_>>(),
            vec![(&1, Prop::Str("Pometry".into()))]
        );
        assert_eq!(
            tpropvec.iter(2).collect::<Vec<_>>(),
            vec![(&2, Prop::I32(2022))]
        );
        assert_eq!(
            tpropvec.iter(3).collect::<Vec<_>>(),
            vec![(&3, Prop::Str("Graph".into()))]
        );
    }

    #[test]
    fn every_new_update_to_the_same_prop_is_recorded_as_history() {
        let mut tpropvec = TPropVec::from(1, 1, &Prop::Str("Pometry".into()));
        tpropvec.set(1, 2, &Prop::Str("Pometry Inc.".into()));

        let prop1 = tpropvec.iter(1).collect::<Vec<_>>();
        assert_eq!(
            prop1,
            vec![
                (&1, Prop::Str("Pometry".into())),
                (&2, Prop::Str("Pometry Inc.".into()))
            ]
        );
    }

    #[test]
    fn new_update_with_the_same_time_to_a_prop_is_ignored() {
        let mut tpropvec = TPropVec::from(1, 1, &Prop::Str("Pometry".into()));
        tpropvec.set(1, 1, &Prop::Str("Pometry Inc.".into()));

        let prop1 = tpropvec.iter(1).collect::<Vec<_>>();
        assert_eq!(prop1, vec![(&1, Prop::Str("Pometry".into()))]);
    }

    #[test]
    fn updates_to_every_prop_can_be_iterated() {
        let mut tpropvec = TPropVec::from(1, 1, &Prop::Str("Pometry".into()));
        tpropvec.set(1, 2, &Prop::Str("Pometry Inc.".into()));
        tpropvec.set(2, 3, &Prop::I32(2022));
        tpropvec.set(3, 4, &Prop::Str("Graph".into()));
        tpropvec.set(3, 5, &Prop::Str("Graph Analytics".into()));

        let prop1 = tpropvec.iter(1).collect::<Vec<_>>();
        assert_eq!(
            prop1,
            vec![
                (&1, Prop::Str("Pometry".into())),
                (&2, Prop::Str("Pometry Inc.".into()))
            ]
        );

        // Windowed iteration
        let prop3 = tpropvec.iter(3).collect::<Vec<_>>();
        assert_eq!(
            prop3,
            vec![
                (&4, Prop::Str("Graph".into())),
                (&5, Prop::Str("Graph Analytics".into()))
            ]
        );
    }

    #[test]
    fn updates_to_every_prop_can_be_window_iterated() {
        let mut tpropvec = TPropVec::from(1, 1, &Prop::Str("Pometry".into()));
        tpropvec.set(1, 2, &Prop::Str("Pometry Inc.".into()));
        tpropvec.set(2, 3, &Prop::I32(2022));
        tpropvec.set(3, 4, &Prop::Str("Graph".into()));
        tpropvec.set(3, 5, &Prop::Str("Graph Analytics".into()));

        let prop1 = tpropvec.iter_window(1, 1..3).collect::<Vec<_>>();
        assert_eq!(
            prop1,
            vec![
                (&1, Prop::Str("Pometry".into())),
                (&2, Prop::Str("Pometry Inc.".into()))
            ]
        );

        // Windowed iteration
        let prop3 = tpropvec.iter_window(3, 5..6).collect::<Vec<_>>();
        assert_eq!(prop3, vec![(&5, Prop::Str("Graph Analytics".into()))]);
    }
}
