use crate::core::lazy_vec::{IllegalSet, LazyVec};
use crate::core::tprop::TProp;
use crate::core::Prop;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("cannot mutate static property '{name}'")]
pub struct IllegalMutate {
    pub name: String,
    pub source: IllegalSet<Option<Prop>>,
}

impl IllegalMutate {
    pub(crate) fn from_source(source: IllegalSet<Option<Prop>>, prop: &str) -> IllegalMutate {
        let id = PropId::Static(source.index);
        IllegalMutate {
            name: prop.to_string(),
            source,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
enum PropId {
    Static(usize),
    Temporal(usize),
}

impl PropId {
    #[allow(dead_code)]
    pub(crate) fn new(id: usize, static_: bool) -> PropId {
        if static_ {
            PropId::Static(id)
        } else {
            PropId::Temporal(id)
        }
    }
    pub(crate) fn get_id(&self) -> usize {
        match self {
            PropId::Static(id) => *id,
            PropId::Temporal(id) => *id,
        }
    }
    pub(crate) fn is_static(&self) -> bool {
        match self {
            PropId::Static(_) => true,
            PropId::Temporal(_) => false,
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Props {
    // Mapping between property name and property id
    prop_ids: HashMap<String, PropId>, // TODO: change name back to prop_ids

    // Vector of vertices properties. Each index represents vertex local (physical) id
    static_props: Vec<LazyVec<Option<Prop>>>,
    temporal_props: Vec<LazyVec<TProp>>,
}

impl Props {
    // GETTERS:

    fn get_prop_id(&self, name: &str, should_be_static: bool) -> Option<usize> {
        match self.prop_ids.get(name) {
            Some(prop_id) if prop_id.is_static() == should_be_static => Some(prop_id.get_id()),
            _ => None,
        }
    }

    #[allow(unused_variables)]
    fn reverse_id(&self, id: &PropId) -> &str {
        self.prop_ids.iter().find(|&(k, v)| v == id).unwrap().0
    }

    fn get_or_default<A>(&self, vector: &Vec<LazyVec<A>>, id: usize, name: &str) -> A
    where
        A: PartialEq + Default + Clone + Debug,
    {
        match self.get_prop_id(name, true) {
            Some(prop_id) => {
                let props = vector.get(id).unwrap_or(&LazyVec::Empty);
                props.get(prop_id).cloned().unwrap_or(Default::default())
            }
            None => Default::default(),
        }
    }

    pub(crate) fn static_prop(&self, id: usize, name: &str) -> Option<Prop> {
        self.get_or_default(&self.static_props, id, name)
    }

    pub(crate) fn temporal_prop(&self, id: usize, name: &str) -> Option<&TProp> {
        // TODO: we should be able to use self.get_or_default() here
        let prop_id = self.get_prop_id(name, false)?;
        let props = self.temporal_props.get(id).unwrap_or(&LazyVec::Empty);
        props.get(prop_id)
    }

    fn get_names<A>(
        &self,
        vector: &Vec<LazyVec<A>>,
        id: usize,
        should_be_static: bool,
    ) -> Vec<String>
    where
        A: Clone + Default + PartialEq + Debug,
    {
        match vector.get(id) {
            Some(props) => {
                let ids = props.filled_ids().into_iter();
                if should_be_static {
                    ids.map(|id| self.reverse_id(&PropId::Static(id)).to_string())
                        .collect_vec()
                } else {
                    ids.map(|id| self.reverse_id(&PropId::Temporal(id)).to_string())
                        .collect_vec()
                }
            }
            None => vec![],
        }
    }

    pub fn static_names(&self, id: usize) -> Vec<String> {
        self.get_names(&self.static_props, id, true)
    }

    pub fn temporal_names(&self, id: usize) -> Vec<String> {
        self.get_names(&self.temporal_props, id, false)
    }

    // SETTERS:

    fn grow_and_get_slot<A>(vector: &mut Vec<A>, id: usize) -> &mut A
    where
        A: Default,
    {
        if vector.len() <= id {
            vector.resize_with(id + 1, || Default::default());
        }
        // now props_storage.len() >= id + 1:
        vector.get_mut(id).unwrap()
    }

    fn get_or_allocate_id(&mut self, name: String, should_be_static: bool) -> Result<usize, ()> {
        match self.prop_ids.get(&name) {
            None => {
                let new_prop_id = if should_be_static {
                    let static_prop_ids = self.prop_ids.iter().filter(|&(_, v)| v.is_static());
                    let new_id = static_prop_ids.count();
                    PropId::Static(new_id)
                } else {
                    let static_prop_ids = self.prop_ids.iter().filter(|&(_, v)| !v.is_static());
                    let new_id = static_prop_ids.count();
                    PropId::Temporal(new_id)
                };
                self.prop_ids.insert(name, new_prop_id.clone());
                Ok(new_prop_id.get_id())
            }
            Some(id) if id.is_static() == should_be_static => Ok(id.get_id()),
            _ => Err(()),
        }
    }

    fn translate_props(
        &mut self,
        props: Vec<(String, Prop)>,
        should_be_static: bool,
    ) -> Vec<(usize, Prop)> {
        // TODO: return Result
        props
            .into_iter()
            .map(|(name, prop)| {
                (
                    self.get_or_allocate_id(name, should_be_static).unwrap(),
                    prop,
                )
            })
            .collect_vec()
    }

    pub fn upsert_temporal_props(&mut self, t: i64, id: usize, props: Vec<(String, Prop)>) {
        if !props.is_empty() {
            let translated_props = self.translate_props(props, false);
            let vertex_slot: &mut LazyVec<TProp> =
                Self::grow_and_get_slot(&mut self.temporal_props, id);
            for (prop_id, prop) in translated_props {
                vertex_slot.update_or_set(prop_id, |p| p.set(t, &prop), TProp::from(t, &prop));
            }
        }
    }

    // pub fn set_static_props(
    //     &mut self,
    //     id: usize,
    //     props: Vec<(String, Prop)>,
    // ) -> Result<(), IllegalMutate> {
    //     if !props.is_empty() {
    //         let translated_props = self.translate_props(props, true);
    //         let vertex_slot: &mut LazyVec<Option<Prop>> =
    //             Self::grow_and_get_slot(&mut self.static_props, id);
    //         for (prop_id, prop) in translated_props {
    //             if let Err(e) = vertex_slot.set(prop_id, Some(prop)) {
    //                 return Err(IllegalMutate::from(e, &self));
    //             }
    //         }
    //     }
    //     Ok(())
    // }
}

#[cfg(test)]
mod props_tests {
    use super::*;

    #[test]
    fn return_prop_id_if_prop_name_found() {
        let mut props = Props::default();
        props
            .prop_ids
            .insert(String::from("key1"), PropId::Temporal(0));
        props
            .prop_ids
            .insert(String::from("key2"), PropId::Temporal(1));

        assert_eq!(props.get_or_allocate_id("key2".to_string(), false), Ok(1));
    }

    #[test]
    fn return_new_prop_id_if_prop_name_not_found() {
        let mut props = Props::default();
        assert_eq!(props.get_or_allocate_id("key1".to_string(), false), Ok(0));
        assert_eq!(props.get_or_allocate_id("key2".to_string(), false), Ok(1));
    }

    #[test]
    fn insert_new_vertex_prop() {
        let mut props = Props::default();
        props.upsert_temporal_props(1, 0, vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = props.get_or_allocate_id("bla".to_string(), false).unwrap();
        assert_eq!(
            props
                .temporal_props
                .get(0)
                .unwrap()
                .get(prop_id)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![(1, Prop::I32(10))]
        )
    }

    #[test]
    fn update_existing_vertex_prop() {
        let mut props = Props::default();
        props.upsert_temporal_props(1, 0, vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_temporal_props(2, 0, vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = props.get_or_allocate_id("bla".to_string(), false).unwrap();
        assert_eq!(
            props
                .temporal_props
                .get(0)
                .unwrap()
                .get(prop_id)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![(1, Prop::I32(10)), (2, Prop::I32(10))]
        )
    }

    #[test]
    fn new_update_with_the_same_time_to_a_vertex_prop_is_ignored() {
        let mut props = Props::default();
        props.upsert_temporal_props(1, 0, vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_temporal_props(1, 0, vec![("bla".to_string(), Prop::I32(20))]);

        let prop_id = props.get_or_allocate_id("bla".to_string(), false).unwrap();
        assert_eq!(
            props
                .temporal_props
                .get(0)
                .unwrap()
                .get(prop_id)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![(1, Prop::I32(10))]
        )
    }
}
