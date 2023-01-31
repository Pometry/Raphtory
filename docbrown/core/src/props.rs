use crate::tpropvec::TPropVec;
use crate::Prop;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Props {
    // Mapping between property name and property id
    pub(crate) prop_ids: HashMap<String, usize>,

    // Vector of vertices properties. Each index represents vertex local (physical) id
    pub(crate) vertex_meta: Vec<TPropVec>,

    // Vector of edge properties. Each "signed" index represents an edge id
    pub(crate) edge_meta: Vec<TPropVec>,
}

impl Default for Props {
    fn default() -> Self {
        Self {
            prop_ids: Default::default(),
            vertex_meta: vec![],
            // Signed indices of "edge_meta" vector are used to denote edge ids. In particular, negative
            // and positive indices to denote remote and local edges, respectively. Here we have initialized
            // "edge_meta" with default value of "TPropVec::Empty" occupying the 0th index. The reason
            // being index "0" can be used to denote neither local nor remote edges. It simply breaks this
            // symmetry, hence we ignore it in our representation.
            edge_meta: vec![Default::default()],
        }
    }
}

impl Props {
    pub fn get_next_available_edge_id(&self) -> usize {
        self.edge_meta.len()
    }

    fn get_prop_id(&mut self, name: &str) -> usize {
        match self.prop_ids.get(name) {
            Some(prop_id) => *prop_id,
            None => {
                let id = self.prop_ids.len();
                self.prop_ids.insert(name.to_string(), id);
                id
            }
        }
    }

    pub fn upsert_vertex_props(&mut self, index: usize, t: i64, props: &Vec<(String, Prop)>) {
        if props.is_empty() {
            match self.vertex_meta.get_mut(index) {
                Some(_) => {}
                None => self.vertex_meta.insert(index, TPropVec::Empty),
            }
            return;
        }

        for (name, prop) in props {
            let prop_id = self.get_prop_id(name);

            match self.vertex_meta.get_mut(index) {
                Some(vertex_props) => vertex_props.set(prop_id, t, prop),
                None => self
                    .vertex_meta
                    .insert(index, TPropVec::from(prop_id, t, prop)),
            }
        }
    }

    pub fn upsert_edge_props(
        &mut self,
        src_edge_meta_id: usize,
        t: i64,
        props: &Vec<(String, Prop)>,
    ) {
        if props.is_empty() {
            match self.edge_meta.get_mut(src_edge_meta_id) {
                Some(_edge_props) => {}
                None => self.edge_meta.insert(src_edge_meta_id, TPropVec::Empty),
            }
            return;
        }

        for (name, prop) in props {
            let prop_id = self.get_prop_id(name);

            match self.edge_meta.get_mut(src_edge_meta_id) {
                Some(edge_props) => edge_props.set(prop_id, t, prop),
                None => self
                    .edge_meta
                    .insert(src_edge_meta_id, TPropVec::from(prop_id, t, prop)),
            }
        }
    }
}

#[cfg(test)]
mod props_tests {
    use super::*;

    #[test]
    fn zero_index_of_edge_meta_is_preassgined_default_value() {
        let Props {
            prop_ids,
            vertex_meta,
            edge_meta,
        } = Props::default();

        assert_eq!(edge_meta, vec![TPropVec::Empty]);
    }

    #[test]
    fn return_valid_next_available_edge_id() {
        let props = Props::default();

        // 0th index is not a valid edge id because it can't be used to correctly denote
        // both local as well as remote edge id. Hence edge ids must always start with 1.
        assert_ne!(props.get_next_available_edge_id(), 0);
        assert_eq!(props.get_next_available_edge_id(), 1);
    }

    #[test]
    fn return_prop_id_if_prop_name_found() {
        let mut props = Props::default();
        props.prop_ids.insert(String::from("key1"), 0);
        props.prop_ids.insert(String::from("key2"), 1);

        assert_eq!(props.get_prop_id("key2"), 1);
    }

    #[test]
    fn return_new_prop_id_if_prop_name_not_found() {
        let mut props = Props::default();
        assert_eq!(props.get_prop_id("key1"), 0);
        assert_eq!(props.get_prop_id("key2"), 1);
    }

    #[test]
    fn insert_default_value_against_no_props_vertex_upsert() {
        let mut props = Props::default();
        props.upsert_vertex_props(0, 1, &vec![]);

        assert_eq!(props.vertex_meta.get(0).unwrap(), &TPropVec::Empty)
    }

    #[test]
    fn insert_new_vertex_prop() {
        let mut props = Props::default();
        props.upsert_vertex_props(0, 1, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = props.get_prop_id("bla");
        assert_eq!(
            props
                .vertex_meta
                .get(0)
                .unwrap()
                .iter(prop_id)
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        )
    }

    #[test]
    fn update_existing_vertex_prop() {
        let mut props = Props::default();
        props.upsert_vertex_props(0, 1, &vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_vertex_props(0, 2, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = props.get_prop_id("bla");
        assert_eq!(
            props
                .vertex_meta
                .get(0)
                .unwrap()
                .iter(prop_id)
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10)), (&2, Prop::I32(10))]
        )
    }

    #[test]
    fn overwrites_existing_vertex_prop_if_exactly_same() {
        let mut props = Props::default();
        props.upsert_vertex_props(0, 1, &vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_vertex_props(0, 1, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = props.get_prop_id("bla");
        assert_eq!(
            props
                .vertex_meta
                .get(0)
                .unwrap()
                .iter(prop_id)
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        )
    }

    #[test]
    fn insert_default_value_against_no_props_edge_upsert() {
        let mut props = Props::default();
        props.upsert_edge_props(0, 1, &vec![]);

        assert_eq!(props.edge_meta.get(0).unwrap(), &TPropVec::Empty)
    }

    #[test]
    fn insert_new_edge_prop() {
        let mut props = Props::default();
        props.upsert_edge_props(1, 1, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = props.get_prop_id("bla");
        assert_eq!(
            props
                .edge_meta
                .get(1)
                .unwrap()
                .iter(prop_id)
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        )
    }

    #[test]
    fn update_existing_edge_prop() {
        let mut props = Props::default();
        props.upsert_edge_props(0, 1, &vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_edge_props(0, 2, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = props.get_prop_id("bla");
        assert_eq!(
            props
                .edge_meta
                .get(0)
                .unwrap()
                .iter(prop_id)
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10)), (&2, Prop::I32(10))]
        )
    }

    #[test]
    fn overwrites_existing_edge_prop_if_exactly_same() {
        let mut props = Props::default();
        props.upsert_edge_props(0, 1, &vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_edge_props(0, 1, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = props.get_prop_id("bla");
        assert_eq!(
            props
                .edge_meta
                .get(0)
                .unwrap()
                .iter(prop_id)
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        )
    }
}
