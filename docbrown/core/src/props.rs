use std::collections::HashMap;
use std::ops::Range;

use serde::{Serialize, Deserialize};

use crate::{tcell::TCell, Prop};

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
    pub fn edges_len(&self) -> usize {
        self.edge_meta.len()
    }

    fn get_prop_id(&mut self, name: &str) -> usize {
        match self.prop_ids.get(name) {
            Some(prop_id) => {
                *prop_id
            }
            None => {
                let id = self.prop_ids.len();
                self.prop_ids.insert(name.to_string(), id);
                id
            }
        }
    }

    pub fn update_vertex_props(&mut self, index: usize, t: i64, props: &Vec<(String, Prop)>) {
        for (name, prop) in props {
            let prop_id = self.get_prop_id(name);

            match self.vertex_meta.get_mut(index) {
                Some(vertex_props) => {
                    vertex_props.set(prop_id, t, prop)
                }
                None => {
                    let prop_cell = TPropVec::from(prop_id, t, prop);
                    self.vertex_meta.insert(index, prop_cell)
                }
            }
        }
    }

    pub fn update_edge_props(&mut self, src_edge_meta_id: usize, t: i64, props: &Vec<(String, Prop)>) {
        // FIXME: ensure the self.edge_meta is updated even if the props vector is null
        for (name, prop) in props {
            let prop_id = self.get_prop_id(name);

            match self.edge_meta.get_mut(src_edge_meta_id) {
                Some(edge_props) => {
                    edge_props.set(prop_id, t, prop)
                }
                None => {
                    let prop_cell = TPropVec::from(prop_id, t, prop);
                    self.edge_meta.insert(src_edge_meta_id, prop_cell)
                }
            }
        }
    }
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum TPropVec {
    #[default] Empty,
    // First tuple value in "SingleProp" and indices in "MultiPropVec" vector denote property id
    // values from "Props::prop_ids" hashmap
    SingleProp(usize, TProp),
    MultiPropVec(Vec<TProp>),
}

impl TPropVec {
    pub(crate) fn from(prop_id: usize, time: i64, prop: &Prop) -> Self {
        TPropVec::SingleProp(prop_id, TProp::from(time, prop))
    }

    pub(crate) fn set(&mut self, prop_id: usize, time: i64, prop: &Prop) {
        match self {
            TPropVec::Empty => {
                *self = Self::from(prop_id, time, prop);
            }
            TPropVec::SingleProp(prop_id0, prop0) => {
                if  *prop_id0 == prop_id {
                    prop0.set(time, prop);
                } else {
                    let mut props = vec![TProp::Empty; usize::max(prop_id, *prop_id0) + 1];
                    props[prop_id] = TProp::from(time, prop);
                    props[*prop_id0] = prop0.clone();
                    *self = TPropVec::MultiPropVec(props);
                }
            }
            TPropVec::MultiPropVec(props) => {
                if props.len() <= prop_id {
                    props.resize(prop_id + 1, TProp::Empty)
                }
                props[prop_id].set(time, prop);
            }
        }
    }

    pub(crate) fn iter(&self, prop_id: usize) -> Box<dyn Iterator<Item=(&i64, Prop)> + '_> {
        match self {
            TPropVec::SingleProp(prop_id0, prop0) if *prop_id0 == prop_id => prop0.iter(),
            TPropVec::MultiPropVec(props) if props.len() > prop_id => props[prop_id].iter(),
            _ => Box::new(std::iter::empty()),
        }
    }

    pub(crate) fn iter_window(
        &self,
        prop_id: usize,
        r: Range<i64>,
    ) -> Box<dyn Iterator<Item=(&i64, Prop)> + '_> {
        match self {
            TPropVec::SingleProp(prop_id0, prop0) if *prop_id0 == prop_id => prop0.iter_window(r),
            TPropVec::MultiPropVec(props) if props.len() >= prop_id => props[prop_id].iter_window(r),
            _ => Box::new(std::iter::empty()),
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub(crate) enum TProp {
    #[default]
    Empty,
    Str(TCell<String>),
    I32(TCell<i32>),
    I64(TCell<i64>),
    U32(TCell<u32>),
    U64(TCell<u64>),
    F32(TCell<f32>),
    F64(TCell<f64>),
}

impl TProp {
    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item=(&i64, Prop)> + '_> {
        match self {
            TProp::Str(cell) => Box::new(cell.iter_t().map(|(t, s)| (t, Prop::Str(s.to_string())))),
            TProp::I32(cell) => Box::new(cell.iter_t().map(|(t, n)| (t, Prop::I32(*n)))),
            TProp::I64(cell) => Box::new(cell.iter_t().map(|(t, n)| (t, Prop::I64(*n)))),
            TProp::U32(cell) => Box::new(cell.iter_t().map(|(t, n)| (t, Prop::U32(*n)))),
            TProp::U64(cell) => Box::new(cell.iter_t().map(|(t, n)| (t, Prop::U64(*n)))),
            TProp::F32(cell) => Box::new(cell.iter_t().map(|(t, n)| (t, Prop::F32(*n)))),
            TProp::F64(cell) => Box::new(cell.iter_t().map(|(t, n)| (t, Prop::F64(*n)))),
            _ => todo!(),
        }
    }

    pub(crate) fn iter_window(&self, r: Range<i64>) -> Box<dyn Iterator<Item=(&i64, Prop)> + '_> {
        match self {
            TProp::Str(cell) => Box::new(
                cell.iter_window_t(r)
                    .map(|(t, s)| (t, Prop::Str(s.to_string()))),
            ),
            TProp::I32(cell) => Box::new(cell.iter_window_t(r).map(|(t, n)| (t, Prop::I32(*n)))),
            TProp::I64(cell) => Box::new(cell.iter_window_t(r).map(|(t, n)| (t, Prop::I64(*n)))),
            TProp::U32(cell) => Box::new(cell.iter_window_t(r).map(|(t, n)| (t, Prop::U32(*n)))),
            TProp::U64(cell) => Box::new(cell.iter_window_t(r).map(|(t, n)| (t, Prop::U64(*n)))),
            TProp::F32(cell) => Box::new(cell.iter_window_t(r).map(|(t, n)| (t, Prop::F32(*n)))),
            TProp::F64(cell) => Box::new(cell.iter_window_t(r).map(|(t, n)| (t, Prop::F64(*n)))),
            _ => todo!(),
        }
    }

    pub(crate) fn from(t: i64, p: &Prop) -> Self {
        match p {
            Prop::Str(a) => TProp::Str(TCell::new(t, a.to_string())),
            Prop::I32(a) => TProp::I32(TCell::new(t, *a)),
            Prop::I64(a) => TProp::I64(TCell::new(t, *a)),
            Prop::U32(a) => TProp::U32(TCell::new(t, *a)),
            Prop::U64(a) => TProp::U64(TCell::new(t, *a)),
            Prop::F32(a) => TProp::F32(TCell::new(t, *a)),
            Prop::F64(a) => TProp::F64(TCell::new(t, *a)),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            TProp::Empty => true,
            _ => false,
        }
    }

    pub(crate) fn set(&mut self, t: i64, p: &Prop) {
        if self.is_empty() {
            *self = TProp::from(t, p);
        } else {
            match self {
                TProp::Empty => todo!(),
                TProp::Str(cell) => {
                    if let Prop::Str(a) = p {
                        cell.set(t, a.to_string());
                    }
                }
                TProp::I32(cell) => {
                    if let Prop::I32(a) = p {
                        cell.set(t, *a);
                    }
                }
                TProp::I64(cell) => {
                    if let Prop::I64(a) = p {
                        cell.set(t, *a);
                    }
                }
                TProp::U32(cell) => {
                    if let Prop::U32(a) = p {
                        cell.set(t, *a);
                    }
                }
                TProp::U64(cell) => {
                    if let Prop::U64(a) = p {
                        cell.set(t, *a);
                    }
                }
                TProp::F32(cell) => {
                    if let Prop::F32(a) = p {
                        cell.set(t, *a);
                    }
                }
                TProp::F64(cell) => {
                    if let Prop::F64(a) = p {
                        cell.set(t, *a);
                    }
                }
            }
        }
    }
}
