use std::collections::HashMap;
use std::ops::Range;

use serde::{Serialize, Deserialize};

use crate::{tcell::TCell, Prop};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Props {
    pub(crate) prop_ids: HashMap<String, usize>,
    pub(crate) vertex_meta: Vec<TPropVec>,
    // attributes for props
    pub(crate) edge_meta: Vec<TPropVec>, // table of edges
}

impl Default for Props {
    fn default() -> Self {
        Self {
            prop_ids: Default::default(),
            vertex_meta: vec![Default::default()],
            // remote/local edges are encoded as i64 with negatives as remote and positives as local,
            // 0 breaks the symetry so we just ignore it
            edge_meta: vec![Default::default()],
        }
    }
}

impl Props {
    pub fn edges_len(&self) -> usize {
        self.edge_meta.len()
    }

    pub fn update_vertex_props(&mut self, index: usize, t: i64, props: &Vec<(String, Prop)>) {
        for (name, prop) in props {
            let property_id = match self.prop_ids.get(name) {
                Some(prop_id) => {
                    *prop_id
                }
                None => {
                    let id = self.prop_ids.len();
                    self.prop_ids.insert(name.to_string(), id);
                    id
                }
            };
            match self.vertex_meta.get_mut(index) {
                Some(vertex_props) => {
                    vertex_props.set(property_id, t, prop)
                }
                None => {
                    let prop_cell = TPropVec::from(property_id, t, prop);
                    self.vertex_meta.insert(index, prop_cell)
                }
            }
        }
    }

    pub fn update_edge_props(&mut self, src_edge_meta_id: usize, t: i64, props: &Vec<(String, Prop)>) {
        //FIXME: ensure the self.edge_meta is updated even if the props vector is null
        for (name, prop) in props {
            // find where do we slot this property in the temporal vec for each edge
            let property_id = if let Some(prop_id) = self.prop_ids.get(name) {
                // there is an existing prop set here
                *prop_id
            } else {
                // first time we see this prop
                let id = self.prop_ids.len();
                self.prop_ids.insert(name.to_string(), id);
                id
            };

            if let Some(edge_props) = self.edge_meta.get_mut(src_edge_meta_id) {
                edge_props.set(property_id, t, prop)
            } else {
                // we don't have metadata for this edge
                let prop_cell = TPropVec::from(property_id, t, prop);
                self.edge_meta.insert(src_edge_meta_id, prop_cell)
            }
        }
    }
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum TPropVec {
    #[default] Empty,
    One(usize, TProp),
    Props(Vec<TProp>),
}

impl TPropVec {

    pub(crate) fn from(i: usize, t: i64, p: &Prop) -> Self {
        TPropVec::One(i, TProp::from(t, p))
    }

    pub(crate) fn set(&mut self, i: usize, t: i64, p: &Prop) {
        match self {
            TPropVec::Empty => {
                *self = Self::from(i, t, p);
            },
            TPropVec::One(i0, p0) => {
                if i == *i0 {
                    p0.set(t, p);
                } else {
                    let mut props = vec![TProp::Empty; usize::max(i, *i0) + 1];
                    props[i] = TProp::from(t, p);
                    props[*i0] = p0.clone();
                    *self = TPropVec::Props(props);
                }
            }
            TPropVec::Props(props) => {
                if props.len() <= i {
                    props.resize(i + 1, TProp::Empty)
                }
                props[i].set(t, p);
            }
        }
    }

    pub(crate) fn iter(&self, i: usize) -> Box<dyn Iterator<Item = (&i64, Prop)> + '_> {
        match self {
            TPropVec::One(i0, p) if *i0 == i => p.iter(),
            TPropVec::Props(props) if props.len() > i => props[i].iter(),
            _ => Box::new(std::iter::empty()),
        }
    }

    pub(crate) fn iter_window(
        &self,
        i: usize,
        r: Range<i64>,
    ) -> Box<dyn Iterator<Item = (&i64, Prop)> + '_> {
        match self {
            TPropVec::One(i0, p) if *i0 == i => p.iter_window(r),
            TPropVec::Props(props) if props.len() >= i => props[i].iter_window(r),
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
    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = (&i64, Prop)> + '_> {
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

    pub(crate) fn iter_window(&self, r: Range<i64>) -> Box<dyn Iterator<Item = (&i64, Prop)> + '_> {
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
