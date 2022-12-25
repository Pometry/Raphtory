use std::ops::Range;

use crate::{tcell::TCell, Prop};

#[derive(Debug, Default, PartialEq)]
pub struct TPropVec {
    props: Vec<TProp>,
}

impl TPropVec {
    pub(crate) fn from(i: usize, t: u64, p: Prop) -> Self {
        let mut props = vec![TProp::Empty; i + 1];
        props.insert(i, TProp::from(t, p));
        TPropVec { props }
    }

    pub(crate) fn set(&mut self, i: usize, t: u64, p: Prop) {
        if self.props.len() <= i {
            self.props.resize(i + 1, TProp::Empty)
        }

        self.props[i].set(t, p);
    }

    pub(crate) fn iter(&self, i: usize) -> Box<dyn Iterator<Item = (&u64, Prop)> + '_> {
        self.props[i].iter()
    }

    pub(crate) fn iter_window(
        &self,
        i: usize,
        r: Range<u64>,
    ) -> Box<dyn Iterator<Item = (&u64, Prop)> + '_> {
        self.props[i].iter_window(r)
    }
}
#[derive(Debug, Default, PartialEq, Clone)]
pub enum TProp {
    #[default]
    Empty,
    Str(TCell<String>),
    U32(TCell<u32>),
    U64(TCell<u64>),
    F32(TCell<f32>),
    F64(TCell<f64>),
}

impl TProp {
    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = (&u64, Prop)> + '_> {
        match self {
            TProp::Str(cell) => Box::new(cell.iter_t().map(|(t, s)| (t, Prop::Str(s.to_string())))),
            TProp::U32(cell) => Box::new(cell.iter_t().map(|(t, n)| (t, Prop::U32(*n)))),
            TProp::U64(cell) => Box::new(cell.iter_t().map(|(t, n)| (t, Prop::U64(*n)))),
            TProp::F32(cell) => Box::new(cell.iter_t().map(|(t, n)| (t, Prop::F32(*n)))),
            TProp::F64(cell) => Box::new(cell.iter_t().map(|(t, n)| (t, Prop::F64(*n)))),
            _ => todo!(),
        }
    }

    pub(crate) fn iter_window(&self, r: Range<u64>) -> Box<dyn Iterator<Item = (&u64, Prop)> + '_> {
        match self {
            TProp::Str(cell) => Box::new(
                cell.iter_window_t(r)
                    .map(|(t, s)| (t, Prop::Str(s.to_string()))),
            ),
            TProp::U32(cell) => Box::new(cell.iter_window_t(r).map(|(t, n)| (t, Prop::U32(*n)))),
            TProp::U64(cell) => Box::new(cell.iter_window_t(r).map(|(t, n)| (t, Prop::U64(*n)))),
            TProp::F32(cell) => Box::new(cell.iter_window_t(r).map(|(t, n)| (t, Prop::F32(*n)))),
            TProp::F64(cell) => Box::new(cell.iter_window_t(r).map(|(t, n)| (t, Prop::F64(*n)))),
            _ => todo!(),
        }
    }

    pub(crate) fn from(t: u64, p: Prop) -> Self {
        match p {
            Prop::Str(a) => TProp::Str(TCell::new(t, a)),
            Prop::U32(a) => TProp::U32(TCell::new(t, a)),
            Prop::U64(a) => TProp::U64(TCell::new(t, a)),
            Prop::F32(a) => TProp::F32(TCell::new(t, a)),
            Prop::F64(a) => TProp::F64(TCell::new(t, a)),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            TProp::Empty => true,
            _ => false,
        }
    }

    pub(crate) fn set(&mut self, t: u64, p: Prop) {
        if self.is_empty() {
            *self = TProp::from(t, p);
        } else {
            match self {
                TProp::Empty => todo!(),
                TProp::Str(cell) => {
                    if let Prop::Str(a) = p {
                        cell.set(t, a);
                    }
                }
                TProp::U32(cell) => {
                    if let Prop::U32(a) = p {
                        cell.set(t, a);
                    }
                }
                TProp::U64(cell) => {
                    if let Prop::U64(a) = p {
                        cell.set(t, a);
                    }
                }
                TProp::F32(cell) => {
                    if let Prop::F32(a) = p {
                        cell.set(t, a);
                    }
                }
                TProp::F64(cell) => {
                    if let Prop::F64(a) = p {
                        cell.set(t, a);
                    }
                }
            }
        }
    }
}
