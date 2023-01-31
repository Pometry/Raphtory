use crate::Prop;
use crate::tcell::TCell;
use std::ops::Range;
use serde::{Deserialize, Serialize};

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
            TProp::Str(cell) => Box::new(
                cell.iter_t()
                    .map(|(t, value)| (t, Prop::Str(value.to_string()))),
            ),
            TProp::I32(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::I32(*value)))),
            TProp::I64(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::I64(*value)))),
            TProp::U32(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::U32(*value)))),
            TProp::U64(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::U64(*value)))),
            TProp::F32(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::F32(*value)))),
            TProp::F64(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::F64(*value)))),
            _ => todo!(),
        }
    }

    pub(crate) fn iter_window(&self, r: Range<i64>) -> Box<dyn Iterator<Item = (&i64, Prop)> + '_> {
        match self {
            TProp::Str(cell) => Box::new(
                cell.iter_window_t(r)
                    .map(|(t, value)| (t, Prop::Str(value.to_string()))),
            ),
            TProp::I32(cell) => Box::new(
                cell.iter_window_t(r)
                    .map(|(t, value)| (t, Prop::I32(*value))),
            ),
            TProp::I64(cell) => Box::new(
                cell.iter_window_t(r)
                    .map(|(t, value)| (t, Prop::I64(*value))),
            ),
            TProp::U32(cell) => Box::new(
                cell.iter_window_t(r)
                    .map(|(t, value)| (t, Prop::U32(*value))),
            ),
            TProp::U64(cell) => Box::new(
                cell.iter_window_t(r)
                    .map(|(t, value)| (t, Prop::U64(*value))),
            ),
            TProp::F32(cell) => Box::new(
                cell.iter_window_t(r)
                    .map(|(t, value)| (t, Prop::F32(*value))),
            ),
            TProp::F64(cell) => Box::new(
                cell.iter_window_t(r)
                    .map(|(t, value)| (t, Prop::F64(*value))),
            ),
            _ => todo!(),
        }
    }

    pub(crate) fn from(t: i64, prop: &Prop) -> Self {
        match prop {
            Prop::Str(value) => TProp::Str(TCell::new(t, value.to_string())),
            Prop::I32(value) => TProp::I32(TCell::new(t, *value)),
            Prop::I64(value) => TProp::I64(TCell::new(t, *value)),
            Prop::U32(value) => TProp::U32(TCell::new(t, *value)),
            Prop::U64(value) => TProp::U64(TCell::new(t, *value)),
            Prop::F32(value) => TProp::F32(TCell::new(t, *value)),
            Prop::F64(value) => TProp::F64(TCell::new(t, *value)),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            TProp::Empty => true,
            _ => false,
        }
    }

    pub(crate) fn set(&mut self, t: i64, prop: &Prop) {
        if self.is_empty() {
            *self = TProp::from(t, prop);
        } else {
            match self {
                TProp::Empty => todo!(),
                TProp::Str(cell) => {
                    if let Prop::Str(a) = prop {
                        cell.set(t, a.to_string());
                    }
                }
                TProp::I32(cell) => {
                    if let Prop::I32(a) = prop {
                        cell.set(t, *a);
                    }
                }
                TProp::I64(cell) => {
                    if let Prop::I64(a) = prop {
                        cell.set(t, *a);
                    }
                }
                TProp::U32(cell) => {
                    if let Prop::U32(a) = prop {
                        cell.set(t, *a);
                    }
                }
                TProp::U64(cell) => {
                    if let Prop::U64(a) = prop {
                        cell.set(t, *a);
                    }
                }
                TProp::F32(cell) => {
                    if let Prop::F32(a) = prop {
                        cell.set(t, *a);
                    }
                }
                TProp::F64(cell) => {
                    if let Prop::F64(a) = prop {
                        cell.set(t, *a);
                    }
                }
            }
        }
    }
}
