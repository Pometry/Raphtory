use crate::{
    core::{
        entities::properties::tcell::TCell,
        storage::{timeindex::TimeIndexEntry, TPropColumn},
        utils::errors::GraphError,
        Prop, PropArray,
    },
    db::api::storage::graph::tprop_storage_ops::TPropOps,
};
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use raphtory_api::{
    core::storage::arc_str::ArcStr,
    iter::{BoxedLDIter, BoxedLIter},
};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{iter, ops::Range, sync::Arc};

#[derive(Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub enum TProp {
    #[default]
    Empty,
    Str(TCell<ArcStr>),
    U8(TCell<u8>),
    U16(TCell<u16>),
    I32(TCell<i32>),
    I64(TCell<i64>),
    U32(TCell<u32>),
    U64(TCell<u64>),
    F32(TCell<f32>),
    F64(TCell<f64>),
    Bool(TCell<bool>),
    DTime(TCell<DateTime<Utc>>),
    Array(TCell<PropArray>),
    NDTime(TCell<NaiveDateTime>),
    List(TCell<Arc<Vec<Prop>>>),
    Map(TCell<Arc<FxHashMap<ArcStr, Prop>>>),
    Decimal(TCell<BigDecimal>),
}

#[derive(Copy, Clone, Debug)]
pub struct TPropCell<'a> {
    t_cell: Option<&'a TCell<Option<usize>>>,
    log: Option<&'a TPropColumn>,
}

impl<'a> TPropCell<'a> {
    pub(crate) fn new(t_cell: &'a TCell<Option<usize>>, log: Option<&'a TPropColumn>) -> Self {
        Self {
            t_cell: Some(t_cell),
            log,
        }
    }

    fn iter_window_inner(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        self.t_cell.into_iter().flat_map(move |t_cell| {
            t_cell
                .iter_window(r.clone())
                .filter_map(move |(t, &id)| self.log?.get(id?).map(|prop| (*t, prop)))
        })
    }
}

impl<'a> TPropOps<'a> for TPropCell<'a> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.iter_window_inner(TimeIndexEntry::MIN..t).next_back()
    }

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        self.t_cell.into_iter().flat_map(move |t_cell| {
            t_cell
                .iter()
                .filter_map(move |(t, &id)| self.log?.get(id?).map(|prop| (*t, prop)))
        })
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        self.iter_window_inner(r)
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        self.t_cell?.at(ti).and_then(|&id| self.log?.get(id?))
    }
}

impl TProp {
    pub(crate) fn from(t: TimeIndexEntry, prop: Prop) -> Self {
        match prop {
            Prop::Str(value) => TProp::Str(TCell::new(t, value)),
            Prop::I32(value) => TProp::I32(TCell::new(t, value)),
            Prop::I64(value) => TProp::I64(TCell::new(t, value)),
            Prop::U8(value) => TProp::U8(TCell::new(t, value)),
            Prop::U16(value) => TProp::U16(TCell::new(t, value)),
            Prop::U32(value) => TProp::U32(TCell::new(t, value)),
            Prop::U64(value) => TProp::U64(TCell::new(t, value)),
            Prop::F32(value) => TProp::F32(TCell::new(t, value)),
            Prop::F64(value) => TProp::F64(TCell::new(t, value)),
            Prop::Bool(value) => TProp::Bool(TCell::new(t, value)),
            Prop::DTime(value) => TProp::DTime(TCell::new(t, value)),
            Prop::NDTime(value) => TProp::NDTime(TCell::new(t, value)),
            Prop::Array(value) => TProp::Array(TCell::new(t, value)),
            Prop::List(value) => TProp::List(TCell::new(t, value)),
            Prop::Map(value) => TProp::Map(TCell::new(t, value)),
            Prop::Decimal(value) => TProp::Decimal(TCell::new(t, value)),
        }
    }

    pub(crate) fn set(&mut self, t: TimeIndexEntry, prop: Prop) -> Result<(), GraphError> {
        if matches!(self, TProp::Empty) {
            *self = TProp::from(t, prop);
        } else {
            match (self, prop) {
                (TProp::Empty, _) => {}

                (TProp::Str(cell), Prop::Str(a)) => {
                    cell.set(t, a);
                }
                (TProp::I32(cell), Prop::I32(a)) => {
                    cell.set(t, a);
                }
                (TProp::I64(cell), Prop::I64(a)) => {
                    cell.set(t, a);
                }
                (TProp::U32(cell), Prop::U32(a)) => {
                    cell.set(t, a);
                }
                (TProp::U8(cell), Prop::U8(a)) => {
                    cell.set(t, a);
                }
                (TProp::U16(cell), Prop::U16(a)) => {
                    cell.set(t, a);
                }
                (TProp::U64(cell), Prop::U64(a)) => {
                    cell.set(t, a);
                }
                (TProp::F32(cell), Prop::F32(a)) => {
                    cell.set(t, a);
                }
                (TProp::F64(cell), Prop::F64(a)) => {
                    cell.set(t, a);
                }
                (TProp::Bool(cell), Prop::Bool(a)) => {
                    cell.set(t, a);
                }
                (TProp::DTime(cell), Prop::DTime(a)) => {
                    cell.set(t, a);
                }
                (TProp::NDTime(cell), Prop::NDTime(a)) => {
                    cell.set(t, a);
                }
                (TProp::Array(cell), Prop::Array(a)) => {
                    cell.set(t, a);
                }
                (TProp::List(cell), Prop::List(a)) => {
                    cell.set(t, a);
                }
                (TProp::Map(cell), Prop::Map(a)) => {
                    cell.set(t, a);
                }
                (TProp::Decimal(cell), Prop::Decimal(a)) => {
                    cell.set(t, a);
                }
                _ => return Err(GraphError::IncorrectPropertyType),
            };
        }
        Ok(())
    }

    pub(crate) fn iter_inner(&self) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        match self {
            TProp::Empty => Box::new(iter::empty()),
            TProp::Str(cell) => {
                Box::new(cell.iter().map(|(t, value)| (*t, Prop::Str(value.clone()))))
            }
            TProp::I32(cell) => Box::new(cell.iter().map(|(t, value)| (*t, Prop::I32(*value)))),
            TProp::I64(cell) => Box::new(cell.iter().map(|(t, value)| (*t, Prop::I64(*value)))),
            TProp::U8(cell) => Box::new(cell.iter().map(|(t, value)| (*t, Prop::U8(*value)))),
            TProp::U16(cell) => Box::new(cell.iter().map(|(t, value)| (*t, Prop::U16(*value)))),
            TProp::U32(cell) => Box::new(cell.iter().map(|(t, value)| (*t, Prop::U32(*value)))),
            TProp::U64(cell) => Box::new(cell.iter().map(|(t, value)| (*t, Prop::U64(*value)))),
            TProp::F32(cell) => Box::new(cell.iter().map(|(t, value)| (*t, Prop::F32(*value)))),
            TProp::F64(cell) => Box::new(cell.iter().map(|(t, value)| (*t, Prop::F64(*value)))),
            TProp::Bool(cell) => Box::new(cell.iter().map(|(t, value)| (*t, Prop::Bool(*value)))),
            TProp::DTime(cell) => Box::new(cell.iter().map(|(t, value)| (*t, Prop::DTime(*value)))),
            TProp::NDTime(cell) => {
                Box::new(cell.iter().map(|(t, value)| (*t, Prop::NDTime(*value))))
            }
            TProp::Array(cell) => Box::new(
                cell.iter()
                    .map(|(t, value)| (*t, Prop::Array(value.clone()))),
            ),
            TProp::List(cell) => Box::new(
                cell.iter()
                    .map(|(t, value)| (*t, Prop::List(value.clone()))),
            ),
            TProp::Map(cell) => {
                Box::new(cell.iter().map(|(t, value)| (*t, Prop::Map(value.clone()))))
            }
            TProp::Decimal(cell) => Box::new(
                cell.iter()
                    .map(|(t, value)| (*t, Prop::Decimal(value.clone()))),
            ),
        }
    }

    pub(crate) fn iter_t(&self) -> BoxedLIter<(i64, Prop)> {
        match self {
            TProp::Empty => Box::new(iter::empty()),
            TProp::Str(cell) => Box::new(
                cell.iter_t()
                    .map(|(t, value)| (t, Prop::Str(value.clone()))),
            ),
            TProp::I32(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::I32(*value)))),
            TProp::I64(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::I64(*value)))),
            TProp::U8(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::U8(*value)))),
            TProp::U16(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::U16(*value)))),
            TProp::U32(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::U32(*value)))),
            TProp::U64(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::U64(*value)))),
            TProp::F32(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::F32(*value)))),
            TProp::F64(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::F64(*value)))),
            TProp::Bool(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::Bool(*value)))),
            TProp::DTime(cell) => {
                Box::new(cell.iter_t().map(|(t, value)| (t, Prop::DTime(*value))))
            }
            TProp::NDTime(cell) => {
                Box::new(cell.iter_t().map(|(t, value)| (t, Prop::NDTime(*value))))
            }
            TProp::Array(cell) => Box::new(
                cell.iter_t()
                    .map(|(t, value)| (t, Prop::Array(value.clone()))),
            ),
            TProp::List(cell) => Box::new(
                cell.iter_t()
                    .map(|(t, value)| (t, Prop::List(value.clone()))),
            ),
            TProp::Map(cell) => Box::new(
                cell.iter_t()
                    .map(|(t, value)| (t, Prop::Map(value.clone()))),
            ),
            TProp::Decimal(cell) => Box::new(
                cell.iter_t()
                    .map(|(t, value)| (t, Prop::Decimal(value.clone()))),
            ),
        }
    }

    pub(crate) fn iter_window_inner(
        &self,
        r: Range<TimeIndexEntry>,
    ) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        match self {
            TProp::Empty => Box::new(iter::empty()),
            TProp::Str(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::Str(value.clone()))),
            ),
            TProp::I32(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::I32(*value))),
            ),
            TProp::I64(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::I64(*value))),
            ),
            TProp::U8(cell) => {
                Box::new(cell.iter_window(r).map(|(t, value)| (*t, Prop::U8(*value))))
            }
            TProp::U16(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::U16(*value))),
            ),
            TProp::U32(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::U32(*value))),
            ),
            TProp::U64(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::U64(*value))),
            ),
            TProp::F32(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::F32(*value))),
            ),
            TProp::F64(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::F64(*value))),
            ),
            TProp::Bool(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::Bool(*value))),
            ),
            TProp::DTime(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::DTime(*value))),
            ),
            TProp::NDTime(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::NDTime(*value))),
            ),
            TProp::Array(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::Array(value.clone()))),
            ),
            TProp::List(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::List(value.clone()))),
            ),
            TProp::Map(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::Map(value.clone()))),
            ),
            TProp::Decimal(cell) => Box::new(
                cell.iter_window(r)
                    .map(|(t, value)| (*t, Prop::Decimal(value.clone()))),
            ),
        }
    }
}

impl<'a> TPropOps<'a> for &'a TProp {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        match self {
            TProp::Empty => None,
            TProp::Str(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::Str(v.clone()))),
            TProp::I32(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::I32(*v))),
            TProp::I64(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::I64(*v))),
            TProp::U8(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::U8(*v))),
            TProp::U16(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::U16(*v))),
            TProp::U32(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::U32(*v))),
            TProp::U64(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::U64(*v))),
            TProp::F32(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::F32(*v))),
            TProp::F64(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::F64(*v))),
            TProp::Bool(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::Bool(*v))),
            TProp::DTime(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::DTime(*v))),
            TProp::NDTime(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::NDTime(*v))),
            TProp::Array(cell) => cell
                .last_before(t)
                .map(|(t, v)| (t, Prop::Array(v.clone()))),
            TProp::List(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::List(v.clone()))),
            TProp::Map(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::Map(v.clone()))),
            TProp::Decimal(cell) => cell
                .last_before(t)
                .map(|(t, v)| (t, Prop::Decimal(v.clone()))),
        }
    }

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        self.iter_inner()
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        self.iter_window_inner(r)
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        match self {
            TProp::Empty => None,
            TProp::Str(cell) => cell.at(ti).map(|v| Prop::Str(v.clone())),
            TProp::I32(cell) => cell.at(ti).map(|v| Prop::I32(*v)),
            TProp::I64(cell) => cell.at(ti).map(|v| Prop::I64(*v)),
            TProp::U32(cell) => cell.at(ti).map(|v| Prop::U32(*v)),
            TProp::U8(cell) => cell.at(ti).map(|v| Prop::U8(*v)),
            TProp::U16(cell) => cell.at(ti).map(|v| Prop::U16(*v)),
            TProp::U64(cell) => cell.at(ti).map(|v| Prop::U64(*v)),
            TProp::F32(cell) => cell.at(ti).map(|v| Prop::F32(*v)),
            TProp::F64(cell) => cell.at(ti).map(|v| Prop::F64(*v)),
            TProp::Bool(cell) => cell.at(ti).map(|v| Prop::Bool(*v)),
            TProp::DTime(cell) => cell.at(ti).map(|v| Prop::DTime(*v)),
            TProp::NDTime(cell) => cell.at(ti).map(|v| Prop::NDTime(*v)),
            TProp::Array(cell) => cell.at(ti).map(|v| Prop::Array(v.clone())),
            TProp::List(cell) => cell.at(ti).map(|v| Prop::List(v.clone())),
            TProp::Map(cell) => cell.at(ti).map(|v| Prop::Map(v.clone())),
            TProp::Decimal(cell) => cell.at(ti).map(|v| Prop::Decimal(v.clone())),
        }
    }
}

#[cfg(test)]
mod tprop_tests {
    use crate::core::storage::lazy_vec::LazyVec;

    use super::*;

    #[test]
    fn t_prop_cell() {
        let col = TPropColumn::Bool(LazyVec::from(0, true));
        assert_eq!(col.get(0), Some(Prop::Bool(true)));

        let t_prop = TPropCell::new(&TCell::TCell1(TimeIndexEntry(0, 0), Some(0)), Some(&col));

        let actual = t_prop.iter().collect::<Vec<_>>();

        assert_eq!(actual, vec![(TimeIndexEntry(0, 0), Prop::Bool(true))]);
    }

    #[test]
    fn set_new_value_for_tprop_initialized_as_empty() {
        let mut tprop = TProp::Empty;
        tprop.set(1.into(), Prop::I32(10)).unwrap();

        assert_eq!(tprop.iter_t().collect::<Vec<_>>(), vec![(1, Prop::I32(10))]);
    }

    #[test]
    fn every_new_update_to_the_same_prop_is_recorded_as_history() {
        let mut tprop = TProp::from(1.into(), "Pometry".into());
        tprop.set(2.into(), "Pometry Inc.".into()).unwrap();

        assert_eq!(
            tprop.iter_t().collect::<Vec<_>>(),
            vec![(1, "Pometry".into()), (2, "Pometry Inc.".into())]
        );
    }

    #[test]
    fn new_update_with_the_same_time_to_a_prop_is_ignored() {
        let mut tprop = TProp::from(1.into(), "Pometry".into());
        tprop.set(1.into(), "Pometry Inc.".into()).unwrap();

        assert_eq!(
            tprop.iter_t().collect::<Vec<_>>(),
            vec![(1, "Pometry Inc.".into())]
        );
    }

    #[test]
    fn updates_to_prop_can_be_iterated() {
        let tprop = TProp::default();

        assert_eq!(tprop.iter_t().collect::<Vec<_>>(), vec![]);

        let mut tprop = TProp::from(1.into(), "Pometry".into());
        tprop.set(2.into(), "Pometry Inc.".into()).unwrap();

        assert_eq!(
            tprop.iter_t().collect::<Vec<_>>(),
            vec![
                (1, Prop::Str("Pometry".into())),
                (2, Prop::Str("Pometry Inc.".into()))
            ]
        );

        let mut tprop = TProp::from(1.into(), Prop::I32(2022));
        tprop.set(2.into(), Prop::I32(2023)).unwrap();

        assert_eq!(
            tprop.iter_t().collect::<Vec<_>>(),
            vec![(1, Prop::I32(2022)), (2, Prop::I32(2023))]
        );

        let mut tprop = TProp::from(1.into(), Prop::I64(2022));
        tprop.set(2.into(), Prop::I64(2023)).unwrap();

        assert_eq!(
            tprop.iter_t().collect::<Vec<_>>(),
            vec![(1, Prop::I64(2022)), (2, Prop::I64(2023))]
        );

        let mut tprop = TProp::from(1.into(), Prop::F32(10.0));
        tprop.set(2.into(), Prop::F32(11.0)).unwrap();

        assert_eq!(
            tprop.iter_t().collect::<Vec<_>>(),
            vec![(1, Prop::F32(10.0)), (2, Prop::F32(11.0))]
        );

        let mut tprop = TProp::from(1.into(), Prop::F64(10.0));
        tprop.set(2.into(), Prop::F64(11.0)).unwrap();

        assert_eq!(
            tprop.iter_t().collect::<Vec<_>>(),
            vec![(1, Prop::F64(10.0)), (2, Prop::F64(11.0))]
        );

        let mut tprop = TProp::from(1.into(), Prop::U32(1));
        tprop.set(2.into(), Prop::U32(2)).unwrap();

        assert_eq!(
            tprop.iter_t().collect::<Vec<_>>(),
            vec![(1, Prop::U32(1)), (2, Prop::U32(2))]
        );

        let mut tprop = TProp::from(1.into(), Prop::U64(1));
        tprop.set(2.into(), Prop::U64(2)).unwrap();

        assert_eq!(
            tprop.iter_t().collect::<Vec<_>>(),
            vec![(1, Prop::U64(1)), (2, Prop::U64(2))]
        );

        let mut tprop = TProp::from(1.into(), Prop::U8(1));
        tprop.set(2.into(), Prop::U8(2)).unwrap();

        assert_eq!(
            tprop.iter_t().collect::<Vec<_>>(),
            vec![(1, Prop::U8(1)), (2, Prop::U8(2))]
        );

        let mut tprop = TProp::from(1.into(), Prop::U16(1));
        tprop.set(2.into(), Prop::U16(2)).unwrap();

        assert_eq!(
            tprop.iter_t().collect::<Vec<_>>(),
            vec![(1, Prop::U16(1)), (2, Prop::U16(2))]
        );

        let mut tprop = TProp::from(1.into(), Prop::Bool(true));
        tprop.set(2.into(), Prop::Bool(true)).unwrap();

        assert_eq!(
            tprop.iter_t().collect::<Vec<_>>(),
            vec![(1, Prop::Bool(true)), (2, Prop::Bool(true))]
        );
    }

    #[test]
    fn updates_to_prop_can_be_window_iterated() {
        let tprop = TProp::default();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![]
        );

        let mut tprop = TProp::from(3.into(), Prop::Str("Pometry".into()));
        tprop
            .set(1.into(), Prop::Str("Pometry Inc.".into()))
            .unwrap();
        tprop.set(2.into(), Prop::Str("Raphtory".into())).unwrap();

        assert_eq!(
            tprop.iter_window_t(2..3).collect::<Vec<_>>(),
            vec![(2, Prop::Str("Raphtory".into()))]
        );

        assert_eq!(tprop.iter_window_t(4..5).collect::<Vec<_>>(), vec![]);

        assert_eq!(
            // Results are ordered by time
            tprop.iter_window_t(1..i64::MAX).collect::<Vec<_>>(),
            vec![
                (1, Prop::Str("Pometry Inc.".into())),
                (2, Prop::Str("Raphtory".into())),
                (3, Prop::Str("Pometry".into()))
            ]
        );

        assert_eq!(
            tprop.iter_window_t(3..i64::MAX).collect::<Vec<_>>(),
            vec![(3, Prop::Str("Pometry".into()))]
        );

        assert_eq!(
            tprop.iter_window_t(2..i64::MAX).collect::<Vec<_>>(),
            vec![
                (2, Prop::Str("Raphtory".into())),
                (3, Prop::Str("Pometry".into()))
            ]
        );

        assert_eq!(tprop.iter_window_t(5..i64::MAX).collect::<Vec<_>>(), vec![]);

        assert_eq!(
            tprop.iter_window_t(i64::MIN..4).collect::<Vec<_>>(),
            // Results are ordered by time
            vec![
                (1, Prop::Str("Pometry Inc.".into())),
                (2, Prop::Str("Raphtory".into())),
                (3, Prop::Str("Pometry".into()))
            ]
        );

        assert_eq!(tprop.iter_window_t(i64::MIN..1).collect::<Vec<_>>(), vec![]);

        let mut tprop = TProp::from(1.into(), Prop::I32(2022));
        tprop.set(2.into(), Prop::I32(2023)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::I32(2022)), (2, Prop::I32(2023))]
        );

        let mut tprop = TProp::from(1.into(), Prop::I64(2022));
        tprop.set(2.into(), Prop::I64(2023)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::I64(2022)), (2, Prop::I64(2023))]
        );

        let mut tprop = TProp::from(1.into(), Prop::F32(10.0));
        tprop.set(2.into(), Prop::F32(11.0)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::F32(10.0)), (2, Prop::F32(11.0))]
        );

        let mut tprop = TProp::from(1.into(), Prop::F64(10.0));
        tprop.set(2.into(), Prop::F64(11.0)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::F64(10.0)), (2, Prop::F64(11.0))]
        );

        let mut tprop = TProp::from(1.into(), Prop::U32(1));
        tprop.set(2.into(), Prop::U32(2)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::U32(1)), (2, Prop::U32(2))]
        );

        let mut tprop = TProp::from(1.into(), Prop::U64(1));
        tprop.set(2.into(), Prop::U64(2)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::U64(1)), (2, Prop::U64(2))]
        );

        let mut tprop = TProp::from(1.into(), Prop::U8(1));
        tprop.set(2.into(), Prop::U8(2)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::U8(1)), (2, Prop::U8(2))]
        );

        let mut tprop = TProp::from(1.into(), Prop::U16(1));
        tprop.set(2.into(), Prop::U16(2)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::U16(1)), (2, Prop::U16(2))]
        );

        let mut tprop = TProp::from(1.into(), Prop::Bool(true));
        tprop.set(2.into(), Prop::Bool(true)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::Bool(true)), (2, Prop::Bool(true))]
        );
    }
}
