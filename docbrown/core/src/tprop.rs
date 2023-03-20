use crate::tcell::TCell;
use crate::Prop;
use serde::{Deserialize, Serialize};
use std::ops::Range;

// TODO TProp struct could be replaced with Option<TCell<Prop>>, with the only issue (or advantage) that then the type can change?

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
    Bool(TCell<bool>)
}

impl TProp {
    pub(crate) fn from(t: i64, prop: &Prop) -> Self {
        match prop {
            Prop::Str(value) => TProp::Str(TCell::new(t, value.to_string())),
            Prop::I32(value) => TProp::I32(TCell::new(t, *value)),
            Prop::I64(value) => TProp::I64(TCell::new(t, *value)),
            Prop::U32(value) => TProp::U32(TCell::new(t, *value)),
            Prop::U64(value) => TProp::U64(TCell::new(t, *value)),
            Prop::F32(value) => TProp::F32(TCell::new(t, *value)),
            Prop::F64(value) => TProp::F64(TCell::new(t, *value)),
            Prop::Bool(value) => TProp::Bool(TCell::new(t, *value))
        }
    }

    pub(crate) fn set(&mut self, t: i64, prop: &Prop) {
        match self {
            TProp::Empty => {
                *self = TProp::from(t, prop);
            }
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
            TProp::Bool(cell) => {
                if let Prop::Bool(a) = prop {
                    cell.set(t, *a);
                }
            }
        }
    }

    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = (&i64, Prop)> + '_> {
        match self {
            TProp::Empty => Box::new(std::iter::empty()),
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
            TProp::Bool(cell) => Box::new(cell.iter_t().map(|(t, value)| (t, Prop::Bool(*value))))
        }
    }

    pub(crate) fn iter_window(&self, r: Range<i64>) -> Box<dyn Iterator<Item = (&i64, Prop)> + '_> {
        match self {
            TProp::Empty => Box::new(std::iter::empty()),
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
            TProp::Bool(cell) => Box::new(
                cell.iter_window_t(r)
                    .map(|(t, value)| (t, Prop::Bool(*value))),
            ),
        }
    }
}

#[cfg(test)]
mod tprop_tests {
    use super::*;

    #[test]
    fn set_new_value_for_tprop_initialized_as_empty() {
        let mut tprop = TProp::Empty;
        tprop.set(1, &Prop::I32(10));

        assert_eq!(tprop.iter().collect::<Vec<_>>(), vec![(&1, Prop::I32(10))]);
    }

    #[test]
    fn every_new_update_to_the_same_prop_is_recorded_as_history() {
        let mut tprop = TProp::from(1, &Prop::Str("Pometry".into()));
        tprop.set(2, &Prop::Str("Pometry Inc.".into()));

        assert_eq!(
            tprop.iter().collect::<Vec<_>>(),
            vec![
                (&1, Prop::Str("Pometry".into())),
                (&2, Prop::Str("Pometry Inc.".into()))
            ]
        );
    }

    #[test]
    fn new_update_with_the_same_time_to_a_prop_is_ignored() {
        let mut tprop = TProp::from(1, &Prop::Str("Pometry".into()));
        tprop.set(1, &Prop::Str("Pometry Inc.".into()));

        assert_eq!(
            tprop.iter().collect::<Vec<_>>(),
            vec![(&1, Prop::Str("Pometry".into()))]
        );
    }

    #[test]
    fn updates_to_prop_can_be_iterated() {
        let tprop = TProp::default();

        assert_eq!(tprop.iter().collect::<Vec<_>>(), vec![]);

        let mut tprop = TProp::from(1, &Prop::Str("Pometry".into()));
        tprop.set(2, &Prop::Str("Pometry Inc.".into()));

        assert_eq!(
            tprop.iter().collect::<Vec<_>>(),
            vec![
                (&1, Prop::Str("Pometry".into())),
                (&2, Prop::Str("Pometry Inc.".into()))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::I32(2022));
        tprop.set(2, &Prop::I32(2023));

        assert_eq!(
            tprop.iter().collect::<Vec<_>>(),
            vec![
                (&1, Prop::I32(2022)),
                (&2, Prop::I32(2023))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::I64(2022));
        tprop.set(2, &Prop::I64(2023));

        assert_eq!(
            tprop.iter().collect::<Vec<_>>(),
            vec![
                (&1, Prop::I64(2022)),
                (&2, Prop::I64(2023))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::F32(10.0));
        tprop.set(2, &Prop::F32(11.0));

        assert_eq!(
            tprop.iter().collect::<Vec<_>>(),
            vec![
                (&1, Prop::F32(10.0)),
                (&2, Prop::F32(11.0))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::F64(10.0));
        tprop.set(2, &Prop::F64(11.0));

        assert_eq!(
            tprop.iter().collect::<Vec<_>>(),
            vec![
                (&1, Prop::F64(10.0)),
                (&2, Prop::F64(11.0))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::U32(1));
        tprop.set(2, &Prop::U32(2));

        assert_eq!(
            tprop.iter().collect::<Vec<_>>(),
            vec![
                (&1, Prop::U32(1)),
                (&2, Prop::U32(2))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::U64(1));
        tprop.set(2, &Prop::U64(2));

        assert_eq!(
            tprop.iter().collect::<Vec<_>>(),
            vec![
                (&1, Prop::U64(1)),
                (&2, Prop::U64(2))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::Bool(true));
        tprop.set(2, &Prop::Bool(true));

        assert_eq!(
            tprop.iter().collect::<Vec<_>>(),
            vec![
                (&1, Prop::Bool(true)),
                (&2, Prop::Bool(true))
            ]
        );
    }

    #[test]
    fn updates_to_prop_can_be_window_iterated() {
        let tprop = TProp::default();

        assert_eq!(
            tprop.iter_window(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![]
        );

        let mut tprop = TProp::from(3, &Prop::Str("Pometry".into()));
        tprop.set(1, &Prop::Str("Pometry Inc.".into()));
        tprop.set(2, &Prop::Str("Raphtory".into()));

        assert_eq!(
            tprop.iter_window(2..3).collect::<Vec<_>>(),
            vec![(&2, Prop::Str("Raphtory".into()))]
        );

        assert_eq!(tprop.iter_window(4..5).collect::<Vec<_>>(), vec![]);

        assert_eq!(
            // Results are ordered by time
            tprop.iter_window(1..i64::MAX).collect::<Vec<_>>(),
            vec![
                (&1, Prop::Str("Pometry Inc.".into())),
                (&2, Prop::Str("Raphtory".into())),
                (&3, Prop::Str("Pometry".into()))
            ]
        );

        assert_eq!(
            tprop.iter_window(3..i64::MAX).collect::<Vec<_>>(),
            vec![(&3, Prop::Str("Pometry".into()))]
        );

        assert_eq!(
            tprop.iter_window(2..i64::MAX).collect::<Vec<_>>(),
            vec![
                (&2, Prop::Str("Raphtory".into())),
                (&3, Prop::Str("Pometry".into()))
            ]
        );

        assert_eq!(tprop.iter_window(5..i64::MAX).collect::<Vec<_>>(), vec![]);

        assert_eq!(
            tprop.iter_window(i64::MIN..4).collect::<Vec<_>>(),
            // Results are ordered by time
            vec![
                (&1, Prop::Str("Pometry Inc.".into())),
                (&2, Prop::Str("Raphtory".into())),
                (&3, Prop::Str("Pometry".into()))
            ]
        );

        assert_eq!(tprop.iter_window(i64::MIN..1).collect::<Vec<_>>(), vec![]);

        let mut tprop = TProp::from(1, &Prop::I32(2022));
        tprop.set(2, &Prop::I32(2023));

        assert_eq!(
            tprop.iter_window(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![
                (&1, Prop::I32(2022)),
                (&2, Prop::I32(2023))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::I64(2022));
        tprop.set(2, &Prop::I64(2023));

        assert_eq!(
            tprop.iter_window(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![
                (&1, Prop::I64(2022)),
                (&2, Prop::I64(2023))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::F32(10.0));
        tprop.set(2, &Prop::F32(11.0));

        assert_eq!(
            tprop.iter_window(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![
                (&1, Prop::F32(10.0)),
                (&2, Prop::F32(11.0))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::F64(10.0));
        tprop.set(2, &Prop::F64(11.0));

        assert_eq!(
            tprop.iter_window(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![
                (&1, Prop::F64(10.0)),
                (&2, Prop::F64(11.0))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::U32(1));
        tprop.set(2, &Prop::U32(2));

        assert_eq!(
            tprop.iter_window(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![
                (&1, Prop::U32(1)),
                (&2, Prop::U32(2))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::U64(1));
        tprop.set(2, &Prop::U64(2));

        assert_eq!(
            tprop.iter_window(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![
                (&1, Prop::U64(1)),
                (&2, Prop::U64(2))
            ]
        );

        let mut tprop = TProp::from(1, &Prop::Bool(true));
        tprop.set(2, &Prop::Bool(true));

        assert_eq!(
            tprop.iter_window(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![
                (&1, Prop::Bool(true)),
                (&2, Prop::Bool(true))
            ]
        );
    }
}
