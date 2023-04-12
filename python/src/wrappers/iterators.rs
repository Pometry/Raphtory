use crate::wrappers::prop::{PropHistories, PropHistory, PropValue, Props};
use docbrown::core as db_c;
use docbrown::db::view_api::BoxedIter;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::i64;

macro_rules! py_iterator {
    ($name:ident, $item:ty) => {
        #[pyclass]
        pub struct $name {
            iter: Box<dyn Iterator<Item = $item> + Send>,
        }

        #[pymethods]
        impl $name {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }
            fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<$item> {
                slf.iter.next()
            }
        }

        impl From<Box<dyn Iterator<Item = $item> + Send>> for $name {
            fn from(value: Box<dyn Iterator<Item = $item> + Send>) -> Self {
                Self { iter: value }
            }
        }

        impl IntoIterator for $name {
            type Item = $item;
            type IntoIter = Box<dyn Iterator<Item = $item> + Send>;

            fn into_iter(self) -> Self::IntoIter {
                self.iter
            }
        }
    };

    ($name:ident, $item:ty, $pyitem:ty) => {
        #[pyclass]
        pub struct $name {
            iter: Box<dyn Iterator<Item = $pyitem> + Send>,
        }

        #[pymethods]
        impl $name {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }
            fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<$pyitem> {
                slf.iter.next()
            }
        }

        impl From<Box<dyn Iterator<Item = $item> + Send>> for $name {
            fn from(value: Box<dyn Iterator<Item = $item> + Send>) -> Self {
                let iter = Box::new(value.map(|v| v.into()));
                Self { iter }
            }
        }

        impl From<Box<dyn Iterator<Item = $pyitem> + Send>> for $name {
            fn from(value: Box<dyn Iterator<Item = $pyitem> + Send>) -> Self {
                Self { iter: value }
            }
        }

        impl IntoIterator for $name {
            type Item = $pyitem;
            type IntoIter = Box<dyn Iterator<Item = $pyitem> + Send>;

            fn into_iter(self) -> Self::IntoIter {
                self.iter
            }
        }
    };
}

py_iterator!(U64Iter, u64);
py_iterator!(NestedU64Iter, BoxedIter<u64>, U64Iter);

py_iterator!(I64Iter, i64);
py_iterator!(NestedI64Iter, BoxedIter<i64>, I64Iter);

py_iterator!(OptionI64Iter, Option<i64>);
py_iterator!(NestedOptionI64Iter, BoxedIter<Option<i64>>, OptionI64Iter);

py_iterator!(UsizeIter, usize);
py_iterator!(NestedUsizeIter, BoxedIter<usize>, UsizeIter);

py_iterator!(BoolIter, bool);
py_iterator!(NestedBoolIter, BoxedIter<bool>, BoolIter);

py_iterator!(StringIter, String);
py_iterator!(NestedStringIter, BoxedIter<String>, StringIter);

py_iterator!(StringVecIter, Vec<String>);
py_iterator!(NestedStringVecIter, BoxedIter<Vec<String>>, StringVecIter);

py_iterator!(OptionPropIter, Option<db_c::Prop>, PropValue);
py_iterator!(
    NestedOptionPropIter,
    BoxedIter<Option<db_c::Prop>>,
    OptionPropIter
);

py_iterator!(PropHistoryIter, Vec<(i64, db_c::Prop)>, PropHistory);
py_iterator!(
    NestedPropHistoryIter,
    BoxedIter<Vec<(i64, db_c::Prop)>>,
    PropHistoryIter
);

py_iterator!(PropsIter, HashMap<String, db_c::Prop>, Props);
py_iterator!(
    NestedPropsIter,
    BoxedIter<HashMap<String, db_c::Prop>>,
    PropsIter
);

py_iterator!(PropHistoriesIter, HashMap<String, Vec<(i64, db_c::Prop)>>, PropHistories);
py_iterator!(
    NestedPropHistoriesIter,
    BoxedIter<HashMap<String, Vec<(i64, db_c::Prop)>>>,
    PropHistoriesIter
);
