use crate::python::graph::properties::{
    DynStaticProperties, DynTemporalProperties, PyStaticProperties, PyTemporalProperties,
};
use crate::python::utils::{PyGenericIterator, PyNestedGenericIterator};
use crate::{
    core as db_c,
    db::api::view::BoxedIter,
    python::types::{
        repr::Repr,
        wrappers::prop::{PropHistory, PropValue},
    },
};
use num::cast::AsPrimitive;
use pyo3::prelude::*;
use std::{i64, iter::Sum};

pub(crate) trait MeanExt<V>: Iterator<Item = V>
where
    V: AsPrimitive<f64> + Sum<V>,
{
    fn mean(self) -> f64
    where
        Self: Sized,
    {
        let mut count: usize = 0;
        let sum: V = self.inspect(|_| count += 1).sum();

        if count > 0 {
            sum.as_() / (count as f64)
        } else {
            0.0
        }
    }
}

impl<I: ?Sized + Iterator<Item = V>, V: AsPrimitive<f64> + Sum<V>> MeanExt<V> for I {}

py_iterator!(Float64Iter, f64);
py_float_iterable!(Float64Iterable, f64, Float64Iter);

py_iterator!(U64Iter, u64);
py_numeric_iterable!(U64Iterable, u64, U64Iter);
py_iterator!(NestedU64Iter, BoxedIter<u64>, U64Iter);
py_nested_numeric_iterable!(
    NestedU64Iterable,
    u64,
    NestedU64Iter,
    U64Iterable,
    OptionU64Iterable
);

py_iterable!(OptionU64U64Iterable, Option<(u64, u64)>, PyGenericIterator);
py_ordered_iterable!(U64U64Iterable, (u64, u64), PyGenericIterator);
py_nested_ordered_iterable!(
    NestedU64U64Iterable,
    (u64, u64),
    PyNestedGenericIterator,
    OptionU64U64Iterable
);

py_iterator!(OptionU64Iter, Option<u64>);
py_iterable!(OptionU64Iterable, Option<u64>, Option<u64>, OptionU64Iter);
_py_ord_max_min_methods!(OptionU64Iterable, Option<u64>);

py_iterator!(I64Iter, i64);
py_numeric_iterable!(I64Iterable, i64, I64Iter);
py_iterator!(NestedI64Iter, BoxedIter<i64>, I64Iter);
py_nested_numeric_iterable!(
    NestedI64Iterable,
    i64,
    NestedI64Iter,
    I64Iterable,
    OptionI64Iterable
);

py_iterator!(OptionI64Iter, Option<i64>);
py_iterable!(OptionI64Iterable, Option<i64>, OptionI64Iter);
_py_ord_max_min_methods!(OptionI64Iterable, Option<i64>);
py_iterator!(OptionOptionI64Iter, Option<Option<i64>>);
py_iterable!(
    OptionOptionI64Iterable,
    Option<Option<i64>>,
    OptionOptionI64Iter
);
_py_ord_max_min_methods!(OptionOptionI64Iterable, Option<Option<i64>>);

py_iterator!(NestedOptionI64Iter, BoxedIter<Option<i64>>, OptionI64Iter);
py_nested_ordered_iterable!(
    NestedOptionI64Iterable,
    Option<i64>,
    NestedOptionI64Iter,
    OptionOptionI64Iterable
);

py_iterator!(UsizeIter, usize);
py_numeric_iterable!(UsizeIterable, usize, UsizeIter);
py_iterator!(OptionUsizeIter, Option<usize>);
py_ordered_iterable!(OptionUsizeIterable, Option<usize>, OptionUsizeIter);
py_iterator!(NestedUsizeIter, BoxedIter<usize>, UsizeIter);
py_nested_numeric_iterable!(
    NestedUsizeIterable,
    usize,
    NestedUsizeIter,
    UsizeIterable,
    OptionUsizeIterable
);

py_iterator!(BoolIter, bool);
py_iterable!(BoolIterable, bool, BoolIter);
py_iterator!(NestedBoolIter, BoxedIter<bool>, BoolIter);
py_nested_iterable!(NestedBoolIterable, bool, NestedBoolIter);

py_iterator!(StringIter, String);
py_iterable!(StringIterable, String, StringIter);
py_iterator!(NestedStringIter, BoxedIter<String>, StringIter);
py_nested_iterable!(NestedStringIterable, String, NestedStringIter);

py_iterator!(StringVecIter, Vec<String>);
py_iterable!(StringVecIterable, Vec<String>, StringVecIter);
py_iterator!(NestedStringVecIter, BoxedIter<Vec<String>>, StringVecIter);
py_nested_iterable!(NestedStringVecIterable, Vec<String>, NestedStringVecIter);

py_iterator!(OptionPropIter, Option<db_c::Prop>, PropValue);
py_iterable!(
    OptionPropIterable,
    Option<db_c::Prop>,
    PropValue,
    OptionPropIter
);
py_iterator!(
    NestedOptionPropIter,
    BoxedIter<Option<db_c::Prop>>,
    OptionPropIter
);
py_nested_iterable!(
    NestedOptionPropIterable,
    Option<db_c::Prop>,
    PropValue,
    NestedOptionPropIter
);

py_iterator!(PropHistoryIter, Vec<(i64, db_c::Prop)>, PropHistory);
py_iterable!(
    PropHistoryIterable,
    Vec<(i64, db_c::Prop)>,
    PropHistory,
    PropHistoryIter
);
py_iterator!(
    NestedPropHistoryIter,
    BoxedIter<Vec<(i64, db_c::Prop)>>,
    PropHistoryIter
);
py_nested_iterable!(
    NestedPropHistoryIterable,
    Vec<(i64, db_c::Prop)>,
    PropHistory,
    NestedPropHistoryIter
);

py_iterator!(StaticPropsIter, DynStaticProperties, PyStaticProperties);
py_iterable!(
    StaticPropsIterable,
    DynStaticProperties,
    PyStaticProperties,
    StaticPropsIter
);
py_iterator!(
    NestedStaticPropsIter,
    BoxedIter<DynStaticProperties>,
    StaticPropsIter
);
py_nested_iterable!(
    NestedStaticPropsIterable,
    DynStaticProperties,
    PyStaticProperties,
    NestedStaticPropsIter
);
