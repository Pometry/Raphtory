use crate::{
    db::api::view::{
        history::{History, HistoryDateTime, HistoryTimestamp, InternalHistoryOps},
        BoxedIter,
    },
    prelude::Prop,
    python::types::{
        repr::Repr,
        result_option_iterable::{
            NestedResultOptionUtcDateTimeIterable, ResultOptionUtcDateTimeIterable,
        },
    },
};
use chrono::{DateTime, Utc};
use num::cast::AsPrimitive;
use pyo3::prelude::*;
use raphtory_api::{
    core::{
        entities::GID,
        storage::{
            arc_str::ArcStr,
            timeindex::{AsTime, TimeIndexEntry},
        },
    },
    inherit::Base,
    iter::IntoDynBoxed,
};
use std::{iter::Sum, ops::Deref, sync::Arc};

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

py_float_iterable!(Float64Iterable, f64);
py_iterable_comp!(Float64Iterable, f64, Float64IterableCmp);

py_ordered_iterable!(GIDIterable, GID);
py_iterable_comp!(GIDIterable, GID, GIDIterableCmp);

py_iterable!(OptionGIDIterable, Option<GID>);
py_iterable_comp!(OptionGIDIterable, Option<GID>, OptionGIDIterableCmp);
py_nested_ordered_iterable!(NestedGIDIterable, GID, OptionGIDIterable);
py_iterable_comp!(NestedGIDIterable, GIDIterableCmp, NestedGIDIterableCmp);

py_numeric_iterable!(U64Iterable, u64);
py_iterable_comp!(U64Iterable, u64, U64IterableCmp);
py_nested_numeric_iterable!(NestedU64Iterable, u64, U64Iterable, OptionU64Iterable);
py_iterable_comp!(NestedU64Iterable, U64IterableCmp, NestedU64IterableCmp);

py_iterable!(OptionGIDGIDIterable, Option<(GID, GID)>);
py_iterable_comp!(
    OptionGIDGIDIterable,
    Option<(GID, GID)>,
    OptionGIDGIDIterableCmp
);
py_ordered_iterable!(GIDGIDIterable, (GID, GID));
py_iterable_comp!(GIDGIDIterable, (GID, GID), GIDGIDIterableCmp);
py_nested_ordered_iterable!(NestedGIDGIDIterable, (GID, GID), OptionGIDGIDIterable);
py_iterable_comp!(
    NestedGIDGIDIterable,
    GIDGIDIterableCmp,
    NestedGIDGIDIterableCmp
);

py_iterable!(OptionU64Iterable, Option<u64>, Option<u64>);
_py_ord_max_min_methods!(OptionU64Iterable, Option<u64>);
py_iterable_comp!(OptionU64Iterable, Option<u64>, OptionU64IterableCmp);

py_iterable!(PropIterable, Prop, Prop);
py_iterable_comp!(PropIterable, Prop, PropIterableCmp);

py_numeric_iterable!(I64Iterable, i64);
py_iterable_comp!(I64Iterable, i64, I64IterableCmp);
py_nested_numeric_iterable!(NestedI64Iterable, i64, I64Iterable, OptionI64Iterable);
py_iterable_comp!(NestedI64Iterable, I64IterableCmp, NestedI64IterableCmp);

py_iterable!(OptionI64Iterable, Option<i64>);
_py_ord_max_min_methods!(OptionI64Iterable, Option<i64>);
py_iterable_comp!(OptionI64Iterable, Option<i64>, OptionI64IterableCmp);
py_iterable!(OptionOptionI64Iterable, Option<Option<i64>>);
_py_ord_max_min_methods!(OptionOptionI64Iterable, Option<Option<i64>>);
py_iterable_comp!(
    OptionOptionI64Iterable,
    Option<Option<i64>>,
    OptionOptionI64IterableCmp
);

py_nested_ordered_iterable!(
    NestedOptionI64Iterable,
    Option<i64>,
    OptionOptionI64Iterable
);
py_iterable_comp!(
    NestedOptionI64Iterable,
    OptionI64IterableCmp,
    NestedOptionI64IterableCmp
);

py_ordered_iterable!(OptionTimeIndexEntryIterable, Option<TimeIndexEntry>);
py_iterable_comp!(
    OptionTimeIndexEntryIterable,
    Option<TimeIndexEntry>,
    OptionTimeIndexEntryIterableCmp
);
// Implement custom TimeIndexEntry operations on iterables as well
#[pymethods]
impl OptionTimeIndexEntryIterable {
    #[getter]
    fn t(&self) -> OptionI64Iterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_opt| t_opt.map(|t| t.t()))).into()
    }

    #[getter]
    fn dt(&self) -> ResultOptionUtcDateTimeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_opt| t_opt.map(|t| t.dt()).transpose())).into()
    }

    #[getter]
    fn secondary_index(&self) -> OptionUsizeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_opt| t_opt.map(|t| t.i()))).into()
    }
}
py_ordered_iterable!(
    OptionOptionTimeIndexEntryIterable,
    Option<Option<TimeIndexEntry>>
);
py_iterable_comp!(
    OptionOptionTimeIndexEntryIterable,
    Option<Option<TimeIndexEntry>>,
    OptionOptionTimeIndexEntryIterableCmp
);
py_nested_ordered_iterable!(
    NestedOptionTimeIndexEntryIterable,
    Option<TimeIndexEntry>,
    OptionOptionTimeIndexEntryIterable
);
py_iterable_comp!(
    NestedOptionTimeIndexEntryIterable,
    OptionTimeIndexEntryIterableCmp,
    NestedOptionTimeIndexEntryIterableCmp
);
// Implement custom TimeIndexEntry operations on nested iterables as well
#[pymethods]
impl NestedOptionTimeIndexEntryIterable {
    #[getter]
    fn t(&self) -> NestedOptionI64Iterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_iter| t_iter.map(|t_opt| t_opt.map(|t| t.t())))).into()
    }

    #[getter]
    fn dt(&self) -> NestedResultOptionUtcDateTimeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_iter| t_iter.map(|t_opt| t_opt.map(|t| t.dt()).transpose())))
            .into()
    }

    #[getter]
    fn secondary_index(&self) -> NestedOptionUsizeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_iter| t_iter.map(|t_opt| t_opt.map(|t| t.i())))).into()
    }
}

py_numeric_iterable!(UsizeIterable, usize);
py_iterable_comp!(UsizeIterable, usize, UsizeIterableCmp);

py_ordered_iterable!(OptionUsizeIterable, Option<usize>);
py_iterable_comp!(OptionUsizeIterable, Option<usize>, OptionUsizeIterableCmp);
py_nested_numeric_iterable!(
    NestedUsizeIterable,
    usize,
    UsizeIterable,
    OptionUsizeIterable
);
py_iterable_comp!(
    NestedUsizeIterable,
    UsizeIterableCmp,
    NestedUsizeIterableCmp
);
py_iterable!(OptionOptionUsizeIterable, Option<Option<usize>>);
_py_ord_max_min_methods!(OptionOptionUsizeIterable, Option<Option<usize>>);
py_iterable_comp!(
    OptionOptionUsizeIterable,
    Option<Option<usize>>,
    OptionOptionUsizeIterableCmp
);
py_nested_ordered_iterable!(
    NestedOptionUsizeIterable,
    Option<usize>,
    OptionOptionUsizeIterable
);
py_iterable_comp!(
    NestedOptionUsizeIterable,
    OptionUsizeIterableCmp,
    NestedOptionUsizeIterableCmp
);

py_iterable!(BoolIterable, bool);
py_iterable_comp!(BoolIterable, bool, BoolIterableCmp);
py_nested_iterable!(NestedBoolIterable, bool);
py_iterable_comp!(NestedBoolIterable, BoolIterableCmp, NestedBoolIterableCmp);

py_iterable!(StringIterable, String);
py_iterable_comp!(StringIterable, String, StringIterableCmp);
py_iterable!(OptionArcStringIterable, Option<ArcStr>);
py_iterable!(ArcStringIterable, ArcStr);
py_iterable_comp!(
    OptionArcStringIterable,
    Option<ArcStr>,
    OptionArcStringIterableCmp
);
py_nested_iterable!(NestedOptionArcStringIterable, Option<ArcStr>);
py_nested_iterable!(NestedArcStringIterable, ArcStr);
py_iterable_comp!(
    NestedOptionArcStringIterable,
    OptionArcStringIterableCmp,
    NestedOptionArcStringIterableCmp
);
py_nested_iterable!(NestedStringIterable, String);
py_iterable_comp!(
    NestedStringIterable,
    StringIterableCmp,
    NestedStringIterableCmp
);

py_iterable!(ArcStringVecIterable, Vec<ArcStr>);
py_iterable_comp!(ArcStringVecIterable, Vec<ArcStr>, ArcStringVecIterableCmp);
py_nested_iterable!(NestedArcStringVecIterable, Vec<ArcStr>);
py_iterable_comp!(
    NestedArcStringVecIterable,
    ArcStringVecIterableCmp,
    NestedArcStringVecIterableCmp
);

py_iterable!(I64VecIterable, Vec<i64>);
py_iterable_comp!(I64VecIterable, Vec<i64>, I64VecIterableCmp);
py_nested_iterable!(NestedI64VecIterable, Vec<i64>);
py_iterable_comp!(
    NestedI64VecIterable,
    I64VecIterableCmp,
    NestedI64VecIterableCmp
);

py_iterable!(OptionUtcDateTimeIterable, Option<DateTime<Utc>>);
py_iterable_comp!(
    OptionUtcDateTimeIterable,
    Option<DateTime<Utc>>,
    OptionUtcDateTimeIterableCmp
);
py_nested_iterable!(NestedUtcDateTimeIterable, Option<DateTime<Utc>>);
py_iterable_comp!(
    NestedUtcDateTimeIterable,
    OptionUtcDateTimeIterableCmp,
    NestedUtcDateTimeIterableCmp
);

py_iterable!(OptionVecUtcDateTimeIterable, Option<Vec<DateTime<Utc>>>);
py_iterable_comp!(
    OptionVecUtcDateTimeIterable,
    Option<Vec<DateTime<Utc>>>,
    OptionVecUtcDateTimeIterableCmp
);
py_nested_iterable!(NestedVecUtcDateTimeIterable, Option<Vec<DateTime<Utc>>>);
py_iterable_comp!(
    NestedVecUtcDateTimeIterable,
    OptionVecUtcDateTimeIterableCmp,
    NestedVecUtcDateTimeIterableCmp
);
