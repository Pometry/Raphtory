use crate::python::types::{
    result_iterable::{
        NestedResultOptionUtcDateTimeIterable, NestedResultUtcDateTimeIterable,
        ResultOptionUtcDateTimeIterable, ResultUtcDateTimeIterable,
    },
    wrappers::iterables::{
        EventTimeIterable, I64Iterable, NestedEventTimeIterable, NestedI64Iterable,
        NestedOptionEventTimeIterable, NestedOptionI64Iterable, NestedOptionUsizeIterable,
        NestedUsizeIterable, OptionEventTimeIterable, OptionI64Iterable, OptionUsizeIterable,
        UsizeIterable,
    },
};
use pyo3::pymethods;
use raphtory_api::core::storage::timeindex::AsTime;

// Custom EventTime operations on iterables
#[pymethods]
impl EventTimeIterable {
    /// Change this Iterable of EventTime into an Iterable of corresponding Unix timestamps in milliseconds.
    ///
    /// Returns:
    ///     I64Iterable: Iterable of millisecond timestamps since the Unix epoch for each EventTime.
    #[getter]
    fn t(&self) -> I64Iterable {
        let builder = self.builder.clone();
        (move || builder().map(|t| t.t())).into()
    }

    /// Change this Iterable of EventTime into an Iterable of corresponding UTC DateTimes.
    ///
    /// Returns:
    ///     ResultUtcDateTimeIterable: Iterable of UTC datetimes for each EventTime.
    ///
    /// Raises:
    ///     TimeError: Returns TimeError on timestamp conversion errors (e.g. out-of-range timestamp).
    #[getter]
    fn dt(&self) -> ResultUtcDateTimeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|t| t.dt())).into()
    }

    /// Change this Iterable of EventTime into an Iterable of their associated event ids.
    ///
    /// Returns:
    ///     UsizeIterable: Iterable of event ids associated to each EventTime.
    #[getter]
    fn event_id(&self) -> UsizeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|t| t.i())).into()
    }
}

// Custom EventTime operations on nested iterables
#[pymethods]
impl NestedEventTimeIterable {
    /// Change this nested Iterable of EventTime into a nested Iterable of corresponding Unix timestamps in milliseconds.
    ///
    /// Returns:
    ///     NestedI64Iterable: Nested iterable of millisecond timestamps since the Unix epoch for each EventTime.
    #[getter]
    fn t(&self) -> NestedI64Iterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_iter| t_iter.map(|t| t.t()))).into()
    }

    /// Change this nested Iterable of EventTime into a nested Iterable of corresponding UTC DateTimes.
    ///
    /// Returns:
    ///     NestedResultUtcDateTimeIterable: Nested iterable of UTC datetimes for each EventTime.
    ///
    /// Raises:
    ///     TimeError: Returns TimeError on timestamp conversion errors (e.g. out-of-range timestamp).
    #[getter]
    fn dt(&self) -> NestedResultUtcDateTimeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_iter| t_iter.map(|t| t.dt()))).into()
    }

    /// Change this nested Iterable of EventTime into a nested Iterable of their associated event ids.
    ///
    /// Returns:
    ///     NestedUsizeIterable: Nested iterable of event ids associated to each EventTime.
    #[getter]
    fn event_id(&self) -> NestedUsizeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_iter| t_iter.map(|t| t.i()))).into()
    }
}

// Custom EventTime operations on iterables of Option<EventTime>
#[pymethods]
impl OptionEventTimeIterable {
    /// Change this Iterable of Optional[EventTime] into an Iterable of corresponding Unix timestamps in milliseconds.
    ///
    /// Returns:
    ///     OptionI64Iterable: Iterable of millisecond timestamps since the Unix epoch for each EventTime, if available.
    #[getter]
    fn t(&self) -> OptionI64Iterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_opt| t_opt.map(|t| t.t()))).into()
    }

    /// Change this Iterable of Optional[EventTime] into an Iterable of corresponding UTC DateTimes.
    ///
    /// Returns:
    ///     ResultOptionUtcDateTimeIterable: Iterable of UTC datetimes for each EventTime, if available.
    ///
    /// Raises:
    ///     TimeError: Returns TimeError on timestamp conversion errors (e.g. out-of-range timestamp).
    #[getter]
    fn dt(&self) -> ResultOptionUtcDateTimeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_opt| t_opt.map(|t| t.dt()).transpose())).into()
    }

    /// Change this Iterable of Optional[EventTime] into an Iterable of their associated event ids.
    ///
    /// Returns:
    ///     OptionUsizeIterable: Iterable of event ids associated to each EventTime, if available.
    #[getter]
    fn event_id(&self) -> OptionUsizeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_opt| t_opt.map(|t| t.i()))).into()
    }
}

// Custom EventTime operations on nested iterables of Option<EventTime>
#[pymethods]
impl NestedOptionEventTimeIterable {
    /// Change this nested Iterable of Optional[EventTime] into a nested Iterable of corresponding Unix timestamps in milliseconds.
    ///
    /// Returns:
    ///     NestedOptionI64Iterable: Nested iterable of millisecond timestamps since the Unix epoch for each EventTime, if available.
    #[getter]
    fn t(&self) -> NestedOptionI64Iterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_iter| t_iter.map(|t_opt| t_opt.map(|t| t.t())))).into()
    }

    /// Change this nested Iterable of Optional[EventTime] into a nested Iterable of corresponding UTC DateTimes.
    ///
    /// Returns:
    ///     NestedResultOptionUtcDateTimeIterable: Nested iterable of UTC datetimes for each EventTime, if available.
    ///
    /// Raises:
    ///     TimeError: Returns TimeError on timestamp conversion errors (e.g. out-of-range timestamp).
    #[getter]
    fn dt(&self) -> NestedResultOptionUtcDateTimeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_iter| t_iter.map(|t_opt| t_opt.map(|t| t.dt()).transpose())))
            .into()
    }

    /// Change this nested Iterable of Optional[EventTime] into a nested Iterable of their associated event ids.
    ///
    /// Returns:
    ///     NestedOptionUsizeIterable: Nested iterable of event ids associated to each EventTime, if available.
    #[getter]
    fn event_id(&self) -> NestedOptionUsizeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|t_iter| t_iter.map(|t_opt| t_opt.map(|t| t.i())))).into()
    }
}
