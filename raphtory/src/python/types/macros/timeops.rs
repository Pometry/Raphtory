/// Macro for implementing all the timeops methods on a python wrapper
///
/// # Arguments
/// * obj: The name of the struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing `TimeOps`
/// * window_type: The `WindowViewType` of `field` (note that this should have an `IntoPy<PyObject>` implementation
macro_rules! impl_timeops {
    ($obj:ty, $field:ident, $base_type:ty, $name:literal) => {
        #[pyo3::pymethods]
        impl $obj {
            #[doc = concat!(r" Gets the start time for rolling and expanding windows for this ", $name)]
            ///
            /// Returns:
            #[doc = concat!(r"    The earliest time that this ", $name, r" is valid or None if the ", $name, r" is valid for all times.")]
            #[getter]
            pub fn start(&self) -> Option<i64> {
                self.$field.start()
            }

            #[doc = concat!(r" Gets the earliest datetime that this ", $name, r" is valid")]
            ///
            /// Returns:
            #[doc = concat!(r"     The earliest datetime that this ", $name, r" is valid or None if the ", $name, r" is valid for all times.")]
            #[getter]
            pub fn start_date_time(&self) -> Option<NaiveDateTime> {
                let start_time = self.$field.start()?;
                NaiveDateTime::from_timestamp_millis(start_time)
            }

            #[doc = concat!(r" Gets the latest time that this ", $name, r" is valid.")]
            ///
            /// Returns:
            #[doc = concat!("   The latest time that this ", $name, r" is valid or None if the ", $name, r" is valid for all times.")]
            #[getter]
            pub fn end(&self) -> Option<i64> {
                self.$field.end()
            }

            #[doc = concat!(r" Gets the latest datetime that this ", $name, r" is valid")]
            ///
            /// Returns:
            #[doc = concat!(r"     The latest datetime that this ", $name, r" is valid or None if the ", $name, r" is valid for all times.")]
            #[getter]
            pub fn end_date_time(&self) -> Option<NaiveDateTime> {
                let end_time = self.$field.end()?;
                NaiveDateTime::from_timestamp_millis(end_time)
            }

            #[doc = concat!(r" Get the window size (difference between start and end) for this ", $name)]
            #[getter]
            pub fn window_size(&self) -> Option<u64> {
                self.$field.window_size()
            }

            /// Creates a `WindowSet` with the given `step` size using an expanding window.
            ///
            /// An expanding window is a window that grows by `step` size at each iteration.
            ///
            /// Arguments:
            ///     step (int): The step size of the window.
            ///
            /// Returns:
            ///     A `WindowSet` object.
            fn expanding(&self, step: PyInterval) -> Result<WindowSet<$base_type>, ParseTimeError> {
                self.$field.expanding(step)
            }

            /// Creates a `WindowSet` with the given `window` size and optional `step` using a rolling window.
            ///
            /// A rolling window is a window that moves forward by `step` size at each iteration.
            ///
            /// Arguments:
            ///     window: The size of the window.
            ///     step: The step size of the window. Defaults to the window size.
            ///
            /// Returns:
            ///     A `WindowSet` object.
            fn rolling(
                &self,
                window: PyInterval,
                step: Option<PyInterval>,
            ) -> Result<WindowSet<$base_type>, ParseTimeError> {
                self.$field.rolling(window, step)
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events between `start` (inclusive) and `end` (exclusive)")]
            ///
            /// Arguments:
            #[doc = concat!(r"     start: The start time of the window. Defaults to the start time of the ", $name, r".")]
            #[doc = concat!(r"     end: The end time of the window. Defaults to the end time of the ", $name, r".")]
            ///
            /// Returns:
            #[doc = concat!("r    A ", $name, " object.")]
            #[pyo3(signature = (start = None, end = None))]
            pub fn window(
                &self,
                start: Option<PyTime>,
                end: Option<PyTime>,
            ) -> <$base_type as TimeOps>::WindowedViewType {
                self.$field
                    .window(start.unwrap_or(PyTime::MIN), end.unwrap_or(PyTime::MAX))
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events at `time`.")]
            ///
            /// Arguments:
            ///     time: The time of the window.
            ///
            /// Returns:
            #[doc = concat!(r"     A ", $name, r" object.")]
            pub fn at(&self, time: PyTime) -> <$base_type as TimeOps>::WindowedViewType {
                self.$field.at(time)
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events before `end` (exclusive).")]
            ///
            /// Arguments:
            ///     end: The end time of the window.
            ///
            /// Returns:
            #[doc = concat!(r"     A ", $name, r" object.")]
            pub fn before(&self, end: PyTime) -> <$base_type as TimeOps>::WindowedViewType {
                self.$field.before(end)
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events after `start` (exclusive).")]
            ///
            /// Arguments:
            ///     start: The start time of the window.
            ///
            /// Returns:
            #[doc = concat!(r"     A ", $name, r" object.")]
            pub fn after(&self, start: PyTime) -> <$base_type as TimeOps>::WindowedViewType {
                self.$field.after(start)
            }
        }
    };
}
