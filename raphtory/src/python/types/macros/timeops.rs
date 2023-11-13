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
            #[doc = concat!(r" Gets the earliest time that this ", $name, r" is valid")]
            ///
            /// Returns:
            #[doc = concat!(r"    The earliest time that this ", $name, r" is valid or None if the ", $name, r" is valid for all times.")]
            #[getter]
            pub fn start(&self) -> Option<i64> {
                self.$field.start()
            }

            /// Gets the earliest datetime that this vertex is valid
            ///
            /// Returns:
            ///     The earliest datetime that this vertex is valid or None if the vertex is valid for all times.
            #[getter]
            pub fn start_date_time(&self) -> Option<NaiveDateTime> {
                let start_time = self.$field.start()?;
                NaiveDateTime::from_timestamp_millis(start_time)
            }

            /// Gets the latest time that this vertex is valid.
            ///
            /// Returns:
            ///   The latest time that this vertex is valid or None if the vertex is valid for all times.
            #[getter]
            pub fn end(&self) -> Option<i64> {
                self.$field.end()
            }

            /// Gets the latest datetime that this vertex is valid
            ///
            /// Returns:
            ///     The latest datetime that this vertex is valid or None if the vertex is valid for all times.
            #[getter]
            pub fn end_date_time(&self) -> Option<NaiveDateTime> {
                let end_time = self.$field.end()?;
                NaiveDateTime::from_timestamp_millis(end_time)
            }

            #[getter]
            pub fn window_size(&self) -> Option<u64> {
                self.$field.window_size()
            }

            /// Creates a `PyVertexWindowSet` with the given `step` size and optional `start` and `end` times,
            /// using an expanding window.
            ///
            /// An expanding window is a window that grows by `step` size at each iteration.
            /// This will tell you whether a vertex exists at different points in the window and what
            /// its properties are at those points.
            ///
            /// Arguments:
            ///     step (int): The step size of the window.
            ///
            /// Returns:
            ///     A `PyVertexWindowSet` object.
            fn expanding(&self, step: PyInterval) -> Result<WindowSet<$base_type>, ParseTimeError> {
                self.$field.expanding(step)
            }

            /// Creates a `PyVertexWindowSet` with the given `window` size and optional `step`, `start` and `end` times,
            /// using a rolling window.
            ///
            /// A rolling window is a window that moves forward by `step` size at each iteration.
            /// This will tell you whether a vertex exists at different points in the window and what
            /// its properties are at those points.
            ///
            /// Arguments:
            ///     window: The size of the window.
            ///     step: The step size of the window. Defaults to the window size.
            ///
            /// Returns:
            ///     A `PyVertexWindowSet` object.
            fn rolling(
                &self,
                window: PyInterval,
                step: Option<PyInterval>,
            ) -> Result<WindowSet<$base_type>, ParseTimeError> {
                self.$field.rolling(window, step)
            }

            // #[doc =r" Create a view of the `{}` including all events between `start` (inclusive) and `end` (exclusive)", stringify!($obj))]
            ///
            /// Arguments:
            ///     start (int, str or datetime(utc)): The start time of the window. Defaults to the start time of the vertex.
            ///     end (int, str or datetime(utc)): The end time of the window. Defaults to the end time of the vertex.
            ///
            /// Returns:
            ///    A `PyVertex` object.
            #[pyo3(signature = (start = None, end = None))]
            pub fn window(
                &self,
                start: Option<PyTime>,
                end: Option<PyTime>,
            ) -> <$base_type as TimeOps>::WindowedViewType {
                self.$field
                    .window(start.unwrap_or(PyTime::MIN), end.unwrap_or(PyTime::MAX))
            }

            /// Create a view of the vertex including all events at `t`.
            ///
            /// Arguments:
            ///     end (int, str or datetime(utc)): The time of the window.
            ///
            /// Returns:
            ///     A `PyVertex` object.
            #[pyo3(signature = (end))]
            pub fn at(&self, end: PyTime) -> <$base_type as TimeOps>::WindowedViewType {
                self.$field.at(end)
            }

            pub fn before(&self, end: PyTime) -> <$base_type as TimeOps>::WindowedViewType {
                self.$field.before(end)
            }

            pub fn after(&self, start: PyTime) -> <$base_type as TimeOps>::WindowedViewType {
                self.$field.after(start)
            }
        }
    };
}
