/// Macro for implementing all the timeops methods on a python wrapper
///
/// # Arguments
/// * obj: The struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing `TimeOps`
/// * base_type: The rust type of `field` (note that `<$base_type as TimeOps<'static>>::WindowedViewType`
///              and `WindowSet<$base_type>` should have an `IntoPy<PyObject>` implementation)
/// * name: The name of the object that appears in the docstring
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
            pub fn start_date_time(&self) -> Option<DateTime<Utc>> {
                self.$field.start_date_time()
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
            pub fn end_date_time(&self) -> Option<DateTime<Utc>> {
                self.$field.end_date_time()
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
            ///     step (int | str): The step size of the window.
            ///
            /// Returns:
            ///     A `WindowSet` object.
            fn expanding(&self, step: PyInterval) -> Result<WindowSet<'static, $base_type>, ParseTimeError> {
                self.$field.expanding(step)
            }

            /// Creates a `WindowSet` with the given `window` size and optional `step` using a rolling window.
            ///
            /// A rolling window is a window that moves forward by `step` size at each iteration.
            ///
            /// Arguments:
            ///     window (int | str): The size of the window.
            ///     step (int | str | None): The step size of the window. Defaults to `window`.
            ///
            /// Returns:
            ///     A `WindowSet` object.
            fn rolling(
                &self,
                window: PyInterval,
                step: Option<PyInterval>,
            ) -> Result<WindowSet<'static, $base_type>, ParseTimeError> {
                self.$field.rolling(window, step)
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events between `start` (inclusive) and `end` (exclusive)")]
            ///
            /// Arguments:
            ///     start (int | DateTime | str | None): The start time of the window (unbounded if `None`).
            ///     end (int | DateTime | str | None): The end time of the window (unbounded if `None`).
            ///
            /// Returns:
            #[doc = concat!("r    A ", $name, " object.")]
            pub fn window(
                &self,
                start: PyTime,
                end: PyTime,
            ) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field
                    .window(start, end)
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events at `time`.")]
            ///
            /// Arguments:
            ///     time (int | DateTime | str): The time of the window.
            ///
            /// Returns:
            #[doc = concat!(r"     A ", $name, r" object.")]
            pub fn at(&self, time: PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.at(time)
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events before `end` (exclusive).")]
            ///
            /// Arguments:
            ///     end (int | DateTime | str): The end time of the window.
            ///
            /// Returns:
            #[doc = concat!(r"     A ", $name, r" object.")]
            pub fn before(&self, end: PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.before(end)
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events after `start` (exclusive).")]
            ///
            /// Arguments:
            ///     start (int | DateTime | str): The start time of the window.
            ///
            /// Returns:
            #[doc = concat!(r"     A ", $name, r" object.")]
            pub fn after(&self, start: PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.after(start)
            }

            /// Set the start of the window to the larger of `start` and `self.start()`
            ///
            /// Arguments:
            ///    start (int | DateTime | str): the new start time of the window
            ///
            /// Returns:
            #[doc = concat!(r"     A ", $name, r" object.")]
            pub fn shrink_start(&self, start: PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.shrink_start(start)
            }

            /// Set the end of the window to the smaller of `end` and `self.end()`
            ///
            /// Arguments:
            ///     end (int | DateTime | str): the new end time of the window
            /// Returns:
            #[doc = concat!(r"     A ", $name, r" object.")]
            fn shrink_end(&self, end: PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                    self.$field.shrink_end(end)
            }

            /// Shrink both the start and end of the window (same as calling `shrink_start` followed by `shrink_end` but more efficient)
            ///
            /// Arguments:
            ///
            fn shrink_window(&self, start: PyTime, end: PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.shrink_window(start, end)
            }
        }
    };
}
