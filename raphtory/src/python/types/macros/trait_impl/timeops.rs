/// Macro for implementing all the timeops methods on a python wrapper
///
/// # Arguments
/// * obj: The struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing `TimeOps`
/// * base_type: The rust type of `field` (note that `<$base_type as TimeOps<'static>>::WindowedViewType`
///              and `WindowSet<$base_type>` should have an `IntoPyObject` implementation)
/// * name: The name of the object that appears in the docstring
macro_rules! impl_timeops {
    ($obj:ty, $field:ident, $base_type:ty, $name:literal) => {
        #[pyo3::pymethods]
        impl $obj {
            #[doc = concat!(r" Gets the start time for rolling and expanding windows for this ", $name)]
            ///
            /// Returns:
            #[doc = concat!(r"    Optional[int]: The earliest time that this ", $name, r" is valid or None if the ", $name, r" is valid for all times.")]
            #[getter]
            pub fn start(&self) -> Option<i64> {
                self.$field.start()
            }

            #[doc = concat!(r" Gets the earliest datetime that this ", $name, r" is valid")]
            ///
            /// Returns:
            #[doc = concat!(r"     Optional[datetime]: The earliest datetime that this ", $name, r" is valid or None if the ", $name, r" is valid for all times.")]
            #[getter]
            pub fn start_date_time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
                self.$field.start_date_time()
            }

            #[doc = concat!(r" Gets the latest time that this ", $name, r" is valid.")]
            ///
            /// Returns:
            #[doc = concat!("   Optional[int]: The latest time that this ", $name, r" is valid or None if the ", $name, r" is valid for all times.")]
            #[getter]
            pub fn end(&self) -> Option<i64> {
                self.$field.end()
            }

            #[doc = concat!(r" Gets the latest datetime that this ", $name, r" is valid")]
            ///
            /// Returns:
            #[doc = concat!(r"     Optional[datetime]: The latest datetime that this ", $name, r" is valid or None if the ", $name, r" is valid for all times.")]
            #[getter]
            pub fn end_date_time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
                self.$field.end_date_time()
            }

            #[doc = concat!(r" Get the window size (difference between start and end) for this ", $name)]
            ///
            /// Returns:
            ///     Optional[int]:
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
            ///     WindowSet: A `WindowSet` object.
            fn expanding(&self, step: $crate::python::utils::PyInterval) -> Result<$crate::db::api::view::WindowSet<'static, $base_type>, $crate::core::utils::time::error::ParseTimeError> {
                self.$field.expanding(step)
            }

            /// Creates a `WindowSet` with the given `window` size and optional `step` using a rolling window.
            ///
            /// A rolling window is a window that moves forward by `step` size at each iteration.
            ///
            /// Arguments:
            ///     window (int | str): The size of the window.
            ///     step (int | str | None): The step size of the window.
            ///         `step` defaults to `window`.
            ///
            /// Returns:
            ///     WindowSet: A `WindowSet` object.
            #[pyo3(signature = (window, step=None))]
            fn rolling(
                &self,
                window: $crate::python::utils::PyInterval,
                step: Option<$crate::python::utils::PyInterval>,
            ) -> Result<$crate::db::api::view::WindowSet<'static, $base_type>, $crate::core::utils::time::error::ParseTimeError> {
                self.$field.rolling(window, step)
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events between `start` (inclusive) and `end` (exclusive)")]
            ///
            /// Arguments:
            ///     start (TimeInput | None): The start time of the window (unbounded if `None`).
            ///     end (TimeInput | None): The end time of the window (unbounded if `None`).
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ":")]
            pub fn window(
                &self,
                start: $crate::python::utils::PyTime,
                end: $crate::python::utils::PyTime,
            ) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field
                    .window(start, end)
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events at `time`.")]
            ///
            /// Arguments:
            ///     time (TimeInput): The time of the window.
            ///
            /// Returns:
            #[doc = concat!(r"     ", $name, ":")]
            pub fn at(&self, time: $crate::python::utils::PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.at(time)
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events at the latest time.")]
            ///
            /// Returns:
            #[doc = concat!(r"     ", $name, ":")]
            pub fn latest(&self) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.latest()
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events that have not been explicitly deleted at `time`.")]
            ///
            /// This is equivalent to `before(time + 1)` for `Graph` and `at(time)` for `PersistentGraph`
            ///
            /// Arguments:
            ///     time (TimeInput): The time of the window.
            ///
            /// Returns:
            #[doc = concat!(r"     ", $name, ":")]
            pub fn snapshot_at(&self, time: $crate::python::utils::PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.snapshot_at(time)
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events that have not been explicitly deleted at the latest time.")]
            ///
            /// This is equivalent to a no-op for `Graph` and `latest()` for `PersistentGraph`
            ///
            /// Returns:
            #[doc = concat!(r"     ", $name, ":")]
            pub fn snapshot_latest(&self) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.snapshot_latest()
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events before `end` (exclusive).")]
            ///
            /// Arguments:
            ///     end (TimeInput): The end time of the window.
            ///
            /// Returns:
            #[doc = concat!(r"     ", $name, ":")]
            pub fn before(&self, end: $crate::python::utils::PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.before(end)
            }

            #[doc = concat!(r" Create a view of the ", $name, r" including all events after `start` (exclusive).")]
            ///
            /// Arguments:
            ///     start (TimeInput): The start time of the window.
            ///
            /// Returns:
            #[doc = concat!(r"     ", $name, ":")]
            pub fn after(&self, start: $crate::python::utils::PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.after(start)
            }

            /// Set the start of the window to the larger of `start` and `self.start()`
            ///
            /// Arguments:
            ///    start (TimeInput): the new start time of the window
            ///
            /// Returns:
            #[doc = concat!(r"     ", $name, ":")]
            pub fn shrink_start(&self, start: $crate::python::utils::PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.shrink_start(start)
            }

            /// Set the end of the window to the smaller of `end` and `self.end()`
            ///
            /// Arguments:
            ///     end (TimeInput): the new end time of the window
            /// Returns:
            #[doc = concat!(r"     ", $name, ":")]
            fn shrink_end(&self, end: $crate::python::utils::PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                    self.$field.shrink_end(end)
            }

            /// Shrink both the start and end of the window (same as calling `shrink_start` followed by `shrink_end` but more efficient)
            ///
            /// Arguments:
            ///     start (TimeInput): the new start time for the window
            ///     end (TimeInput): the new end time for the window
            ///
            /// Returns:
            #[doc = concat!(r"     ", $name, ":")]
            fn shrink_window(&self, start: $crate::python::utils::PyTime, end: $crate::python::utils::PyTime) -> <$base_type as TimeOps<'static>>::WindowedViewType {
                self.$field.shrink_window(start, end)
            }
        }
    };
}
