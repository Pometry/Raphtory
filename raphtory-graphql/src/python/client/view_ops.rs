//! The Python view-op surface shared by every remote handle.
//!
//! The bindings are pure delegation to the Rust client's shared view ops
//! (`client::remote::view_ops`), identical across handles apart from the type
//! they return, so they are generated here rather than written out per handle.

/// Implement the shared lazy view operations on a Python remote handle.
///
/// `$py` is the pyclass, `$field` the field holding the Rust handle, and
/// `$name` the Python type name used in the generated docstrings.
macro_rules! py_remote_view_ops {
    ($py:ident, $field:ident, $name:literal) => {
        #[pyo3::pymethods]
        impl $py {
            /// Time-window this handle. Lazy — no RPC.
            ///
            /// Arguments:
            ///     start (TimeInput): inclusive start of the window.
            ///     end (TimeInput): exclusive end of the window.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view restricted to the window.")]
            pub fn window(&self, start: InputTime, end: InputTime) -> $py {
                $py::new(self.$field.window(start, end))
            }

            /// Restrict to a single named layer. Lazy — no RPC.
            ///
            /// Arguments:
            ///     name (str): the name of the layer.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view restricted to that layer.")]
            pub fn layer(&self, name: &str) -> $py {
                $py::new(self.$field.layer(name))
            }

            /// View including all events at a specific time. Lazy — no RPC.
            ///
            /// Arguments:
            ///     time (TimeInput): the time to view.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view of that time.")]
            pub fn at(&self, time: InputTime) -> $py {
                $py::new(self.$field.at(time))
            }

            /// Restrict to events strictly before the given time. Lazy — no RPC.
            ///
            /// Arguments:
            ///     time (TimeInput): only events strictly before this time are kept.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view restricted to events before that time.")]
            pub fn before(&self, time: InputTime) -> $py {
                $py::new(self.$field.before(time))
            }

            /// Restrict to events strictly after the given time. Lazy — no RPC.
            ///
            /// Arguments:
            ///     time (TimeInput): only events strictly after this time are kept.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view restricted to events after that time.")]
            pub fn after(&self, time: InputTime) -> $py {
                $py::new(self.$field.after(time))
            }

            /// Latest state. Lazy — no RPC.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view of the latest state.")]
            pub fn latest(&self) -> $py {
                $py::new(self.$field.latest())
            }

            /// Snapshot at the latest time. Lazy — no RPC.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view snapshotted at the latest time.")]
            pub fn snapshot_latest(&self) -> $py {
                $py::new(self.$field.snapshot_latest())
            }

            /// Snapshot at a specific time. Lazy — no RPC.
            ///
            /// Arguments:
            ///     time (TimeInput): the time to snapshot at.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view snapshotted at that time.")]
            pub fn snapshot_at(&self, time: InputTime) -> $py {
                $py::new(self.$field.snapshot_at(time))
            }

            /// Exclude a specific layer. Lazy — no RPC.
            ///
            /// Arguments:
            ///     name (str): the name of the layer to exclude.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view with that layer excluded.")]
            pub fn exclude_layer(&self, name: &str) -> $py {
                $py::new(self.$field.exclude_layer(name))
            }

            /// Shrink the start of the current window. Lazy — no RPC.
            ///
            /// Arguments:
            ///     start (TimeInput): the new inclusive start of the window.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view with the window start shrunk.")]
            pub fn shrink_start(&self, start: InputTime) -> $py {
                $py::new(self.$field.shrink_start(start))
            }

            /// Shrink the end of the current window. Lazy — no RPC.
            ///
            /// Arguments:
            ///     end (TimeInput): the new exclusive end of the window.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view with the window end shrunk.")]
            pub fn shrink_end(&self, end: InputTime) -> $py {
                $py::new(self.$field.shrink_end(end))
            }

            /// Restrict to the default layer. Lazy — no RPC.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view restricted to the default layer.")]
            pub fn default_layer(&self) -> $py {
                $py::new(self.$field.default_layer())
            }

            /// Restrict to the given set of layers. Lazy — no RPC.
            ///
            /// Arguments:
            ///     names (list[str]): the names of the layers.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view restricted to those layers.")]
            pub fn layers(&self, names: Vec<String>) -> $py {
                $py::new(self.$field.layers(names))
            }

            /// Exclude the given set of layers. Lazy — no RPC.
            ///
            /// Arguments:
            ///     names (list[str]): the names of the layers to exclude.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view with those layers excluded.")]
            pub fn exclude_layers(&self, names: Vec<String>) -> $py {
                $py::new(self.$field.exclude_layers(names))
            }

            /// Restrict to the given set of valid layers. Lazy — no RPC.
            ///
            /// Arguments:
            ///     names (list[str]): the names of the valid layers.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view restricted to those valid layers.")]
            pub fn valid_layers(&self, names: Vec<String>) -> $py {
                $py::new(self.$field.valid_layers(names))
            }

            /// Exclude a specific valid layer from the view. Lazy — no RPC.
            ///
            /// Arguments:
            ///     name (str): the name of the valid layer to exclude.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view with that valid layer excluded.")]
            pub fn exclude_valid_layer(&self, name: &str) -> $py {
                $py::new(self.$field.exclude_valid_layer(name))
            }

            /// Exclude the given set of valid layers from the view. Lazy — no RPC.
            ///
            /// Arguments:
            ///     names (list[str]): the names of the valid layers to exclude.
            ///
            /// Returns:
            #[doc = concat!("    ", $name, ": a new view with those valid layers excluded.")]
            pub fn exclude_valid_layers(&self, names: Vec<String>) -> $py {
                $py::new(self.$field.exclude_valid_layers(names))
            }
        }
    };
}

pub(crate) use py_remote_view_ops;
