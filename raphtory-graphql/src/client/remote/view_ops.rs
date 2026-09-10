//! The view-op surface shared by every remote handle.
//!
//! Each handle wraps the same 17 lazy view operations, differing only in the
//! type they return, so they are generated from one definition rather than
//! written out per handle: adding a `ViewOp` means editing the enum, the
//! renderer, the parser and this file, not fourteen handle files.

/// Implement the shared lazy view operations for a remote handle.
///
/// The handle must provide `fn with_view_op(&self, op: ViewOp) -> Self`.
macro_rules! remote_view_ops {
    ($ty:ident) => {
        impl $ty {
            /// Time-window this handle. Lazy — no RPC.
            pub fn window(&self, start: InputTime, end: InputTime) -> $ty {
                self.with_view_op(ViewOp::Window { start, end })
            }

            /// Restrict to a single named layer. Lazy — no RPC.
            pub fn layer(&self, name: impl ToString) -> $ty {
                self.with_view_op(ViewOp::Layer {
                    name: name.to_string(),
                })
            }

            /// Snapshot at a specific time. Lazy — no RPC.
            pub fn at(&self, time: InputTime) -> $ty {
                self.with_view_op(ViewOp::At { time })
            }

            /// Restrict to events strictly before the given time. Lazy — no RPC.
            pub fn before(&self, time: InputTime) -> $ty {
                self.with_view_op(ViewOp::Before { time })
            }

            /// Restrict to events strictly after the given time. Lazy — no RPC.
            pub fn after(&self, time: InputTime) -> $ty {
                self.with_view_op(ViewOp::After { time })
            }

            /// Latest state. Lazy — no RPC.
            pub fn latest(&self) -> $ty {
                self.with_view_op(ViewOp::Latest)
            }

            /// Snapshot at the latest time. Lazy — no RPC.
            pub fn snapshot_latest(&self) -> $ty {
                self.with_view_op(ViewOp::SnapshotLatest)
            }

            /// Snapshot at a specific time. Lazy — no RPC.
            pub fn snapshot_at(&self, time: InputTime) -> $ty {
                self.with_view_op(ViewOp::SnapshotAt { time })
            }

            /// Exclude a specific layer. Lazy — no RPC.
            pub fn exclude_layer(&self, name: impl ToString) -> $ty {
                self.with_view_op(ViewOp::ExcludeLayer {
                    name: name.to_string(),
                })
            }

            /// Shrink the start of the current window. Lazy — no RPC.
            pub fn shrink_start(&self, start: InputTime) -> $ty {
                self.with_view_op(ViewOp::ShrinkStart { start })
            }

            /// Shrink the end of the current window. Lazy — no RPC.
            pub fn shrink_end(&self, end: InputTime) -> $ty {
                self.with_view_op(ViewOp::ShrinkEnd { end })
            }

            /// Restrict to the default layer. Lazy — no RPC.
            pub fn default_layer(&self) -> $ty {
                self.with_view_op(ViewOp::DefaultLayer)
            }

            /// Restrict to the given set of layers. Lazy — no RPC.
            pub fn layers(&self, names: Vec<String>) -> $ty {
                self.with_view_op(ViewOp::Layers {
                    names: names.into(),
                })
            }

            /// Exclude the given set of layers. Lazy — no RPC.
            pub fn exclude_layers(&self, names: Vec<String>) -> $ty {
                self.with_view_op(ViewOp::ExcludeLayers {
                    names: names.into(),
                })
            }

            /// Restrict to the given set of valid layers. Lazy — no RPC.
            pub fn valid_layers(&self, names: Vec<String>) -> $ty {
                self.with_view_op(ViewOp::ValidLayers {
                    names: names.into(),
                })
            }

            /// Exclude a specific valid layer from the view. Lazy — no RPC.
            pub fn exclude_valid_layer(&self, name: impl ToString) -> $ty {
                self.with_view_op(ViewOp::ExcludeValidLayer {
                    name: name.to_string(),
                })
            }

            /// Exclude the given set of valid layers from the view. Lazy — no RPC.
            pub fn exclude_valid_layers(&self, names: Vec<String>) -> $ty {
                self.with_view_op(ViewOp::ExcludeValidLayers {
                    names: names.into(),
                })
            }
        }
    };
}

pub(crate) use remote_view_ops;
