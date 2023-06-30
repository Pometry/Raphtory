//! A data structure for sharding a temporal graph.
//!
//! When a raphtory graph is created, the code will automatically shard the graph depending
//! on how many shards you set the graph to originally have when initializing it.
//!
//! For example, Graph::new() will create a graph with 4 shards.
//!
//! Each of these shards will be stored in a separate file, and will be loaded into memory when needed.
//!
//! Each shard will have its own set of vertex and edge data, and will be able to be queried independently.

use std::{hash::BuildHasherDefault, ops::Deref};

use dashmap::mapref::one::Ref;

pub enum LockedView<'a, T> {
    Locked(parking_lot::MappedRwLockReadGuard<'a, T>),
    DashMap(Ref<'a, usize, T, BuildHasherDefault<rustc_hash::FxHasher>>),
}

impl<'a, T> Deref for LockedView<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            LockedView::Locked(guard) => guard.deref(),
            LockedView::DashMap(r) => (*r).deref(),
        }
    }
}
