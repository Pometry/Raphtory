pub mod atomic_extra;
pub mod compute;
pub mod core;
#[cfg(feature = "python")]
pub mod python;

pub mod inherit;
pub mod iter;
pub mod to_millis;

use parking_lot::RawRwLock;
use serde::{Deserialize, Serialize};

pub type ArcRwLockWriteGuard<T> = parking_lot::lock_api::ArcRwLockWriteGuard<RawRwLock, T>;
pub type ArcRwLockReadGuard<T> = parking_lot::lock_api::ArcRwLockReadGuard<RawRwLock, T>;

#[derive(PartialOrd, PartialEq, Debug, Serialize, Deserialize)]
pub enum GraphType {
    EventGraph,
    PersistentGraph,
}

impl GraphType {
    pub fn is_event_graph(&self) -> bool {
        matches!(self, GraphType::EventGraph)
    }

    pub fn is_persistent_graph(&self) -> bool {
        matches!(self, GraphType::PersistentGraph)
    }
}
