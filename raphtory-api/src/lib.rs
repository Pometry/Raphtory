pub mod atomic_extra;
pub mod compute;
pub mod core;
#[cfg(feature = "python")]
pub mod python;

pub mod inherit;
pub mod iter;
pub mod to_millis;

use serde::{Deserialize, Serialize};

#[derive(PartialOrd, PartialEq, Debug, Serialize, Deserialize)]
pub enum GraphType {
    EventGraph,
    PersistentGraph,
}

impl GraphType {
    pub fn is_event_graph(&self) -> bool {
        match self {
            GraphType::EventGraph => true,
            _ => false,
        }
    }

    pub fn is_persistent_graph(&self) -> bool {
        match self {
            GraphType::PersistentGraph => true,
            _ => false,
        }
    }
}
