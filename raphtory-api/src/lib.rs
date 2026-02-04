pub mod atomic_extra;
pub mod compute;
pub mod core;
#[cfg(feature = "python")]
pub mod python;

pub mod inherit;
pub mod iter;

use serde::{Deserialize, Serialize};

#[derive(PartialOrd, PartialEq, Debug, Serialize, Deserialize)]
pub enum GraphType {
    EventGraph,
    PersistentGraph,
}
