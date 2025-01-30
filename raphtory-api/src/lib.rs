pub mod atomic_extra;
pub mod compute;
pub mod core;
#[cfg(feature = "python")]
pub mod python;

pub mod iter;

#[derive(PartialOrd, PartialEq, Debug)]
pub enum GraphType {
    EventGraph,
    PersistentGraph,
}
