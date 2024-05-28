pub mod entities;
pub mod storage;

/// Denotes the direction of an edge. Can be incoming, outgoing or both.
#[derive(
    Clone,
    Copy,
    Hash,
    Eq,
    PartialEq,
    PartialOrd,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum Direction {
    OUT,
    IN,
    #[default]
    BOTH,
}
