pub mod core;
pub mod db;

pub enum FilterState {
    Neither,
    Both,
    BothIndependent,
    Nodes,
    Edges,
}