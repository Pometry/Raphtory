use glam::Vec2;
use std::collections::HashMap;

pub mod cohesive_fruchterman_reingold;
pub mod fruchterman_reingold;

pub type NodeVectors = HashMap<u64, Vec2>;
