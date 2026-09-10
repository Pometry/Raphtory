use glam::DVec2;
use raphtory_api::core::entities::GID;
use std::collections::HashMap;

pub mod cohesive_fruchterman_reingold;
pub mod fruchterman_reingold;

pub type NodeVectors = HashMap<GID, DVec2>;
