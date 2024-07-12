//! Provides functionality for generating graphs for testing and benchmarking.

use rand::Rng;
use raphtory_api::core::entities::GID;

use crate::prelude::*;

pub mod preferential_attachment;
pub mod random_attachment;

pub(crate) fn next_id<'graph, G: GraphViewOps<'graph>>(g: &G, max_gid: Option<GID>) -> GID {
    let max_gid = max_gid.unwrap_or_else(|| g.nodes().id().max().unwrap_or(GID::U64(0)));
    match max_gid {
        GID::U64(id) => GID::U64(id + 1),
        GID::I64(id) => GID::I64(id + 1),
        GID::Str(_) => {
            let mut rng = rand::thread_rng();
            loop {
                let new_id = GID::Str(rng.gen::<u64>().to_string());
                if g.node(&new_id).is_none() {
                    break new_id;
                }
            }
        }
    }
}
