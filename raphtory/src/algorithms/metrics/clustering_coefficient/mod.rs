use crate::{core::entities::nodes::node_ref::AsNodeRef, db::api::view::*};
use raphtory_api::core::entities::VID;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rustc_hash::FxHashSet;

pub mod global_clustering_coefficient;
pub mod local_clustering_coefficient;
pub mod local_clustering_coefficient_batch_intersection;
pub mod local_clustering_coefficient_batch_path;

fn filter_nodes<G: StaticGraphViewOps, V: AsNodeRef>(
    graph: &G,
    v: &Vec<V>,
) -> (FxHashSet<VID>, FxHashSet<VID>) {
    // figure out optimal way to apply these operations
    let src_nodes: FxHashSet<VID> = v
        .into_iter()
        .map(|node| graph.node(node).unwrap().node)
        .collect();

    let neighbours: FxHashSet<VID> = src_nodes
        .par_iter()
        .map(|node| {
            graph
                .node(node)
                .unwrap()
                .neighbours()
                .iter()
                .map(|nbor| nbor.node)
                .collect()
        })
        .reduce(
            || FxHashSet::default(),
            |mut acc, neighbors| {
                acc.extend(neighbors);
                acc
            },
        );
    (src_nodes.union(&neighbours).copied().collect(), src_nodes)
}
