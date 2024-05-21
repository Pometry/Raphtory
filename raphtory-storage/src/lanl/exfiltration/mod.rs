use raphtory::core::entities::VID;
use raphtory_arrow::graph_fragment::TempColGraphFragment;
use rayon::iter::ParallelIterator;

pub mod count;
pub mod list;
pub mod query1;

#[inline]
fn find_active_nodes(layer: &TempColGraphFragment) -> Vec<VID> {
    layer
        .all_nodes_par()
        .filter(|v| v.out_degree() > 0)
        .map(|node| node.vid().into())
        .collect()
}

#[cfg(test)]
mod test {
    use super::{count::query as count, list::query_all_log_vertices as list};
    use raphtory_arrow::graph::TemporalGraph;
    use rayon::prelude::*;
    use std::path::Path;

    #[test]
    #[ignore]
    fn consistency_check() {
        let data: &Path = "/Users/lucasjeub/Data/netflow".as_ref();
        let graph = TemporalGraph::new(data).unwrap();

        let mut vertex_counts_simple: Vec<_> =
            count(&graph, 30).unwrap().filter(|(_, c)| *c > 0).collect();
        vertex_counts_simple.sort();

        let mut vertex_counts_list: Vec<_> = list(&graph, 30)
            .unwrap()
            .map(|(v, iter)| (v, iter.count()))
            .filter(|(_, c)| *c > 0)
            .collect();
        vertex_counts_list.sort();
        assert_eq!(vertex_counts_simple, vertex_counts_list);
    }
}
