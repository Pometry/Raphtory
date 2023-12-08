use raphtory::{
    arrow::{col_graph2::TempColGraphFragment, global_order::GlobalOrder, graph::TemporalGraph},
    core::entities::VID,
};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

pub mod count;
pub mod list;

fn find_active_nodes(layer: &TempColGraphFragment) -> impl ParallelIterator<Item = VID> + '_ {
    let chunk_size = layer.vertex_chunk_size();
    layer
        .outbound()
        .par_iter()
        .enumerate()
        .flat_map_iter(move |(chunk_id, chunk)| {
            let chunk_start = chunk_id * chunk_size;
            chunk
                .adj()
                .offsets()
                .lengths()
                .enumerate()
                .filter_map(move |(i, d)| (d > 0).then(|| VID(chunk_start + i)))
        })
}

#[cfg(test)]
mod test {
    use super::{count::query as count, list::query_all_log_vertices as list};
    use raphtory::arrow::graph::TemporalGraph;
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
