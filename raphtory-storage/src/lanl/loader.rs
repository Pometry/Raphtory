use std::{time::Instant};
use std::collections::HashMap;
use raphtory::arrow::{Error, graph::TemporalGraph, load::ExternalEdgeList};

pub fn load_from_dir(graph_dir: &str) -> Result<TemporalGraph, Error> {
    let now = Instant::now();
    let graph_loaded_from_dir = TemporalGraph::new(&graph_dir);
    println!("Graph loaded in {:?} from dir {}", now.elapsed(), graph_dir);
    graph_loaded_from_dir
}

pub fn load_from_parquet(graph_dir: &str, layernames_parquet_dirs: HashMap<&str, &str>) -> Result<TemporalGraph, Error> {
    let num_threads = 8;
    let chunk_size = 8_388_608;
    let t_props_chunk_size = 20_970_100;

    let now = Instant::now();

    let layered_edge_list: Vec<ExternalEdgeList<&str>> = layernames_parquet_dirs
        .iter()
        .map(|(event_name, file)| {
            ExternalEdgeList::new(
                *event_name,
                *file,
                "src",
                "src_hash",
                "dst",
                "dst_hash",
                "epoch_time",
            )
                .expect("Failed to load events")
        })
        .collect::<Vec<_>>();

    let graph_loaded_from_parquet = TemporalGraph::from_edge_lists(
        num_threads,
        chunk_size,
        t_props_chunk_size,
        None,
        None,
        graph_dir,
        layered_edge_list,
    );

    println!("Graph loaded in {:?} from parquet files {:?}", now.elapsed(), layernames_parquet_dirs.values());
    graph_loaded_from_parquet
}
