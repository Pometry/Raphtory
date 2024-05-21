use raphtory_arrow::{graph::TemporalGraph, load::ExternalEdgeList};

use raphtory_storage::lanl::exfiltration::count::query_total;
use std::time::Instant;

fn main() {
    let window = 30; // change me to get bigger windows
    let graph_dir = std::env::args()
        .nth(1)
        .expect("please supply a graph directory");

    println!("graph_dir: {:?}", graph_dir);

    let now = Instant::now();
    let graph = if std::fs::read_dir(&graph_dir).is_ok() {
        TemporalGraph::new(&graph_dir).expect("failed to load graph")
    } else {
        let netflow_dir = std::env::args()
            .nth(2)
            .expect("please supply a wls directory");

        let v1_dir = std::env::args()
            .nth(3)
            .expect("please supply a v1 directory");

        let v2_dir = std::env::args()
            .nth(4)
            .expect("please supply a v2 directory");

        println!("netflow_dir: {:?}", netflow_dir);
        println!("v1_dir: {:?}", v1_dir);
        println!("v2_dir: {:?}", v2_dir);

        let layered_edge_list = [
            ExternalEdgeList::new("netflow", netflow_dir, "src", "dst", "epoch_time")
                .expect("failed to load netflow"),
            ExternalEdgeList::new("events_1v", v1_dir, "src", "dst", "epoch_time")
                .expect("failed to load events_v1"),
            ExternalEdgeList::new("events_2v", v2_dir, "src", "dst", "epoch_time")
                .expect("failed to load events_v2"),
        ];
        let chunk_size = 8_388_608;
        let t_props_chunk_size = 20_970_100;
        let graph = TemporalGraph::from_edge_lists(
            8,
            chunk_size,
            t_props_chunk_size,
            None,
            None,
            graph_dir,
            layered_edge_list,
            None,
        )
        .expect("failed to load graph");
        graph
    };
    println!("Time taken to load graph: {:?}", now.elapsed());

    let now = Instant::now();

    let count: usize = query_total(&graph, window);

    println!("Time taken: {:?}, count: {:?}", now.elapsed(), count);
}
