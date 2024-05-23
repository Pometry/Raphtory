use raphtory_arrow::{graph::TemporalGraph, load::ExternalEdgeList};
use raphtory_storage::lanl::exfiltration::query1;
use std::time::Instant;

fn main() {
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

        let num_threads = std::env::args()
            .nth(5)
            .and_then(|x| x.parse::<usize>().ok())
            .unwrap_or(8);
        let chunk_size = std::env::args()
            .nth(6)
            .and_then(|x| x.parse::<usize>().ok())
            .unwrap_or(8_388_608);
        let t_props_chunk_size = std::env::args()
            .nth(7)
            .and_then(|x| x.parse::<usize>().ok())
            .unwrap_or(20_970_100);

        println!("netflow_dir: {:?}", netflow_dir);
        println!("v1_dir: {:?}", v1_dir);
        println!("v2_dir: {:?}", v2_dir);
        println!("chunk_size: {:?}", chunk_size);
        println!("t_props_chunk_size: {:?}", t_props_chunk_size);
        println!("num_threads: {:?}", num_threads);

        let layered_edge_list = [
            ExternalEdgeList::new("netflow", netflow_dir, "src", "dst", "epoch_time")
                .expect("failed to load netflow"),
            ExternalEdgeList::new("events_1v", v1_dir, "src", "dst", "epoch_time")
                .expect("failed to load events_v1"),
            ExternalEdgeList::new("events_2v", v2_dir, "src", "dst", "epoch_time")
                .expect("failed to load events_v2"),
        ];
        let graph = TemporalGraph::from_edge_lists(
            num_threads,
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

    println!("graph nodes count {}", graph.num_nodes());
    println!("graph edges count netflow {}", graph.num_edges(0));
    println!("graph edges count 1v {}", graph.num_edges(1));
    println!("graph edges count 2v {}", graph.num_edges(2));
    let now = Instant::now();

    //     MATCH
    //     (E)<-[nf1:Netflow]-(B)<-[login1:Events2v]-(A), (B)<-[prog1:Events1v]-(B)
    //   WHERE A <> B AND B <> E AND A <> E
    //     AND login1.eventID = 4624
    //     AND prog1.eventID = 4688
    //     AND nf1.dstBytes > 100000000
    //     // time constraints within each path
    //     AND login1.epochtime < prog1.epochtime
    //     AND prog1.epochtime < nf1.epochtime
    //     AND nf1.epochtime - login1.epochtime <= 30
    //   RETURN count(*)

    // Launched job 7
    // Number of answers: 2,992,551
    // CPU times: user 25.9 ms, sys: 11.7 ms, total: 37.6 ms
    // Wall time: 21.8 s

    // Devices (vertices): 159,245
    // Netflow (edges): 317,164,045
    // Host event 1-vertex (edges): 33,480,483
    // Host event 2-vertex (edges): 97,716,529
    // Total (edges): 448,361,057

    let count = query1::run(&graph);

    println!("Time taken: {:?}, count: {:?}", now.elapsed(), count);
}
