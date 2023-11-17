use std::{env::Args, time::Instant};

use itertools::Itertools;
use raphtory::arrow::{graph::TemporalGraph, loader::ExternalEdgeList};

pub fn load_graph_from_params(args: Args) -> TemporalGraph {
    let args = args.into_iter().collect_vec();
    assert!(
        args.len() >= 1,
        "please supply at least 1 argument: graph_dir"
    );

    let graph_dir = &args[1];

    println!("graph_dir: {:?}", graph_dir);

    let now = Instant::now();
    let graph = if std::fs::read_dir(&graph_dir).is_ok() {
        TemporalGraph::new(&graph_dir).expect("failed to load graph")
    } else {
        assert!(
            args.len() >= 4,
            "please supply at least 4 arguments: graph_dir, netflow_dir, v1_dir, v2_dir"
        );

        let netflow_dir = &args[2];
        let v1_dir = &args[3];
        let v2_dir = &args[4];

        println!("netflow_dir: {:?}", netflow_dir);
        println!("v1_dir: {:?}", v1_dir);
        println!("v2_dir: {:?}", v2_dir);

        let layered_edge_list = [
            ExternalEdgeList::new(
                "netflow",
                netflow_dir,
                "src",
                "src_hash",
                "dst",
                "dst_hash",
                "epoch_time",
            )
            .expect("failed to load netflow"),
            ExternalEdgeList::new(
                "events_1v",
                v1_dir,
                "src",
                "src_hash",
                "dst",
                "dst_hash",
                "epoch_time",
            )
            .expect("failed to load events_v1"),
            ExternalEdgeList::new(
                "events_2v",
                v2_dir,
                "src",
                "src_hash",
                "dst",
                "dst_hash",
                "epoch_time",
            )
            .expect("failed to load events_v2"),
        ];
        let chunk_size = 8_388_608;
        let t_props_chunk_size = 20_970_100;
        let graph = TemporalGraph::from_edge_lists(
            8,
            chunk_size,
            chunk_size,
            t_props_chunk_size,
            graph_dir,
            layered_edge_list,
        )
        .expect("failed to load graph");
        graph
    };
    println!("graph loaded in {:?}", now.elapsed());
    graph
}
