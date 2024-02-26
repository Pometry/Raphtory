use raphtory::{
    arrow::{
        algorithms::connected_components,
        graph_impl::{ArrowGraph, ParquetLayerCols},
    },
    query::{
        ast::Query, executors::rayon2, forward_time_filter, state::HopState, ForwardState,
    },
    core::entities::VID,
    prelude::*,
};
use std::{
    io::{BufWriter, Write},
    sync::Arc,
    thread,
    time::Instant,
};

use rand::Rng;

fn main() {
    // Retrieve command line arguments
    let args = || std::env::args();

    let graph_dir = args().nth(1).expect("Graph directory not provided");

    let graph2 = if let Ok(_) = std::fs::metadata(&graph_dir) {
        ArrowGraph::load_from_dir(graph_dir).expect("Cannot open graph")
    } else {
        let parquet_dir = &args().nth(2).expect("Parquet directory not provided");

        let chunk_size = 268_435_456;
        let num_threads = 4;
        let t_props_chunk_size = chunk_size / 8;
        let now = Instant::now();
        let graph = ArrowGraph::load_from_parquets(
            graph_dir,
            vec![ParquetLayerCols {
                parquet_dir,
                layer: "default",
                src_col: "src",
                src_hash_col: "src_hash",
                dst_col: "dst",
                dst_hash_col: "dst_hash",
                time_col: "time",
            }],
            chunk_size,
            t_props_chunk_size,
            Some(4_000_000),
            Some(1),
            num_threads,
        )
        .expect("Cannot load graph");
        println!("########## Load took {:?} ########## ", now.elapsed());
        graph
    };
    // } else {
    //     panic!("Graph directory does not exist")
    // };

    let g = &graph2.layer(0);

    // connected_components(g);
    hop_query(g);
}

fn connected_components(tg: &TempColGraphFragment) {
    println!("Graph has {} nodes", tg.num_nodes());
    println!("Graph has {} edges", tg.num_edges());

    let now = Instant::now();
    // let ccs = weakly_connected_components(&graph2, 100, None).group_by();
    let out = connected_components::connected_components(tg);
    println!(
        "########## Arrow CC took {:?} ########## len: {}",
        now.elapsed(),
        out.len()
    );
}

fn hop_query(tg: &TempColGraphFragment) {
    let now = Instant::now();
    // let query = Query::new().out_filter(filter)
    // pick 100 nodes at random between 0 and num_nodes
    let mut rng = rand::thread_rng();

    let mut nodes = Vec::with_capacity(100);
    for _ in 0..100 {
        let vid = VID(rng.gen_range(0..tg.num_nodes()));
        nodes.push(vid);
    }
    let (sender, receiver) = std::sync::mpsc::channel();

    let query: Query<ForwardState> = Query::new()
        .out_filter(Arc::new(forward_time_filter))
        .out_filter(Arc::new(forward_time_filter))
        .out_filter(Arc::new(forward_time_filter))
        .channel(sender);

    thread::spawn(move || {
        let file = std::fs::File::create("hop.bin").expect("Cannot create file");
        let mut writer = BufWriter::new(file);
        while let Ok((state, _)) = receiver.recv() {
            let path = state.path.iter().map(|VID(n)| *n).collect::<Vec<_>>();

            let buf = format!("{:?}\n", path);
            writer.write(buf.as_bytes()).expect("Cannot write to file");
        }
    });

    let _ = rayon2::execute::<ForwardState>(
        query,
        raphtory::arrow::query::NodeSource::NodeIds(nodes),
        tg,
        |node| ForwardState::new(node),
    );
    println!("########## Arrow Hop took {:?} ##########", now.elapsed());
}
