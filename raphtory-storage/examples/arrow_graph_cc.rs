use raphtory::{
    arrow::{
        algorithms::connected_components,
        graph_impl::{ArrowGraph, ParquetLayerCols},
        graph_fragment::TempColGraphFragment,
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

    // let mut nodes = Vec::with_capacity(100);
    // for _ in 0..100 {
    //     let vid = VID(rng.gen_range(0..tg.num_nodes()));
    //     nodes.push(vid);
    // }

    let nodes = vec![
        VID(382535267),
        VID(184891835),
        VID(408229959),
        VID(367744340),
        VID(333413254),
        VID(234198684),
        VID(413563984),
        VID(409024288),
        VID(425947670),
        VID(394189445),
        VID(239780419),
        VID(274348067),
        VID(144929507),
        VID(63202068),
        VID(68341059),
        VID(37734306),
        VID(51674016),
        VID(136788061),
        VID(225357124),
        VID(271353738),
        VID(360625048),
        VID(240855894),
        VID(423558064),
        VID(416526356),
        VID(12513270),
        VID(404042536),
        VID(393426691),
        VID(357077321),
        VID(79601803),
        VID(127720040),
        VID(53857348),
        VID(175779419),
        VID(90829218),
        VID(228031919),
        VID(241662108),
        VID(187127917),
        VID(361500014),
        VID(261116828),
        VID(51105311),
        VID(212452592),
        VID(393221534),
        VID(140503104),
        VID(339275837),
        VID(155445705),
        VID(33551762),
        VID(271868807),
        VID(103589362),
        VID(371164628),
        VID(291515294),
        VID(191132985),
        VID(333857301),
        VID(49276625),
        VID(418030710),
        VID(258162159),
        VID(241396378),
        VID(419981347),
        VID(41689083),
        VID(37716799),
        VID(260785044),
        VID(70906909),
        VID(385949844),
        VID(397485291),
        VID(205860692),
        VID(353952678),
        VID(91204869),
        VID(68013329),
        VID(267438974),
        VID(296877714),
        VID(309308620),
        VID(202905000),
        VID(57402493),
        VID(224032650),
        VID(341206031),
        VID(269091645),
        VID(296400641),
        VID(26051978),
        VID(273435679),
        VID(126931241),
        VID(237458885),
        VID(77853020),
        VID(363060289),
        VID(295783863),
        VID(312442913),
        VID(299583716),
        VID(5800482),
        VID(225740520),
        VID(91430406),
        VID(17638528),
        VID(208388369),
        VID(409056166),
        VID(390692383),
        VID(260642635),
        VID(142406291),
        VID(37823426),
        VID(437161686),
        VID(404903204),
        VID(159765663),
        VID(65571225),
        VID(432470534),
        VID(44285565),
    ];

    println!("Nodes: {:?}", nodes);
    // let (sender, receiver) = std::sync::mpsc::channel();
    let (sender, receiver) = kanal::unbounded();

    let query: Query<ForwardState> = Query::new()
        .out_filter_limit(100, Arc::new(forward_time_filter))
        .out_filter_limit(100, Arc::new(forward_time_filter))
        .out_filter_limit(100, Arc::new(forward_time_filter))
        .out_filter_limit(100, Arc::new(forward_time_filter))
        // .out_filter_limit(100, Arc::new(forward_time_filter))
        .kanal(sender);

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
        |node| {
            let earliest = node.earliest();
            ForwardState::at_time(node, earliest, 100)
        },
    );
    println!("########## Arrow Hop took {:?} ##########", now.elapsed());
}
