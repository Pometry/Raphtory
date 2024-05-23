use raphtory::{
    arrow::{
        graph_impl::{ArrowGraph, ParquetLayerCols},
        query::{ast::Query, executors::rayon2, ForwardState},
    },
    core::entities::VID,
    db::api::view::GraphViewOps,
};
use raphtory_arrow::{algorithms::connected_components, graph_fragment::TempColGraphFragment};
use std::{io::Write, time::Instant};

fn main() {
    // Retrieve command line arguments
    let args = || std::env::args();

    let graph_dir = args().nth(1).expect("Graph directory not provided");

    let graph2 = if let Ok(_) = std::fs::metadata(&graph_dir) {
        ArrowGraph::load_from_dir(graph_dir).expect("Cannot open graph")
    } else {
        let parquet_dir = &args().nth(2).expect("Parquet directory not provided");

        let chunk_size = args()
            .nth(3)
            .and_then(|x| x.parse::<usize>().ok())
            .unwrap_or(268_435_456);
        let num_threads = args()
            .nth(4)
            .and_then(|x| x.parse::<usize>().ok())
            .unwrap_or(8);
        let t_props_chunk_size = args()
            .nth(5)
            .and_then(|x| x.parse::<usize>().ok())
            .unwrap_or(chunk_size / 16);

        let concurrent_files = args()
            .nth(6)
            .and_then(|x| x.parse::<usize>().ok())
            .unwrap_or(1);

        let read_chunk_size = args()
            .nth(7)
            .and_then(|x| x.parse::<usize>().ok())
            .unwrap_or(4_000_000);

        println!(
            "Loading graph from parquet: {} with chunk size: {chunk_size} t_prop chunk size {t_props_chunk_size}, concurrent_files: {concurrent_files} and threads: {num_threads}",
            parquet_dir
        );

        let now = Instant::now();
        let graph = ArrowGraph::load_from_parquets(
            graph_dir,
            vec![ParquetLayerCols {
                parquet_dir,
                layer: "default",
                src_col: "src",
                dst_col: "dst",
                time_col: "time",
            }],
            None,
            chunk_size,
            t_props_chunk_size,
            Some(read_chunk_size),
            Some(concurrent_files),
            num_threads,
        )
        .expect("Cannot load graph");
        println!("########## Load took {:?} ########## ", now.elapsed());
        graph
    };

    connected_components(&graph2);
    hop_query(&graph2);
}

fn connected_components(tg: &ArrowGraph) {
    println!("Graph has {} nodes", tg.count_nodes());
    println!("Graph has {} edges", tg.count_edges());

    let now = Instant::now();
    // let ccs = weakly_connected_components(&graph2, 100, None).group_by();
    let out = connected_components::connected_components(tg.as_ref());
    println!(
        "########## Arrow CC took {:?} ########## len: {}",
        now.elapsed(),
        out.len()
    );
}

fn hop_query(tg: &ArrowGraph) {
    let now = Instant::now();

    let nodes = vec![
        VID(309582099),
        VID(54328484),
        VID(246173700),
        VID(16700593),
        VID(177352288),
        VID(180812214),
        VID(52391093),
        VID(399368698),
        VID(263103204),
        VID(379960042),
        VID(416420825),
        VID(199488353),
        VID(224963182),
        VID(51977241),
        VID(856781),
        VID(444466102),
        VID(418741608),
        VID(192869236),
        VID(299536904),
        VID(85715682),
        VID(132369141),
        VID(202535826),
        VID(333437339),
        VID(263640094),
        VID(33964780),
        VID(379081115),
        VID(290623079),
        VID(395279946),
        VID(133035021),
        VID(249927504),
        VID(261634684),
        VID(430970739),
        VID(253060757),
        VID(272814697),
        VID(158376132),
        VID(86272904),
        VID(326943324),
        VID(82327004),
        VID(261701485),
        VID(109463839),
        VID(117863968),
        VID(163145864),
        VID(330916934),
        VID(211355612),
        VID(281370847),
        VID(371456910),
        VID(299845460),
        VID(344814299),
        VID(90076774),
        VID(277046483),
        VID(202223853),
        VID(315635830),
        VID(404087723),
        VID(217660841),
        VID(262444201),
        VID(38909930),
        VID(299362410),
        VID(436843462),
        VID(228264831),
        VID(146444304),
        VID(89715034),
        VID(109094148),
        VID(71703352),
        VID(253889004),
        VID(264785705),
        VID(36547407),
        VID(158966904),
        VID(319912238),
        VID(20208726),
        VID(156259436),
        VID(1721625),
        VID(205725206),
        VID(442549275),
        VID(410095341),
        VID(339347119),
        VID(318108647),
        VID(235328888),
        VID(398864679),
        VID(18989798),
        VID(257431550),
        VID(299924240),
        VID(296379526),
        VID(9157244),
        VID(89996738),
        VID(170704515),
        VID(134164014),
        VID(219867467),
        VID(244239784),
        VID(258341225),
        VID(274228163),
        VID(342058645),
        VID(66680949),
        VID(150576676),
        VID(248701795),
        VID(221102041),
        VID(325407184),
        VID(45609419),
        VID(69308556),
        VID(130864213),
        VID(205326867),
    ];

    let query: Query<ForwardState> = Query::new()
        .out_limit("default", 100)
        // .out_limit("default", 100)
        // .out_limit("default", 100)
        // .out_limit("default", 100)
        // .out_limit("default", 100)
        .path("hop", |mut writer, state: ForwardState| {
            serde_json::to_writer(&mut writer, &state.path).unwrap();
            write!(writer, "\n").unwrap();
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
