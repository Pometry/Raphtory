use raphtory::{
    algorithms::{
        centrality::pagerank::unweighted_page_rank, components::weakly_connected_components,
    },
    db::{api::mutation::AdditionOps, graph::graph::Graph},
    io::csv_loader::CsvLoader,
    logging::global_info_logger,
    prelude::*,
};
use serde::Deserialize;
use std::{env, path::Path, time::Instant};
use tracing::info;

#[derive(Deserialize, std::fmt::Debug)]
struct Edge {
    src: u64,
    dst: u64,
}

fn main() {
    let now = Instant::now();
    global_info_logger();
    let args: Vec<String> = env::args().collect();
    let data_dir = Path::new(args.get(1).expect("No data directory provided"));

    let g = if std::path::Path::new("/tmp/pokec").exists() {
        Graph::decode("/tmp/pokec").unwrap()
    } else {
        let g = Graph::new();
        CsvLoader::new(data_dir)
            .set_delimiter("\t")
            .set_header(false)
            .load_into_graph(&g, |e: Edge, g| {
                g.add_edge(0, e.src, e.dst, NO_PROPS, None)
                    .expect("Failed to add edge");
            })
            .expect("Failed to load graph from encoded data files");

        g.encode("/tmp/pokec")
            .expect("Failed to save graph to file");
        g
    };

    info!(
        "Loaded graph from encoded data files {} with {} nodes, {} edges which took {} seconds",
        data_dir.to_str().unwrap(),
        g.count_nodes(),
        g.count_edges(),
        now.elapsed().as_secs()
    );

    let now = Instant::now();

    unweighted_page_rank(&g, Some(100), None, Some(0.00000001), true, None);

    info!("PageRank took {} millis", now.elapsed().as_millis());

    let now = Instant::now();

    weakly_connected_components(&g);

    info!(
        "Connected Components took {} millis",
        now.elapsed().as_millis()
    );
}
