use std::{time::Instant, env, path::Path};

use raphtory::{
    algorithms::pagerank::unweighted_page_rank,
    db::{graph::Graph, view_api::GraphViewOps},
};
use raphtory_io::graph_loader::source::csv_loader::CsvLoader;
use serde::Deserialize;
use raphtory::algorithms::connected_components::weakly_connected_components;

#[derive(Deserialize, std::fmt::Debug)]
struct Edge {
    src: u64,
    dst: u64,
}

fn main() {
    let shards = 2;

    let now = Instant::now();

    let args: Vec<String> = env::args().collect();
    //let data_dir = Path::new(args.get(1).expect("No data directory provided"));
    let data_dir = Path::new("/tmp/soc-pokec-relationships.txt");

    let g = if std::path::Path::new("/tmp/pokec").exists() {
        Graph::load_from_file("/tmp/pokec").unwrap()
    }
    else{
        let g = Graph::new(shards);
        CsvLoader::new(data_dir)
            .set_delimiter("\t")
            .set_header(false)
            .load_into_graph(&g, |e: Edge, g| {
                g.add_edge(0, e.src, e.dst, &vec![], None)
                    .expect("Failed to add edge");
            })
            .expect("Failed to load graph from encoded data files");

        g.save_to_file("/tmp/pokec");
        g
    };


    println!(
        "Loaded graph from encoded data files {} with {} vertices, {} edges which took {} seconds",
        "/tmp/soc-pokec-relationships.txt",
        g.num_vertices(),
        g.num_edges(),
        now.elapsed().as_secs()
    );

    let frozen = g.freeze();

    let now = Instant::now();

    unweighted_page_rank(&frozen, 100, None , Some(0.00000001), true);

    println!("PageRank took {} millis", now.elapsed().as_millis());

    let now = Instant::now();

    weakly_connected_components(&frozen, 100, None);

    println!("Connected Components took {} millis", now.elapsed().as_millis());
}
