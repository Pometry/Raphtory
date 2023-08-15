use rand::seq::SliceRandom;
use raphtory::{
    algorithms::{
        connected_components::weakly_connected_components, pagerank::unweighted_page_rank,
    },
    db::{
        api::{
            mutation::AdditionOps,
            view::{internal::TimeSemantics, GraphViewOps},
        },
        graph::graph::Graph,
    },
    graph_loader::{
        example::sx_superuser_graph::sx_superuser_graph, source::csv_loader::CsvLoader,
    },
    prelude::{Layer, TimeOps, NO_PROPS},
};
use serde::Deserialize;
use std::{env, path::Path, time::Instant};

#[derive(Deserialize, std::fmt::Debug)]
struct Edge {
    src: u64,
    dst: u64,
}

fn main() {
    let now = Instant::now();

    let args: Vec<String> = env::args().collect();
    let data_dir = Path::new(args.get(1).expect("No data directory provided"));

    let g = if std::path::Path::new("/tmp/pokec").exists() {
        Graph::load_from_file("/tmp/pokec").unwrap()
    } else {
        let g = sx_superuser_graph().unwrap();
        // let g = Graph::new();
        // CsvLoader::new(data_dir)
        //     .set_delimiter("\t")
        //     .set_header(false)
        //     .load_into_graph(&g, |e: Edge, g| {
        //         g.add_edge(0, e.src, e.dst, NO_PROPS, None)
        //             .expect("Failed to add edge");
        //     })
        //     .expect("Failed to load graph from encoded data files");

        g.save_to_file("/tmp/pokec")
            .expect("Failed to save graph to file");
        g
    };

    println!(
        "Loaded graph from encoded data files {} with {} vertices, {} edges which took {} seconds, t_start {} t_end {}",
        data_dir.to_str().unwrap(),
        g.num_vertices(),
        g.num_edges(),
        now.elapsed().as_secs(),
        g.earliest_time().unwrap(),
        g.latest_time().unwrap()
    );

    // let now = Instant::now();

    // unweighted_page_rank(&g, 100, None, Some(0.00000001), true);

    // println!("PageRank took {} millis", now.elapsed().as_millis());

    // let now = Instant::now();

    // weakly_connected_components(&g, 100, None);

    // println!(
    //     "Connected Components took {} millis",
    //     now.elapsed().as_millis()
    // );

    // create a window graph of 50% of the graph
    // sample 25% of the edges of the graph and shuffle them
    // in 100 loops keep calling has_edge on the window graph

    let t_start = g.earliest_time().unwrap();
    let t_end = g.latest_time().unwrap();
    let t_mid = t_start + (t_end - t_start) / 2;

    let wg = g.window(t_mid, t_end);

    let mut edge_pairs = Vec::with_capacity(wg.num_edges());

    for e in wg.edges() {
        edge_pairs.push((e.edge.src(), e.edge.dst()));
    }

    let mut edge_ids = (0..edge_pairs.len()).into_iter().collect::<Vec<_>>();

    let now = Instant::now();

    let sample = 4;

    let handle = std::thread::spawn(move || {
        let mut check_edges = 0;
        for _ in 0..200 {
            edge_ids.shuffle(&mut rand::thread_rng());
            for id in edge_ids.iter() {
                if *id % sample <= 1 {
                    continue;
                }
                let (src, dst) = &edge_pairs[*id];
                wg.has_edge(*src, *dst, Layer::Default);
                check_edges += 1;
            }
        }
        check_edges
    });
    let check_edges = handle.join().unwrap();

    println!(
        "Has Edge took {} millis checked: {check_edges}",
        now.elapsed().as_millis()
    );
}
