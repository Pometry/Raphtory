// #![allow(unused_imports)]
#![allow(dead_code)]
use itertools::Itertools;
use raphtory::{
    algorithms::{
        components::weakly_connected_components,
        motifs::{
            global_temporal_three_node_motifs::global_temporal_three_node_motif,
            triangle_count::triangle_count,
        },
    },
    io::csv_loader::CsvLoader,
    logging::global_info_logger,
    prelude::*,
};
use regex::Regex;
use serde::Deserialize;
use std::{
    env,
    error::Error,
    fmt::{Debug, Display, Formatter},
    path::Path,
    time::Instant,
};
use tracing::{error, info};

#[derive(Deserialize, Debug)]
pub struct Edge {
    _unknown0: i64,
    _unknown1: i64,
    _unknown2: i64,
    src: u64,
    dst: u64,
    time: i64,
    _unknown3: u64,
    amount_usd: u64,
}

#[derive(Debug)]
pub struct MissingArgumentError;

impl Display for MissingArgumentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Failed to provide the path to the hulongbay data directory"
        )
    }
}

impl Error for MissingArgumentError {}

#[derive(Debug)]
pub struct GraphEmptyError;

impl Display for GraphEmptyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "The graph was empty but data was expected.")
    }
}

impl Error for GraphEmptyError {}

pub fn loader(data_dir: &Path) -> Result<Graph, Box<dyn Error>> {
    let encoded_data_dir = data_dir.join("graphdb.bincode");
    if encoded_data_dir.exists() {
        let now = Instant::now();
        let g = Graph::decode(encoded_data_dir.as_path())?;

        info!(
            "Loaded graph from path {} with {} nodes, {} edges, took {} seconds",
            encoded_data_dir.display(),
            g.count_nodes(),
            g.count_edges(),
            now.elapsed().as_secs()
        );

        Ok(g)
    } else {
        let g = Graph::new();

        let now = Instant::now();

        CsvLoader::new(data_dir)
            .with_filter(Regex::new(r".+(\.csv)$")?)
            .load_into_graph(&g, |sent: Edge, g: &Graph| {
                let src = sent.src;
                let dst = sent.dst;
                let time = sent.time;

                g.add_edge(
                    time,
                    src,
                    dst,
                    [("amount".to_owned(), Prop::U64(sent.amount_usd))],
                    None,
                )
                .unwrap();
            })?;

        info!(
            "Loaded graph from CSV data files {} with {} nodes, {} edges which took {} seconds",
            encoded_data_dir.display(),
            g.count_nodes(),
            g.count_edges(),
            now.elapsed().as_secs()
        );

        g.encode(encoded_data_dir)?;
        Ok(g)
    }
}

fn try_main() -> Result<(), Box<dyn Error>> {
    global_info_logger();
    let args: Vec<String> = env::args().collect();
    let data_dir = Path::new(args.get(1).ok_or(MissingArgumentError)?);

    let graph = loader(data_dir)?;
    let now = Instant::now();
    let actual_tri_count = triangle_count(&graph, None);

    info!("Actual triangle count: {:?}", actual_tri_count);

    info!(
        "Counting triangles took {} seconds",
        now.elapsed().as_secs()
    );

    let now = Instant::now();
    let components = weakly_connected_components(&graph);

    components
        .into_iter()
        .counts_by(|(_, cc)| cc)
        .iter()
        .sorted_by(|l, r| l.1.cmp(r.1))
        .rev()
        .take(50)
        .for_each(|(cc, count)| {
            info!("CC {} has {} nodes", cc, count);
        });

    info!(
        "Connected Components took {} seconds",
        now.elapsed().as_secs()
    );

    let now = Instant::now();
    let num_edges: usize = graph.nodes().out_degree().sum();
    info!(
        "Counting edges by summing degrees returned {} in {} seconds",
        num_edges,
        now.elapsed().as_secs()
    );
    let earliest_time = graph.start().ok_or(GraphEmptyError)?;
    let latest_time = graph.end().ok_or(GraphEmptyError)?;
    info!("graph time range: {}-{}", earliest_time, latest_time);
    let now = Instant::now();
    let window = graph.window(i64::MIN, i64::MAX);
    info!("Creating window took {} seconds", now.elapsed().as_secs());

    let now = Instant::now();
    let num_windowed_edges: usize = window.nodes().out_degree().sum();
    info!(
        "Counting edges in window by summing degrees returned {} in {} seconds",
        num_windowed_edges,
        now.elapsed().as_secs()
    );

    let now = Instant::now();
    let num_windowed_edges2 = window.count_edges();
    info!(
        "Window num_edges returned {} in {} seconds",
        num_windowed_edges2,
        now.elapsed().as_secs()
    );

    Ok(())
}

fn try_main_bm() -> Result<(), Box<dyn Error>> {
    global_info_logger();
    let args: Vec<String> = env::args().collect();
    let data_dir = Path::new(args.get(1).ok_or(MissingArgumentError)?);

    let graph = loader(data_dir)?;

    let now = Instant::now();
    let num_edges: usize = graph.nodes().iter().map(|v| v.out_degree()).sum();
    info!(
        "Counting edges by summing degrees returned {} in {} milliseconds",
        num_edges,
        now.elapsed().as_millis()
    );
    let earliest_time = graph.start().ok_or(GraphEmptyError)?;
    let latest_time = graph.end().ok_or(GraphEmptyError)?;
    info!("graph time range: {}-{}", earliest_time, latest_time);

    let now = Instant::now();
    let num_edges2 = graph.count_edges();
    info!(
        "num_edges returned {} in {} milliseconds",
        num_edges2,
        now.elapsed().as_millis()
    );

    let now = Instant::now();
    let num_exploded_edges = graph.edges().explode().iter().count();
    info!(
        "counted {} exploded edges in {} milliseconds",
        num_exploded_edges,
        now.elapsed().as_millis()
    );

    Ok(())
}

fn try_motif() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let data_dir = Path::new(args.get(1).ok_or(MissingArgumentError)?);
    let graph = loader(data_dir)?;
    global_temporal_three_node_motif(&graph, 3600, None);
    Ok(())
}

fn main() {
    global_info_logger();
    if let Err(e) = try_motif() {
        error!("Failed: {}", e);
        std::process::exit(1)
    }
}
