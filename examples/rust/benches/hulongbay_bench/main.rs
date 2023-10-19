use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use raphtory::{
    algorithms::metrics::clustering_coefficient::clustering_coefficient,
    graph_loader::source::csv_loader::CsvLoader, prelude::*,
};
use regex::Regex;
use serde::Deserialize;
use std::{error::Error, fmt::Debug, path::Path, time::Instant};

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

pub fn loader(data_dir: &Path) -> Result<Graph, Box<dyn Error>> {
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

    println!(
        "Loaded graph from CSV data files with {} vertices, {} edges which took {} seconds",
        g.count_vertices(),
        g.count_edges(),
        now.elapsed().as_secs()
    );
    Ok(g)
}

pub fn criterion_benchmark_cc(c: &mut Criterion) {
    let mut group = c.benchmark_group("hulong_large_clustering_coefficient");
    let data_dir = Path::new("/Users/haaroony/Documents/dev/Data/hulong/sample/hulong_sorted.csv");
    let g: Graph = loader(data_dir).unwrap();
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(100);
    group.bench_with_input(
        BenchmarkId::new("large_graph_clustering_coefficient", &g),
        &g,
        |b, g| {
            b.iter(|| {
                let result = clustering_coefficient(g);
                black_box(result);
            });
        },
    );
    group.finish()
}

criterion_group!(benches, criterion_benchmark_cc);
criterion_main!(benches);
