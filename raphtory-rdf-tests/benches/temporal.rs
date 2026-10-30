//! BEAR-B (DBpedia Live versions, <https://aic.ai.wu.ac.at/qadlod/bear.html>) as a temporal
//! benchmark: ingesting every version of the instant archive into an `RdfGraph` (version `k`
//! at time `k`), and BEAR's versioned queries Mat and Ver through SPARQL.
//!
//! `make bench-rdf-temporal` downloads the data and runs this with `RAPHTORY_RDF_DATA` set;
//! without the data the benchmark prints a message and does nothing.
//! `raphtory-rdf-tests/tests/bear.rs` checks the same forms on the day archive.
//!
//! What is measured:
//!
//! - `ingest/instant`: loading every version (`bear::load`) into a new graph, as triples per
//!   second;
//! - `mat_<kind>/instant/sparql_snapshot@<k>`: every query of a kind at the middle version, through
//!   SPARQL on `rg.before(k + 1).valid()`;
//! - `ver_<kind>/instant/sparql_values_5_versions`: Ver through SPARQL over 5 sampled versions
//!   (`VALUES ?g`);
//! - `export/instant/to_rdf@<k>`: `to_rdf` of the middle version.
//!
//! Version 1 is rebuilt without its static core (see the `bear` module), so the versions are
//! not BEAR-B's own and the numbers are not directly comparable with published BEAR-B results.
#[path = "../tests/common/bear.rs"]
mod bear;
#[path = "../tests/common/data.rs"]
mod data;

use bear::{bear_b_dir, read_queries, BearQuery, Changes, Granularity, Kind};
use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BatchSize, BenchmarkGroup, Criterion,
    SamplingMode, Throughput,
};
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    prelude::*,
};
use raphtory_rdf::{RdfFormat, RdfGraph, RdfView, SparqlOptions, SparqlResults};
use std::time::Duration;

/// A group with small samples, so that a whole run takes minutes.
fn small_group<'a>(c: &'a mut Criterion, name: &str) -> BenchmarkGroup<'a, WallTime> {
    let mut group = c.benchmark_group(name);
    group
        .sample_size(10)
        .sampling_mode(SamplingMode::Flat)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3));
    group
}

/// Runs every query and returns the number of solutions, so the work is not optimised away.
fn run_all<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>, queries: &[String]) -> usize {
    queries
        .iter()
        .map(|q| {
            match view
                .sparql(q, &SparqlOptions::default())
                .expect("BEAR-B query")
            {
                SparqlResults::Solutions { rows, .. } => rows.len(),
                _ => 0,
            }
        })
        .sum()
}

fn bear_b(c: &mut Criterion) {
    let dir = match bear_b_dir() {
        Ok(dir) => dir,
        Err(why) => {
            println!("skipping the BEAR-B benchmark: {why}");
            return;
        }
    };
    let g = Granularity::Instant.name();
    let archive = Granularity::Instant.archive(&dir);
    if !archive.is_file() {
        println!("skipping BEAR-B {g}: {} is missing", archive.display());
        return;
    }
    let changes = Changes::read(&archive).expect("BEAR-B archive");
    let queries = read_queries(&dir).expect("BEAR-B queries");
    let last = changes.versions() as i64;

    println!(
        "BEAR-B {g}: {} versions, {} triples written ({} in the reconstructed version 1)",
        changes.versions(),
        changes.triples(),
        changes.base.len()
    );
    let mut group = small_group(c, "ingest");
    group
        .measurement_time(Duration::from_secs(5))
        .throughput(Throughput::Elements(changes.triples() as u64));
    group.bench_function(g, |b| {
        b.iter_batched(
            RdfGraph::new,
            |rg| {
                bear::load(&rg, &changes).unwrap();
                rg
            },
            BatchSize::PerIteration,
        )
    });
    group.finish();

    let rg = RdfGraph::new();
    bear::load(&rg, &changes).unwrap();
    let mid = (last + 1) / 2;
    let versions: Vec<i64> = (0..5).map(|i| 1 + i * (last - 1) / 4).collect();
    for kind in [Kind::P, Kind::Po] {
        let qs: Vec<&BearQuery> = queries.iter().filter(|q| q.kind == kind).collect();
        let k_name = kind.name();

        let mat: Vec<String> = qs.iter().map(|q| q.mat()).collect();
        let mut group = small_group(c, &format!("mat_{k_name}"));
        group.throughput(Throughput::Elements(qs.len() as u64));
        group.bench_function(format!("{g}/sparql_snapshot@{mid}"), |b| {
            b.iter(|| run_all(&rg.before(mid + 1).valid(), &mat))
        });
        group.finish();

        let ver: Vec<String> = qs.iter().map(|q| q.ver_values(&versions)).collect();
        let mut group = small_group(c, &format!("ver_{k_name}"));
        group.throughput(Throughput::Elements(qs.len() as u64));
        group.bench_function(format!("{g}/sparql_values_5_versions"), |b| {
            b.iter(|| run_all(&rg, &ver))
        });
        group.finish();
    }

    let export = || {
        let mut doc = Vec::new();
        rg.before(mid + 1)
            .valid()
            .to_rdf(&mut doc, RdfFormat::NTriples)
            .unwrap()
            .written
    };
    let mut group = small_group(c, "export");
    group.bench_function(format!("{g}/to_rdf@{mid}"), |b| b.iter(export));
    group.finish();
}

criterion_group!(benches, bear_b);
criterion_main!(benches);
