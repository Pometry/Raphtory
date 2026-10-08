//! BEAR-B (DBpedia Live versions, <https://aic.ai.wu.ac.at/qadlod/bear.html>) as a temporal
//! benchmark: ingesting every version of the day, hour and instant archives into a
//! `PersistentGraph` (version `k` at time `k`), and BEAR's versioned queries (Mat, Diff and Ver)
//! through SPARQL and natively, from the edges and their history.
//!
//! `make bench-temporal` (in `rdf-bench/`) downloads the data and runs this with
//! `RAPHTORY_RDF_DATA` set; without the data the benchmark prints a message and does nothing.
//! `raphtory-rdf-tests/tests/bear.rs` checks that every form measured here gives the same results.
//!
//! What is measured, per granularity:
//!
//! - `ingest/<g>`: loading every version (`bear::load`) into a new graph, as triples per second;
//! - `mat_<kind>/<g>/<form>@<k>`: every query of a kind at the middle and last version, through
//!   SPARQL on `pg.snapshot_at(k)` (`sparql_snapshot`), on `GRAPH <raphtory:asof:k>`
//!   (`sparql_asof`) and natively from the layer's edges (`native`, lookups only);
//! - `diff_<kind>/<g>/<form>@1-<k>`: Diff from version 1 to `k`, through the SPARQL template and
//!   natively from edge history;
//! - `ver_<kind>/<g>/<form>`: Ver through SPARQL over 5 sampled versions (`VALUES ?g`) and
//!   natively, every validity run from edge history;
//! - `export/<g>`: `to_rdf` of the middle version, against reading the subject, layer and object
//!   names of the same `(edge, layer)` pairs (`native_names`).
//!
//! Version 1 is rebuilt without its static core (see the `bear` module), so the versions are
//! not BEAR-B's own and the numbers are not directly comparable with published BEAR-B results.
#[path = "../../raphtory-rdf-tests/tests/common/bear.rs"]
mod bear;

use bear::{bear_b_dir, read_queries, BearQuery, Changes, Granularity, Kind};
use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BatchSize, BenchmarkGroup, Criterion,
    SamplingMode, Throughput,
};
use raphtory::{
    prelude::*,
    rdf::{RdfFormat, RdfViewOps},
};
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
fn run_all<G: RdfViewOps>(view: &G, queries: &[String]) -> usize {
    queries
        .iter()
        .map(|q| match view.sparql(q).expect("BEAR-B query") {
            raphtory::rdf::SparqlResults::Solutions { rows, .. } => rows.len(),
            _ => 0,
        })
        .sum()
}

fn bench_granularity(c: &mut Criterion, granularity: Granularity) {
    let dir = match bear_b_dir() {
        Ok(dir) => dir,
        Err(why) => {
            println!("skipping the BEAR-B benchmark: {why}");
            return;
        }
    };
    let g = granularity.name();
    let archive = granularity.archive(&dir);
    if !archive.is_file() {
        println!("skipping BEAR-B {g}: {} is missing", archive.display());
        return;
    }
    let changes = Changes::read(&archive).expect("BEAR-B archive");
    let queries = read_queries(&dir).expect("BEAR-B queries");
    let last = changes.versions() as i64;

    // ingestion
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
            PersistentGraph::new,
            |pg| {
                bear::load(&pg, &changes).unwrap();
                pg
            },
            BatchSize::PerIteration,
        )
    });
    group.finish();

    let pg = PersistentGraph::new();
    bear::load(&pg, &changes).unwrap();
    let of =
        |kind: Kind| -> Vec<&BearQuery> { queries.iter().filter(|q| q.kind == kind).collect() };
    let mid = (last + 1) / 2;
    let size = |k: i64| {
        pg.snapshot_at(k)
            .valid()
            .edges()
            .explode_layers()
            .iter()
            .count()
    };
    println!(
        "BEAR-B {g}: {} triples in version 1, {} in version {mid}, {} in version {last}",
        size(1),
        size(mid),
        size(last)
    );

    // Mat and Diff at sampled versions
    for kind in [Kind::P, Kind::Po, Kind::Join] {
        let qs = of(kind);
        let k_name = kind.name();
        let mut group = small_group(c, &format!("mat_{k_name}"));
        group.throughput(Throughput::Elements(qs.len() as u64));
        for k in [mid, last] {
            let mat: Vec<String> = qs.iter().map(|q| q.mat()).collect();
            let asof: Vec<String> = qs.iter().map(|q| q.mat_asof(k)).collect();
            group.bench_function(format!("{g}/sparql_snapshot@{k}"), |b| {
                b.iter(|| run_all(&pg.snapshot_at(k), &mat))
            });
            group.bench_function(format!("{g}/sparql_asof@{k}"), |b| {
                b.iter(|| run_all(&pg, &asof))
            });
            if kind != Kind::Join {
                group.bench_function(format!("{g}/native@{k}"), |b| {
                    b.iter(|| {
                        qs.iter()
                            .map(|q| bear::native_mat(&pg, q.lookup.as_ref().unwrap(), k).len())
                            .sum::<usize>()
                    })
                });
            }
        }
        group.finish();

        let mut group = small_group(c, &format!("diff_{k_name}"));
        group.throughput(Throughput::Elements(qs.len() as u64));
        for k in [mid, last] {
            let diff: Vec<String> = qs.iter().map(|q| q.diff(1, k)).collect();
            group.bench_function(format!("{g}/sparql@1-{k}"), |b| {
                b.iter(|| run_all(&pg, &diff))
            });
            if kind != Kind::Join {
                group.bench_function(format!("{g}/native@1-{k}"), |b| {
                    b.iter(|| {
                        qs.iter()
                            .map(|q| {
                                let (added, deleted) =
                                    bear::native_diff(&pg, q.lookup.as_ref().unwrap(), 1, k);
                                added.len() + deleted.len()
                            })
                            .sum::<usize>()
                    })
                });
            }
        }
        group.finish();

        // Ver: SPARQL over 5 sampled versions; natively every run of every version
        let versions: Vec<i64> = (0..5).map(|i| 1 + i * (last - 1) / 4).collect();
        let ver: Vec<String> = qs.iter().map(|q| q.ver_values(&versions)).collect();
        let mut group = small_group(c, &format!("ver_{k_name}"));
        group.throughput(Throughput::Elements(qs.len() as u64));
        group.bench_function(format!("{g}/sparql_values_5_versions"), |b| {
            b.iter(|| run_all(&pg, &ver))
        });
        if kind != Kind::Join {
            group.bench_function(format!("{g}/native_all_versions"), |b| {
                b.iter(|| {
                    qs.iter()
                        .map(|q| bear::native_ver(&pg, q.lookup.as_ref().unwrap()).len())
                        .sum::<usize>()
                })
            });
        }
        group.finish();
    }

    // the RDF scan against reading the same names from the edges, without making terms
    let native_names = || {
        pg.snapshot_at(mid)
            .valid()
            .edges()
            .explode_layers()
            .iter()
            .map(|e| {
                e.src().name().len()
                    + e.layer_name().expect("one layer").len()
                    + e.dst().name().len()
            })
            .sum::<usize>()
    };
    let export = || {
        let mut doc = Vec::new();
        pg.snapshot_at(mid)
            .to_rdf(&mut doc, RdfFormat::NTriples)
            .unwrap()
            .triples
    };
    let mut group = small_group(c, "export");
    group.bench_function(format!("{g}/to_rdf@{mid}"), |b| b.iter(export));
    group.bench_function(format!("{g}/native_names@{mid}"), |b| b.iter(native_names));
    group.finish();
}

fn bear_b(c: &mut Criterion) {
    for granularity in Granularity::ALL {
        bench_granularity(c, granularity);
    }
}

criterion_group!(benches, bear_b);
criterion_main!(benches);
