//! General SPARQL performance: the Berlin SPARQL Benchmark (BSBM) at 5,000 products and the
//! Sparqloscope feature queries, on Raphtory and on oxigraph's in-memory `Store` (see the `harness`
//! module).
//!
//! The data is not in the repository (see the `bsbm` module): `make bench-rdf-sparql` downloads
//! it and runs this with `RAPHTORY_RDF_DATA` set; without the data it prints a message and does
//! nothing.
//!
//! What is measured:
//!
//! - **Loading** (timed once per engine, printed): the dataset as N-Triples.
//! - `bsbm5000_explore/Q<t>/<engine>`: the first ten distinct queries of explore template `t`.
//! - `bsbm5000_sparqloscope/<i>/<engine>`: each Sparqloscope query.
//!
//! Run with `cargo bench -p raphtory-rdf-tests --features bench --bench sparql` (or `make
//! bench-rdf-sparql`).
#[path = "common/bsbm.rs"]
mod bsbm;
#[path = "../tests/common/data.rs"]
mod data;
#[path = "common/harness.rs"]
mod harness;

use bsbm::{bsbm_dir, instances, read_sparqloscope, templates, Bsbm, BASE_TIME};
use criterion::Criterion;
use harness::{bench_case, note, print_table, small_group, Engines, SLOW};
use oxigraph::{io::RdfFormat as OxRdfFormat, store::Store};
use raphtory::{db::api::view::IntoDynamic, prelude::*};
use raphtory_rdf::{RdfFormat, RdfGraph, RdfView};
use std::{
    path::Path,
    slice,
    time::{Duration, Instant},
};

const PRODUCTS: usize = 5000;

/// The slow threshold of the Sparqloscope queries, which are many.
const SLOW_SPARQLOSCOPE: Duration = Duration::from_millis(300);

/// Loads the dataset into both engines, timing each, and returns them.
fn load(bsbm: &Bsbm) -> Engines {
    let n = bsbm.products;
    let report = |engine: &str, time: Duration| {
        note(format!(
            "bsbm{n} load {engine} dataset: {:.3} s, {:.0} triples/s",
            time.as_secs_f64(),
            bsbm.dataset_triples as f64 / time.as_secs_f64()
        ));
    };

    let rg = RdfGraph::new();
    let start = Instant::now();
    rg.load_rdf(
        BASE_TIME,
        bsbm.dataset.as_slice(),
        RdfFormat::NTriples,
        None,
    )
    .expect("load_rdf");
    report("raphtory", start.elapsed());
    let pg = rg.graph().clone();

    let store = Store::new().expect("Store");
    let start = Instant::now();
    let mut loader = store.bulk_loader();
    loader
        .load_from_slice(OxRdfFormat::NTriples, bsbm.dataset.as_slice())
        .expect("bulk load");
    loader.commit().expect("bulk load");
    report("store", start.elapsed());

    note(format!(
        "bsbm{n}: {} triples in Raphtory ({} nodes, {} layers), {} in the Store",
        pg.valid().edges().explode_layers().iter().count(),
        pg.count_nodes(),
        pg.unique_layers().count(),
        store.len().expect("Store")
    ));
    Engines {
        raphtory: Some(RdfView::from_view(pg.valid().into_dynamic()).expect("a persistent graph")),
        store: Some(store),
    }
}

fn bench(c: &mut Criterion, dir: &Path) {
    let bsbm = Bsbm::read(dir, PRODUCTS).expect("BSBM files");
    let engines = load(&bsbm);

    let name = format!("bsbm{PRODUCTS}_explore");
    let mut group = small_group(c, &name, Duration::from_secs(1));
    for t in templates(&bsbm.explore) {
        let queries = instances(&bsbm.explore, t, 10);
        bench_case(
            &mut group,
            &name,
            &format!("Q{t:02}"),
            "",
            &engines,
            &queries,
            SLOW,
        );
    }
    group.finish();

    let sparqloscope = dir.join("sparqloscope-bsbm-5000.csv");
    if sparqloscope.is_file() {
        let name = format!("bsbm{PRODUCTS}_sparqloscope");
        let mut group = small_group(c, &name, Duration::from_millis(500));
        let queries = read_sparqloscope(&sparqloscope).expect("Sparqloscope queries");
        for (i, (description, query)) in queries.iter().enumerate() {
            bench_case(
                &mut group,
                &name,
                &format!("{i:03}"),
                description,
                &engines,
                slice::from_ref(query),
                SLOW_SPARQLOSCOPE,
            );
        }
        group.finish();
    }
}

fn main() {
    let dir = match bsbm_dir() {
        Ok(dir) => dir,
        Err(why) => {
            println!("skipping the BSBM benchmark: {why}");
            return;
        }
    };
    let mut c = harness::criterion();
    bench(&mut c, &dir);
    c.final_summary();
    print_table();
}
