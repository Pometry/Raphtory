//! The Gene Ontology (GO, <https://geneontology.org/docs/download-ontology/>): the current release
//! on Raphtory and oxigraph's in-memory `Store`, and every release as versions of one Raphtory
//! graph against a `Store` with a named graph per release (see the `harness` module). On Raphtory,
//! time graphs are named with `FROM`, `FROM NAMED` or a constant `GRAPH`.
//!
//! Printed: the loads (time and heap growth), and the time per query of `go_current`,
//! `go_versions` and `go_asof_<date>` as of the middle release ([`go::queries`],
//! [`go::versioned_queries`], [`go::queries_asof`]).
//!
//! Run with `make bench-go` in `rdf-bench/`, which checks the releases against `go.sha256`;
//! without the data it prints a message. A criterion filter (`-- <regex>` or `-- --exact <id>`)
//! on `<group>/<query>/<engine>` skips the other cases.
#[path = "../../raphtory-rdf-tests/tests/common/data.rs"]
mod data;
#[path = "common/go.rs"]
mod go;
#[path = "common/harness.rs"]
mod harness;

use criterion::Criterion;
use data::ntriples;
use go::{
    go_dir, load_raphtory, load_store_release, queries, queries_asof, release_dates,
    versioned_queries, GoQuery, Versions, SUMS,
};
use harness::{bench_case, note, print_table, small_group, Engines, Query, SLOW};
use oxigraph::store::Store;
use raphtory::{db::api::view::IntoDynamic, prelude::*, rdf::RdfFormat};
use std::time::{Duration, Instant};

#[global_allocator]
static ALLOCATOR: heap::Counting = heap::Counting;

/// Counts the heap bytes allocated and freed while counting is on.
mod heap {
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        sync::atomic::{AtomicBool, AtomicIsize, Ordering::Relaxed},
    };

    pub struct Counting;

    static ON: AtomicBool = AtomicBool::new(false);
    static NOW: AtomicIsize = AtomicIsize::new(0);
    static PEAK: AtomicIsize = AtomicIsize::new(0);

    fn add(bytes: isize) {
        if ON.load(Relaxed) {
            let now = NOW.fetch_add(bytes, Relaxed) + bytes;
            PEAK.fetch_max(now, Relaxed);
        }
    }

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let p = unsafe { System.alloc(layout) };
            if !p.is_null() {
                add(layout.size() as isize);
            }
            p
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let p = unsafe { System.alloc_zeroed(layout) };
            if !p.is_null() {
                add(layout.size() as isize);
            }
            p
        }

        unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
            unsafe { System.dealloc(p, layout) };
            add(-(layout.size() as isize));
        }

        unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let q = unsafe { System.realloc(p, layout, size) };
            if !q.is_null() {
                add(size as isize - layout.size() as isize);
            }
            q
        }
    }

    /// Heap growth: its peak and what is left.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct Growth {
        pub peak: isize,
        pub kept: isize,
    }

    pub fn start() {
        NOW.store(0, Relaxed);
        PEAK.store(0, Relaxed);
        ON.store(true, Relaxed);
    }

    pub fn pause() {
        ON.store(false, Relaxed);
    }

    pub fn resume() {
        ON.store(true, Relaxed);
    }

    pub fn stop() -> Growth {
        ON.store(false, Relaxed);
        Growth {
            peak: PEAK.load(Relaxed),
            kept: NOW.load(Relaxed),
        }
    }
}

/// Runs `f`, timing it and counting the heap.
fn measure<T>(f: impl FnOnce() -> T) -> (T, Duration, heap::Growth) {
    heap::start();
    let start = Instant::now();
    let value = f();
    let time = start.elapsed();
    (value, time, heap::stop())
}

fn gb(bytes: isize) -> String {
    format!("{:.2} GB", bytes as f64 / 1e9)
}

impl Query for GoQuery {
    fn raphtory(&self) -> &str {
        &self.raphtory
    }

    fn store(&self) -> &str {
        &self.store
    }
}

fn bench_queries(
    c: &mut Criterion,
    name: &str,
    measure: Duration,
    engines: &Engines,
    queries: &[GoQuery],
) {
    let mut group = small_group(c, name, measure);
    for q in queries {
        let case = q.name.split('@').next().unwrap_or(&q.name);
        bench_case(
            &mut group,
            name,
            case,
            q.what,
            engines,
            std::slice::from_ref(q),
            SLOW,
        );
    }
    group.finish();
}

fn note_load(what: &str, time: Duration, triples: usize, growth: heap::Growth) {
    note(format!(
        "go load {what}: {:.2} s, {:.0} triples/s, heap peak {}, kept {}",
        time.as_secs_f64(),
        triples as f64 / time.as_secs_f64(),
        gb(growth.peak),
        gb(growth.kept)
    ));
}

/// The current release, skolemized: loading it and the queries.
fn current(c: &mut Criterion, versions: &Versions) {
    let date = versions.dates.last().expect("a release");
    let t = date.as_str();
    let triples = versions.last.len();

    let doc = ntriples(&versions.last);
    let (pg, time, growth) = measure(|| {
        let rg = RdfGraph::new();
        rg.load_rdf(t, doc.as_slice(), RdfFormat::NTriples, None)
            .expect("load_rdf");
        rg.graph().clone()
    });
    note_load("raphtory skolemized (N-Triples)", time, triples, growth);
    let (store, time, growth) = measure(|| {
        let store = Store::new().expect("Store");
        let mut loader = store.bulk_loader();
        loader
            .load_from_slice(RdfFormat::NTriples, doc.as_slice())
            .expect("bulk load");
        loader.commit().expect("bulk load");
        store
    });
    note_load("store skolemized (N-Triples)", time, triples, growth);
    drop(doc);
    note(format!(
        "go current release {date}: {} triples in Raphtory ({} nodes, {} layers), {} in the Store",
        pg.valid().edges().explode_layers().iter().count(),
        pg.count_nodes(),
        pg.unique_layers().count(),
        store.len().expect("Store"),
    ));
    let engines = Engines {
        raphtory: Some(RdfView::from_view(pg.valid().into_dynamic()).expect("a persistent graph")),
        store: Some(store),
    };
    bench_queries(
        c,
        "go_current",
        Duration::from_secs(1),
        &engines,
        &queries(),
    );
}

/// The groups across releases and as of the middle release.
fn versioned_groups(dates: &[String]) -> Vec<(String, Duration, Vec<GoQuery>)> {
    let date = &dates[dates.len() / 2];
    vec![
        (
            "go_versions".to_owned(),
            Duration::from_secs(1),
            versioned_queries(dates),
        ),
        (
            format!("go_asof_{date}"),
            Duration::from_millis(500),
            queries_asof(date),
        ),
    ]
}

/// Every release: the Store with a graph per release, then Raphtory's versions, each loaded and
/// queried alone.
fn versioned(c: &mut Criterion, versions: Versions) {
    let dates = versions.dates.clone();
    let groups = versioned_groups(&dates);

    let (read, diff) = (versions.read_time, versions.diff_time);
    let store = Store::new().expect("Store");
    let mut store_load = Duration::ZERO;
    heap::start();
    heap::pause();
    versions
        .for_each_release(|k, release| {
            let doc = ntriples(release.iter().copied());
            heap::resume();
            let start = Instant::now();
            load_store_release(&store, &dates[k], &doc)?;
            store_load += start.elapsed();
            heap::pause();
            Ok(())
        })
        .expect("Store load");
    let growth = heap::stop();
    let quads = store.len().expect("Store");
    note(format!(
        "go load store {} releases (a graph each): {:.2} s, {quads} quads, {:.0} quads/s, heap peak {}, kept {}",
        dates.len(),
        store_load.as_secs_f64(),
        quads as f64 / store_load.as_secs_f64(),
        gb(growth.peak),
        gb(growth.kept)
    ));
    let docs = versions.docs();
    let events = versions.events();
    drop(versions);
    let engines = Engines {
        store: Some(store),
        ..Engines::default()
    };
    for (name, measure, queries) in &groups {
        bench_queries(c, name, *measure, &engines, queries);
    }
    drop(engines);

    let (pg, raphtory_load, growth) = measure(|| {
        let rg = RdfGraph::new();
        load_raphtory(&rg, &dates, &docs).expect("load_raphtory");
        rg.graph().clone()
    });
    drop(docs);
    note(format!(
        "go load raphtory {} releases (the first, then the changes): {:.2} s, {events} triples written, {:.0} triples/s, heap peak {}, kept {}",
        dates.len(),
        raphtory_load.as_secs_f64(),
        events as f64 / raphtory_load.as_secs_f64(),
        gb(growth.peak),
        gb(growth.kept)
    ));
    note(format!(
        "go versions from the go.owl files: raphtory {:.1} s (read and skolemize {:.1} s, diff {:.1} s, load {:.1} s), store {:.1} s (read and skolemize {:.1} s, load {:.1} s)",
        (read + diff + raphtory_load).as_secs_f64(),
        read.as_secs_f64(),
        diff.as_secs_f64(),
        raphtory_load.as_secs_f64(),
        (read + store_load).as_secs_f64(),
        read.as_secs_f64(),
        store_load.as_secs_f64(),
    ));
    note(format!(
        "go versions: {} triples now in Raphtory ({} nodes, {} layers)",
        pg.valid().edges().explode_layers().iter().count(),
        pg.count_nodes(),
        pg.unique_layers().count(),
    ));
    // every query names its time graphs, so the whole graph, without `valid()`
    let engines = Engines {
        raphtory: Some(RdfView::from_view(pg.into_dynamic()).expect("a persistent graph")),
        ..Engines::default()
    };
    for (name, measure, queries) in &groups {
        bench_queries(c, name, *measure, &engines, queries);
    }
}

fn main() {
    let dir = match go_dir() {
        Ok(dir) => dir,
        Err(why) => {
            println!("skipping the GO benchmark: {why}");
            return;
        }
    };
    let mut c = harness::criterion();
    let start = Instant::now();
    let dates = release_dates(SUMS);
    let versions = Versions::read(&dir, &dates).expect("GO releases");
    note(format!(
        "go: {} releases read, skolemized and diffed in {:.1} s (read and skolemize {:.1} s, diff {:.1} s)",
        dates.len(),
        start.elapsed().as_secs_f64(),
        versions.read_time.as_secs_f64(),
        versions.diff_time.as_secs_f64()
    ));
    current(&mut c, &versions);
    versioned(&mut c, versions);
    c.final_summary();
    print_table();
}
