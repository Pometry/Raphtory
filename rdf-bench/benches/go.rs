//! The Gene Ontology (GO, <https://geneontology.org/docs/download-ontology/>): the current release
//! on three engines, and every release as versions of one Raphtory graph against a `Store` with a
//! named graph per release. All three evaluate SPARQL with spareval:
//!
//! - `raphtory`: a `PersistentGraph` through `RaphtoryDataset`; time graphs are named with `FROM`,
//!   `FROM NAMED` or a constant `GRAPH`;
//! - `store`: oxigraph's in-memory `Store`;
//! - `dataset`: an `oxrdf::Dataset` (the current release only).
//!
//! Printed: the loads (time and heap growth, best of two for the versions on both engines), and
//! the time per query of `go_current`, `go_versions` and `go_asof_<date>` ([`go::queries`],
//! [`go::versioned_queries`], [`go::queries_asof`]). A case whose first run exceeds [`SLOW`] is
//! timed by the best of a few single runs (marked `*`); criterion measures the others.
//!
//! Run with `make bench-go` in `rdf-bench/`; without the data it prints a message. A criterion
//! filter (`-- <regex>` or `-- --exact <id>`) on `<group>/<query>/<engine>` skips the other cases.
#[path = "../../raphtory-rdf-tests/tests/common/go.rs"]
mod go;

use criterion::{measurement::WallTime, BenchmarkGroup, Criterion, SamplingMode};
use go::{
    go_dir, load_raphtory, load_store_release, millis, ntriples, owl_file, queries, queries_asof,
    release_dates, verify, versioned_queries, GoQuery, Versions, SUMS,
};
use oxigraph::{
    model::Dataset,
    sparql::{QueryResults, SparqlEvaluator},
    store::Store,
};
use raphtory::{
    prelude::*,
    rdf::{evaluator, with_temporal_functions, RaphtoryDataset, RdfFormat, RdfParser},
};
use regex::Regex;
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs::File,
    hint::black_box,
    io::BufReader,
    path::Path,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

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

/// A query whose first run takes longer than this is timed by single runs.
const SLOW: Duration = Duration::from_secs(1);

/// A slow case is run again until it has [`SLOW_RUNS`] runs or this much time, at least twice.
const SLOW_BUDGET: Duration = Duration::from_secs(10);

const SLOW_RUNS: u32 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Engine {
    Raphtory,
    Store,
    Dataset,
}

impl Engine {
    fn name(self) -> &'static str {
        match self {
            Engine::Raphtory => "raphtory",
            Engine::Store => "store",
            Engine::Dataset => "dataset",
        }
    }
}

#[derive(Default)]
struct Engines {
    pg: Option<PersistentGraph>,
    store: Option<Store>,
    dataset: Option<Dataset>,
}

impl Engines {
    fn engines(&self) -> Vec<Engine> {
        [
            (Engine::Raphtory, self.pg.is_some()),
            (Engine::Store, self.store.is_some()),
            (Engine::Dataset, self.dataset.is_some()),
        ]
        .into_iter()
        .filter_map(|(engine, loaded)| loaded.then_some(engine))
        .collect()
    }

    /// Runs a query and returns its number of solutions or triples. Every engine parses the
    /// query and reads each result once ([`consume`]).
    fn run(&self, engine: Engine, q: &GoQuery) -> usize {
        match engine {
            Engine::Raphtory => {
                let pg = self.pg.as_ref().expect("a graph");
                consume(
                    with_temporal_functions(evaluator(), pg.clone())
                        .parse_query(&q.raphtory)
                        .expect("query")
                        .on_queryable_dataset(RaphtoryDataset::new(pg.clone()))
                        .execute()
                        .expect("Raphtory query"),
                )
            }
            Engine::Store => consume(
                SparqlEvaluator::new()
                    .parse_query(&q.store)
                    .expect("query")
                    .on_store(self.store.as_ref().expect("a Store"))
                    .execute()
                    .expect("Store query"),
            ),
            Engine::Dataset => consume(
                SparqlEvaluator::new()
                    .parse_query(&q.store)
                    .expect("query")
                    .on_queryable_dataset(self.dataset.as_ref().expect("a dataset"))
                    .execute()
                    .expect("Dataset query"),
            ),
        }
    }
}

/// Reads every result.
fn consume(results: QueryResults<'_>) -> usize {
    match results {
        QueryResults::Solutions(solutions) => solutions.fold(0, |n, s| {
            s.expect("solution");
            n + 1
        }),
        QueryResults::Boolean(_) => 1,
        QueryResults::Graph(triples) => triples.fold(0, |n, t| {
            t.expect("triple");
            n + 1
        }),
    }
}

/// The time of one query on one engine.
#[derive(Clone, Debug, Default)]
struct Timing {
    iterations: u64,
    total: Duration,
    results: usize,
    /// The best of this many single runs instead of criterion's mean.
    runs: u32,
}

impl Timing {
    fn per_query(&self) -> f64 {
        self.total.as_secs_f64() / self.iterations as f64
    }
}

/// (group, query) -> engine -> timing, for the table at the end.
type Table = BTreeMap<(String, String), BTreeMap<Engine, Timing>>;

static TABLE: Mutex<Table> = Mutex::new(BTreeMap::new());

/// (group, query) -> what the query asks.
static WHAT: Mutex<BTreeMap<(String, String), &'static str>> = Mutex::new(BTreeMap::new());

/// (group, query) -> the number of results of each engine.
type Counts = BTreeMap<(String, String), Vec<(Engine, usize)>>;

static COUNTS: Mutex<Counts> = Mutex::new(BTreeMap::new());

/// Printed after the table.
static NOTES: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn note(line: String) {
    println!("{line}");
    NOTES.lock().unwrap().push(line);
}

/// A group with 10 samples over about `measure`.
fn small_group<'a>(
    c: &'a mut Criterion,
    name: &str,
    measure: Duration,
) -> BenchmarkGroup<'a, WallTime> {
    let mut group = c.benchmark_group(name);
    group
        .sample_size(10)
        .sampling_mode(SamplingMode::Flat)
        .warm_up_time(measure / 5)
        .measurement_time(measure);
    group
}

/// Which benchmarks the command line selects, parsed as criterion's `configure_from_args` does,
/// so [`bench_case`] runs nothing criterion would skip.
#[derive(Debug)]
struct Selection {
    filter: Filter,
    list: bool,
}

#[derive(Debug)]
enum Filter {
    All,
    Regex(Regex),
    Exact(String),
    Nothing,
}

/// Set in `main`, after criterion has read the command line.
static SELECTION: OnceLock<Selection> = OnceLock::new();

impl Selection {
    /// The long options of criterion's command line that take a value (as the next argument
    /// unless written `--option=value`).
    const WITH_VALUE: [&'static str; 17] = [
        "--color",
        "--colour",
        "--save-baseline",
        "--baseline",
        "--baseline-lenient",
        "--format",
        "--profile-time",
        "--load-baseline",
        "--sample-size",
        "--warm-up-time",
        "--measurement-time",
        "--nresamples",
        "--noise-threshold",
        "--confidence-level",
        "--significance-level",
        "--plotting-backend",
        "--output-format",
    ];

    fn from_args(args: impl IntoIterator<Item = String>) -> Self {
        let mut args = args.into_iter();
        let mut filter = None;
        let (mut exact, mut ignored, mut list, mut positional) = (false, false, false, false);
        while let Some(arg) = args.next() {
            if positional || arg == "-" || !arg.starts_with('-') {
                filter.get_or_insert(arg);
                continue;
            }
            match arg.as_str() {
                "--" => positional = true,
                "--exact" => exact = true,
                "--ignored" => ignored = true,
                "--list" => list = true,
                // the short options with a value: `-c`, `-s` and `-b` (`-sNAME` has it inline)
                "-c" | "-s" | "-b" => {
                    args.next();
                }
                long if Self::WITH_VALUE.contains(&long) => {
                    args.next();
                }
                _ => {}
            }
        }
        let filter = match filter {
            _ if ignored => Filter::Nothing,
            None => Filter::All,
            Some(filter) if exact => Filter::Exact(filter),
            Some(filter) => Filter::Regex(Regex::new(&filter).unwrap_or_else(|e| {
                panic!("Unable to parse '{filter}' as a regular expression: {e}")
            })),
        };
        Self { filter, list }
    }

    fn get() -> &'static Self {
        SELECTION.get_or_init(|| Self::from_args(std::env::args().skip(1)))
    }

    fn matches(&self, id: &str) -> bool {
        match &self.filter {
            Filter::All => true,
            Filter::Regex(regex) => regex.is_match(id),
            Filter::Exact(exact) => id == exact,
            Filter::Nothing => false,
        }
    }
}

/// Benchmarks a query on every loaded engine the command line selects ([`Selection`]) and
/// records its number of results. An engine whose first run exceeds [`SLOW`] is timed by the best
/// of single runs (marked `*`); criterion measures the others.
fn bench_case(
    group: &mut BenchmarkGroup<'_, WallTime>,
    group_name: &str,
    engines: &Engines,
    q: &GoQuery,
) {
    let case = q.name.split('@').next().unwrap_or(&q.name);
    let selection = Selection::get();
    let id = |engine: Engine| format!("{case}/{}", engine.name());
    let selected: Vec<Engine> = engines
        .engines()
        .into_iter()
        .filter(|&e| selection.matches(&format!("{group_name}/{}", id(e))))
        .collect();
    if selected.is_empty() {
        return;
    }
    if selection.list {
        for &engine in &selected {
            group.bench_function(id(engine), |b| b.iter(|| ()));
        }
        return;
    }
    let key = (group_name.to_owned(), case.to_owned());
    WHAT.lock().unwrap().insert(key.clone(), q.what);
    for engine in selected {
        let start = Instant::now();
        let results = engines.run(engine, q);
        let first = start.elapsed();
        COUNTS
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .push((engine, results));
        let mut timing = Timing {
            results,
            ..Timing::default()
        };
        if first > SLOW {
            let mut runs = vec![first];
            while runs.len() < 2
                || (runs.len() < SLOW_RUNS as usize && runs.iter().sum::<Duration>() < SLOW_BUDGET)
            {
                let start = Instant::now();
                black_box(engines.run(engine, q));
                runs.push(start.elapsed());
            }
            let secs: Vec<String> = runs
                .iter()
                .map(|t| format!("{:.2}", t.as_secs_f64()))
                .collect();
            println!(
                "{group_name}/{}: slow, timed by single runs ({} s)",
                id(engine),
                secs.join(", ")
            );
            timing.iterations = 1;
            timing.total = runs.iter().min().copied().expect("a run");
            timing.runs = runs.len() as u32;
        } else {
            let acc = Mutex::new((0u64, Duration::ZERO));
            group.bench_function(id(engine), |b| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    for _ in 0..iters {
                        black_box(engines.run(engine, q));
                    }
                    let elapsed = start.elapsed();
                    let mut acc = acc.lock().unwrap();
                    acc.0 += iters;
                    acc.1 += elapsed;
                    elapsed
                })
            });
            let (iterations, total) = *acc.lock().unwrap();
            if iterations == 0 {
                continue;
            }
            timing.iterations = iterations;
            timing.total = total;
        }
        TABLE
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .insert(engine, timing);
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
        bench_case(&mut group, name, engines, q);
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

/// The current release: loading it as published and skolemized, and the queries.
fn current(c: &mut Criterion, dir: &Path, versions: &Versions) {
    let date = versions.dates.last().expect("a release");
    let t = millis(date);
    let triples = versions.last.len();
    let open = || BufReader::new(File::open(owl_file(dir, date)).expect("go.owl"));

    let (pg, time, growth) = measure(|| {
        let pg = PersistentGraph::new();
        pg.load_rdf(t, open(), RdfFormat::RdfXml, None)
            .expect("load_rdf");
        pg
    });
    note_load("raphtory go.owl (RDF/XML)", time, triples, growth);
    drop(pg);
    let (store, time, growth) = measure(|| {
        let store = Store::new().expect("Store");
        let mut loader = store.bulk_loader();
        loader
            .load_from_reader(RdfFormat::RdfXml, open())
            .expect("bulk load");
        loader.commit().expect("bulk load");
        store
    });
    note_load("store go.owl (RDF/XML)", time, triples, growth);
    drop(store);

    let doc = ntriples(&versions.last);
    let (pg, time, growth) = measure(|| {
        let pg = PersistentGraph::new();
        pg.load_rdf(t, doc.as_slice(), RdfFormat::NTriples, None)
            .expect("load_rdf");
        pg
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
    let (dataset, time, growth) = measure(|| {
        let mut dataset = Dataset::new();
        for quad in RdfParser::from_format(RdfFormat::NTriples).for_slice(doc.as_slice()) {
            dataset.insert(&quad.expect("N-Triples"));
        }
        dataset
    });
    note_load("dataset skolemized (N-Triples)", time, triples, growth);
    drop(doc);
    note(format!(
        "go current release {date}: {} triples in Raphtory ({} nodes, {} layers), {} in the Store, {} in the Dataset",
        pg.valid().edges().explode_layers().iter().count(),
        pg.count_nodes(),
        pg.unique_layers().count(),
        store.len().expect("Store"),
        dataset.len()
    ));
    let engines = Engines {
        pg: Some(pg),
        store: Some(store),
        dataset: Some(dataset),
    };
    bench_queries(
        c,
        "go_current",
        Duration::from_secs(1),
        &engines,
        &queries(),
    );
}

/// The groups across releases and as of the first, middle and last release.
fn versioned_groups(dates: &[String]) -> Vec<(String, Duration, Vec<GoQuery>)> {
    let mut groups = vec![(
        "go_versions".to_owned(),
        Duration::from_secs(1),
        versioned_queries(dates),
    )];
    for date in [&dates[0], &dates[dates.len() / 2], &dates[dates.len() - 1]] {
        groups.push((
            format!("go_asof_{date}"),
            Duration::from_millis(500),
            queries_asof(date),
        ));
    }
    groups
}

/// The versions are loaded this many times by each engine, and timed by the best.
const LOAD_RUNS: usize = 2;

/// Every release: the Store with a graph per release, then Raphtory's versions, each loaded and
/// queried alone.
fn versioned(c: &mut Criterion, versions: Versions) {
    let dates = versions.dates.clone();
    let groups = versioned_groups(&dates);

    let (read, diff) = (versions.read_time, versions.diff_time);
    let mut best = Duration::MAX;
    let mut store = None;
    let mut growth = heap::Growth::default();
    for _ in 0..LOAD_RUNS {
        drop(store.take());
        let s = Store::new().expect("Store");
        let mut time = Duration::ZERO;
        heap::start();
        heap::pause();
        versions
            .for_each_release(|k, release| {
                let doc = ntriples(release.iter().copied());
                heap::resume();
                let start = Instant::now();
                load_store_release(&s, &dates[k], &doc)?;
                time += start.elapsed();
                heap::pause();
                Ok(())
            })
            .expect("Store load");
        growth = heap::stop();
        best = best.min(time);
        store = Some(s);
    }
    let store = store.expect("a Store");
    let quads = store.len().expect("Store");
    note(format!(
        "go load store {} releases (a graph each): {:.2} s, {quads} quads, {:.0} quads/s, heap peak {}, kept {}",
        dates.len(),
        best.as_secs_f64(),
        quads as f64 / best.as_secs_f64(),
        gb(growth.peak),
        gb(growth.kept)
    ));
    let store_load = best;
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

    let mut best = Duration::MAX;
    let mut pg = None;
    let mut growth = heap::Growth::default();
    for _ in 0..LOAD_RUNS {
        drop(pg.take());
        let (g, time, g_growth) = measure(|| {
            let g = PersistentGraph::new();
            load_raphtory(&g, &dates, &docs).expect("load_raphtory");
            g
        });
        best = best.min(time);
        growth = g_growth;
        pg = Some(g);
    }
    let pg = pg.expect("a graph");
    drop(docs);
    note(format!(
        "go load raphtory {} releases (the first, then the changes): {:.2} s, {events} triples written, {:.0} triples/s, heap peak {}, kept {}",
        dates.len(),
        best.as_secs_f64(),
        events as f64 / best.as_secs_f64(),
        gb(growth.peak),
        gb(growth.kept)
    ));
    note(format!(
        "go versions from the go.owl files: raphtory {:.1} s (read and skolemize {:.1} s, diff {:.1} s, load {:.1} s), store {:.1} s (read and skolemize {:.1} s, load {:.1} s)",
        (read + diff + best).as_secs_f64(),
        read.as_secs_f64(),
        diff.as_secs_f64(),
        best.as_secs_f64(),
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
    let engines = Engines {
        pg: Some(pg),
        ..Engines::default()
    };
    for (name, measure, queries) in &groups {
        bench_queries(c, name, *measure, &engines, queries);
    }
}

/// Formats a time per query.
fn ms(timing: Option<&Timing>) -> String {
    match timing {
        None => "-".to_owned(),
        Some(t) => {
            let ms = t.per_query() * 1e3;
            let star = if t.runs > 0 { "*" } else { "" };
            if ms >= 100.0 {
                format!("{ms:.0}{star}")
            } else if ms >= 1.0 {
                format!("{ms:.2}{star}")
            } else {
                format!("{ms:.3}{star}")
            }
        }
    }
}

fn print_table() {
    let table = TABLE.lock().unwrap();
    if table.is_empty() {
        return;
    }
    let what = WHAT.lock().unwrap();
    let mut out = String::new();
    let _ = writeln!(
        out,
        "\n| group | query | what | raphtory ms | store ms | dataset ms | raphtory / store | raphtory / dataset | results |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|---|---|---|---|");
    // group -> logs of raphtory / store
    let mut ratios: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for (key, timings) in table.iter() {
        let r = timings.get(&Engine::Raphtory);
        let ratio = |other: Engine| match (r, timings.get(&other)) {
            (Some(r), Some(o)) => format!("{:.2}", r.per_query() / o.per_query()),
            _ => "-".to_owned(),
        };
        if let (Some(r), Some(s)) = (r, timings.get(&Engine::Store)) {
            ratios
                .entry(key.0.as_str())
                .or_default()
                .push((r.per_query() / s.per_query()).ln());
        }
        let results = timings.values().next().map_or(0, |t| t.results);
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {results} |",
            key.0,
            key.1,
            what.get(key).copied().unwrap_or(""),
            ms(r),
            ms(timings.get(&Engine::Store)),
            ms(timings.get(&Engine::Dataset)),
            ratio(Engine::Store),
            ratio(Engine::Dataset),
        );
    }
    println!("{out}");
    println!("(ms per query, mean over every iteration; * = best of 2 or 3 single runs)");
    for (group, logs) in ratios {
        println!(
            "{group}: raphtory / store, geometric mean {:.2} over {} queries",
            (logs.iter().sum::<f64>() / logs.len() as f64).exp(),
            logs.len()
        );
    }
    for line in NOTES.lock().unwrap().iter() {
        println!("{line}");
    }
    for ((group, case), counts) in COUNTS.lock().unwrap().iter() {
        if counts.iter().any(|(_, n)| *n != counts[0].1) {
            println!("{group}/{case}: the engines return different numbers of results: {counts:?}");
        }
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
    let mut c = Criterion::default().without_plots().configure_from_args();
    // after criterion, which exits on `--help` and rejects a bad command line
    SELECTION.get_or_init(|| Selection::from_args(std::env::args().skip(1)));
    let start = Instant::now();
    verify(&dir, SUMS).expect("the GO releases do not match go.sha256");
    let dates = release_dates(SUMS);
    let versions = Versions::read(&dir, &dates).expect("GO releases");
    note(format!(
        "go: {} releases checked, read, skolemized and diffed in {:.1} s (read and skolemize {:.1} s, diff {:.1} s)",
        dates.len(),
        start.elapsed().as_secs_f64(),
        versions.read_time.as_secs_f64(),
        versions.diff_time.as_secs_f64()
    ));
    current(&mut c, &dir, &versions);
    versioned(&mut c, versions);
    c.final_summary();
    print_table();
}
