//! General SPARQL performance: the Berlin SPARQL Benchmark (BSBM) and the Sparqloscope feature
//! queries on three engines that evaluate SPARQL with the same evaluator (spareval):
//!
//! - `raphtory`: a `PersistentGraph` loaded with `load_rdf`, evaluated as `sparql()` does, but
//!   with results read and dropped one by one as on the other engines;
//! - `store`: oxigraph's in-memory `Store` (built without RocksDB), queried with
//!   `SparqlEvaluator`;
//! - `dataset`: spareval over an `oxrdf::Dataset`, the correctness suites' oracle.
//!
//! The data is not in the repository (see the `bsbm` module, shared with
//! `raphtory-rdf-tests/tests/bsbm.rs`): `make bench-sparql` (in `rdf-bench/`) downloads it and
//! runs this with `RAPHTORY_RDF_DATA` set; without the data it prints a message and does nothing.
//! `RAPHTORY_BSBM_SCALES` picks the scales (default `1000,5000`) and `RAPHTORY_BSBM_SKIP` skips
//! cases (comma-separated prefixes of `<group>/<case>`, default [`DEFAULT_SKIP`]; empty skips
//! nothing).
//!
//! What is measured, per scale:
//!
//! - **Loading** (timed once per engine, best of three for 1,000 products, printed): the dataset
//!   as N-Triples, then the updates of the explore-and-update mix (1,000 products only). The
//!   Store runs them as SPARQL Update; Raphtory maps them onto RDF writes
//!   (`bsbm::apply_raphtory`) and the Dataset does the same work
//!   (`bsbm::apply_dataset_like_raphtory`).
//! - `bsbm<n>_explore/Q<t>/<engine>`: the first ten distinct queries of explore template `t`.
//! - `bsbm<n>_bi/Q<t>/<engine>`: the first two distinct queries of business intelligence
//!   template `t`.
//! - `bsbm<n>_mix/explore/<engine>`: the last ten runs of the 25-query explore mix, from which
//!   the query mixes per hour (QMpH) follow.
//! - `bsbm<n>_sparqloscope/<i>/<engine>`: each Sparqloscope query.
//!
//! All queries run on the final state. Slow cases are timed by a single first run instead of by
//! criterion (see [`bench_case`]; marked `*` in the table). The engines' result counts are
//! compared and differences printed. At the end a table gives the mean time per query of every
//! case and engine.
//!
//! Run with `cargo bench -p raphtory-rdf-bench --features rdf --bench sparql` (or `make
//! bench-sparql` in `rdf-bench/`). A criterion filter (`-- <regex>`, or `-- --exact <id>`) is matched against
//! `<group>/<case>/<engine>` before the first runs, so excluded cases are not run at all.
#[path = "../../raphtory-rdf-tests/tests/common/bsbm.rs"]
mod bsbm;

use bsbm::{
    apply_dataset_like_raphtory, apply_raphtory, bsbm_dir, instances, load_dataset,
    read_sparqloscope, templates, update_time, Bsbm, Op, BASE_TIME,
};
use criterion::{measurement::WallTime, BenchmarkGroup, Criterion, SamplingMode};
use oxigraph::{
    model::Dataset,
    sparql::{QueryResults, SparqlEvaluator},
    store::Store,
};
use raphtory::{
    prelude::*,
    rdf::{evaluator, with_temporal_functions, RaphtoryDataset, RdfFormat},
};
use regex::Regex;
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    hint::black_box,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

/// Picks the scales to run (`1000,5000` by default).
const SCALES_ENV: &str = "RAPHTORY_BSBM_SCALES";

/// Skips the cases whose `<group>/<case>` starts with one of these comma-separated prefixes.
const SKIP_ENV: &str = "RAPHTORY_BSBM_SKIP";

/// What is skipped when [`SKIP_ENV`] is not set: template 7 of the business intelligence mix at
/// 5,000 products, which is very slow on every engine.
const DEFAULT_SKIP: &str = "bsbm5000_bi/Q07";

/// A case whose first run takes longer than this is measured by that run instead of by
/// criterion (see [`bench_case`]).
const SLOW: Duration = Duration::from_secs(1);

/// The same for the Sparqloscope queries, which are many.
const SLOW_SPARQLOSCOPE: Duration = Duration::from_millis(300);

/// The engines, in table order.
const ENGINES: [Engine; 3] = [Engine::Raphtory, Engine::Store, Engine::Dataset];

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

/// The same triples in the three engines.
struct Engines {
    pg: PersistentGraph,
    store: Store,
    dataset: Dataset,
}

impl Engines {
    /// Runs a query and returns its number of solutions or triples (1 for `ASK`). Every engine
    /// parses the query and reads each result once ([`consume`]).
    fn run(&self, engine: Engine, query: &str) -> usize {
        match engine {
            Engine::Raphtory => consume(
                with_temporal_functions(evaluator(), self.pg.clone())
                    .parse_query(query)
                    .expect("query")
                    .on_queryable_dataset(RaphtoryDataset::new(self.pg.clone()))
                    .execute()
                    .expect("Raphtory query"),
            ),
            Engine::Store => consume(
                SparqlEvaluator::new()
                    .parse_query(query)
                    .expect("query")
                    .on_store(&self.store)
                    .execute()
                    .expect("Store query"),
            ),
            Engine::Dataset => consume(
                SparqlEvaluator::new()
                    .parse_query(query)
                    .expect("query")
                    .on_queryable_dataset(&self.dataset)
                    .execute()
                    .expect("Dataset query"),
            ),
        }
    }

    fn run_all(&self, engine: Engine, queries: &[String]) -> usize {
        queries.iter().map(|q| self.run(engine, q)).sum()
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

/// The mean time per query of one case on one engine.
#[derive(Clone, Debug, Default)]
struct Timing {
    iterations: u64,
    total: Duration,
    /// Queries per iteration.
    queries: usize,
    results: usize,
    /// Timed once instead of by criterion.
    once: bool,
}

impl Timing {
    fn per_query(&self) -> f64 {
        self.total.as_secs_f64() / (self.iterations as f64 * self.queries as f64)
    }
}

/// (group, case) -> engine -> timing, for the table at the end.
type Table = BTreeMap<(String, String), BTreeMap<Engine, Timing>>;

static TABLE: Mutex<Table> = Mutex::new(BTreeMap::new());

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

/// Whether `RAPHTORY_BSBM_SKIP` (a comma-separated list of prefixes of `<group>/<case>`,
/// [`DEFAULT_SKIP`] when it is not set) skips a case.
fn skipped(group_name: &str, case: &str) -> bool {
    let id = format!("{group_name}/{case}");
    let skip = std::env::var(SKIP_ENV).unwrap_or_else(|_| DEFAULT_SKIP.to_owned());
    skip.split(',')
        .map(str::trim)
        .any(|prefix| !prefix.is_empty() && id.starts_with(prefix))
}

/// Benchmarks `queries` on every engine the command line selects ([`Selection`]) as the case
/// `case` of `group`, and checks that the engines return the same numbers of results.
///
/// Each engine first runs the queries once; one that exceeds `slow` stops there, and later
/// engines run only the queries it ran. On those queries, an engine whose first run exceeded
/// `slow` is measured by that run (marked `*`); criterion measures the others.
fn bench_case(
    group: &mut BenchmarkGroup<'_, WallTime>,
    group_name: &str,
    case: &str,
    engines: &Engines,
    queries: &[String],
    slow: Duration,
) {
    let selection = Selection::get();
    let id = |engine: Engine| format!("{case}/{}", engine.name());
    let selected: Vec<Engine> = ENGINES
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
    if skipped(group_name, case) {
        note(format!("{group_name}/{case}: skipped ({SKIP_ENV})"));
        return;
    }
    // the first runs: per engine, the time and number of results of each query it ran
    let mut queries = queries;
    let mut first_runs: Vec<(Engine, Vec<(Duration, usize)>)> = Vec::new();
    for &engine in &selected {
        let start = Instant::now();
        let mut runs = Vec::new();
        for q in queries {
            let query_start = Instant::now();
            let results = engines.run(engine, q);
            runs.push((query_start.elapsed(), results));
            if start.elapsed() > slow {
                break;
            }
        }
        queries = &queries[..runs.len()];
        println!(
            "{group_name}/{case}/{}: first run {:.3} ms per query ({} queries), {} results",
            engine.name(),
            start.elapsed().as_secs_f64() * 1e3 / runs.len() as f64,
            runs.len(),
            runs.iter().map(|(_, n)| n).sum::<usize>()
        );
        first_runs.push((engine, runs));
    }
    let done = queries.len();
    let mut counts = Vec::new();
    for (engine, runs) in first_runs {
        let runs = &runs[..done];
        let results = runs.iter().map(|(_, n)| n).sum();
        counts.push((engine, results));
        let mut timing = Timing {
            queries: done,
            results,
            ..Timing::default()
        };
        let first: Duration = runs.iter().map(|(time, _)| *time).sum();
        if first > slow {
            println!(
                "{group_name}/{case}/{}: slow, timed by its first run of {done} queries",
                engine.name()
            );
            timing.iterations = 1;
            timing.total = first;
            timing.once = true;
        } else {
            let acc = Mutex::new((0u64, Duration::ZERO));
            group.bench_function(id(engine), |b| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    for _ in 0..iters {
                        black_box(engines.run_all(engine, queries));
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
                // criterion did not run it
                continue;
            }
            timing.iterations = iterations;
            timing.total = total;
        }
        TABLE
            .lock()
            .unwrap()
            .entry((group_name.to_owned(), case.to_owned()))
            .or_default()
            .insert(engine, timing);
    }
    if counts.iter().any(|(_, n)| *n != counts[0].1) {
        note(format!(
            "{group_name}/{case}: the engines return different numbers of results: {counts:?}"
        ));
    }
}

/// Loads the scale into the three engines, timing each, and returns them.
fn load(bsbm: &Bsbm) -> Engines {
    let n = bsbm.products;
    let runs = if n <= 1000 { 3 } else { 1 };
    let updates = bsbm.updates();
    let best = |f: &mut dyn FnMut() -> Duration| (0..runs).map(|_| f()).min().unwrap();
    let report = |engine: &str, what: &str, time: Duration, items: usize, unit: &str| {
        note(format!(
            "bsbm{n} load {engine} {what}: {:.3} s, {:.0} {unit}/s",
            time.as_secs_f64(),
            items as f64 / time.as_secs_f64()
        ));
    };

    // Raphtory
    let mut pg = PersistentGraph::new();
    let base = best(&mut || {
        pg = PersistentGraph::new();
        let start = Instant::now();
        pg.load_rdf(
            BASE_TIME,
            bsbm.dataset.as_slice(),
            RdfFormat::NTriples,
            None,
        )
        .expect("load_rdf");
        start.elapsed()
    });
    report("raphtory", "dataset", base, bsbm.dataset_triples, "triples");
    let start = Instant::now();
    for (k, op) in updates.iter().enumerate() {
        apply_raphtory(&pg, update_time(k), op).expect("update");
    }
    let raphtory_updates = start.elapsed();

    // the Store
    let mut store = Store::new().expect("Store");
    let base = best(&mut || {
        store = Store::new().expect("Store");
        let start = Instant::now();
        let mut loader = store.bulk_loader();
        loader
            .load_from_slice(RdfFormat::NTriples, bsbm.dataset.as_slice())
            .expect("bulk load");
        loader.commit().expect("bulk load");
        start.elapsed()
    });
    report("store", "dataset", base, bsbm.dataset_triples, "triples");
    let start = Instant::now();
    for op in &updates {
        SparqlEvaluator::new()
            .parse_update(op.text())
            .expect("update")
            .on_store(&store)
            .execute()
            .expect("Store update");
    }
    let store_updates = start.elapsed();

    // the Dataset
    let mut dataset = Dataset::new();
    let base = best(&mut || {
        let start = Instant::now();
        dataset = load_dataset(&bsbm.dataset).expect("Dataset");
        start.elapsed()
    });
    report("dataset", "dataset", base, bsbm.dataset_triples, "triples");
    let start = Instant::now();
    for op in &updates {
        apply_dataset_like_raphtory(&mut dataset, op).expect("Dataset update");
    }
    let dataset_updates = start.elapsed();

    if !updates.is_empty() {
        let inserted = bsbm.inserted_triples();
        for (engine, time) in [
            ("raphtory", raphtory_updates),
            ("store", store_updates),
            ("dataset", dataset_updates),
        ] {
            note(format!(
                "bsbm{n} load {engine} {} updates ({inserted} triples inserted): {:.3} s, {:.0} updates/s, {:.0} inserted triples/s",
                updates.len(),
                time.as_secs_f64(),
                updates.len() as f64 / time.as_secs_f64(),
                inserted as f64 / time.as_secs_f64()
            ));
        }
    }
    let triples = pg.valid().edges().explode_layers().iter().count();
    let store_len = store.len().expect("Store");
    note(format!(
        "bsbm{n}: {triples} triples in Raphtory ({} nodes, {} layers), {store_len} in the Store, {} in the Dataset",
        pg.count_nodes(),
        pg.unique_layers().count(),
        dataset.len()
    ));
    Engines { pg, store, dataset }
}

fn bench_scale(c: &mut Criterion, dir: &std::path::Path, products: usize) {
    if !Bsbm::file(dir, "dataset", products, "nt.bz2").is_file() {
        println!("skipping BSBM {products}: dataset-{products}.nt.bz2 is missing");
        return;
    }
    let bsbm = Bsbm::read(dir, products).expect("BSBM files");
    let engines = load(&bsbm);
    let n = products;

    let name = format!("bsbm{n}_explore");
    let mut group = small_group(c, &name, Duration::from_secs(1));
    for t in templates(&bsbm.explore) {
        let queries = instances(&bsbm.explore, t, 10);
        bench_case(
            &mut group,
            &name,
            &format!("Q{t:02}"),
            &engines,
            &queries,
            SLOW,
        );
    }
    group.finish();

    let name = format!("bsbm{n}_bi");
    let mut group = small_group(c, &name, Duration::from_secs(1));
    for t in templates(&bsbm.bi) {
        let queries = instances(&bsbm.bi, t, 2);
        bench_case(
            &mut group,
            &name,
            &format!("Q{t:02}"),
            &engines,
            &queries,
            SLOW,
        );
    }
    group.finish();

    // the last ten runs of the 25-query explore mix
    let name = format!("bsbm{n}_mix");
    let mut group = small_group(c, &name, Duration::from_secs(2));
    let mix: Vec<String> = bsbm
        .explore
        .iter()
        .rev()
        .take(250)
        .rev()
        .filter_map(|e| match &e.op {
            Op::Query(q) => Some(q.clone()),
            _ => None,
        })
        .collect();
    bench_case(&mut group, &name, "explore", &engines, &mix, SLOW);
    group.finish();

    let sparqloscope = dir.join("sparqloscope-bsbm-5000.csv");
    if sparqloscope.is_file() {
        let name = format!("bsbm{n}_sparqloscope");
        let mut group = small_group(c, &name, Duration::from_millis(500));
        let queries = read_sparqloscope(&sparqloscope).expect("Sparqloscope queries");
        for (i, (description, query)) in queries.iter().enumerate() {
            let case = format!("{i:03}");
            NOTES
                .lock()
                .unwrap()
                .push(format!("{name}/{case}: {description}"));
            bench_case(
                &mut group,
                &name,
                &case,
                &engines,
                std::slice::from_ref(query),
                SLOW_SPARQLOSCOPE,
            );
        }
        group.finish();
    }
}

/// Formats a time per query.
fn ms(timing: Option<&Timing>) -> String {
    match timing {
        None => "-".to_owned(),
        Some(t) => {
            let ms = t.per_query() * 1e3;
            let star = if t.once { "*" } else { "" };
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
    let mut out = String::new();
    let _ = writeln!(
        out,
        "\n| group | case | raphtory ms/query | store ms/query | dataset ms/query | raphtory / store | raphtory / dataset | results |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|---|---|---|");
    for ((group, case), timings) in table.iter() {
        let r = timings.get(&Engine::Raphtory);
        let ratio = |other: Engine| match (r, timings.get(&other)) {
            (Some(r), Some(o)) => format!("{:.1}", r.per_query() / o.per_query()),
            _ => "-".to_owned(),
        };
        let results = timings
            .values()
            .next()
            .map_or(0, |t| t.results / t.queries.max(1));
        let _ = writeln!(
            out,
            "| {group} | {case} | {} | {} | {} | {} | {} | {results} |",
            ms(r),
            ms(timings.get(&Engine::Store)),
            ms(timings.get(&Engine::Dataset)),
            ratio(Engine::Store),
            ratio(Engine::Dataset),
        );
        if case == "explore" && group.ends_with("_mix") {
            let qmph = |t: Option<&Timing>| {
                t.map_or("-".to_owned(), |t| {
                    format!("{:.0}", 3600.0 / (t.per_query() * 25.0))
                })
            };
            NOTES.lock().unwrap().push(format!(
                "{group}: explore query mixes per hour (QMpH): raphtory {}, store {}, dataset {}",
                qmph(r),
                qmph(timings.get(&Engine::Store)),
                qmph(timings.get(&Engine::Dataset))
            ));
        }
    }
    println!("{out}");
    println!("(ms per query, mean over every iteration; * = timed once; results = per query)");
    for line in NOTES.lock().unwrap().iter() {
        println!("{line}");
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
    let scales: Vec<usize> = std::env::var(SCALES_ENV)
        .unwrap_or_else(|_| "1000,5000".to_owned())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    // hundreds of cases: plotting each would take longer than measuring it
    let mut c = Criterion::default().without_plots().configure_from_args();
    // after criterion, which exits on `--help` and rejects a bad command line
    SELECTION.get_or_init(|| Selection::from_args(std::env::args().skip(1)));
    for products in scales {
        bench_scale(&mut c, &dir, products);
    }
    c.final_summary();
    print_table();
}
