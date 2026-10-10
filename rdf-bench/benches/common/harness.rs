//! The engines and the measuring shared by the `sparql` and `go` benchmarks. Both engines evaluate
//! SPARQL with spareval:
//!
//! - `raphtory`: `RdfView::sparql` on the `valid()` view for the current state, or on the whole
//!   graph for queries that name their time graphs;
//! - `store`: oxigraph's in-memory `Store` (built without RocksDB), queried with
//!   `SparqlEvaluator`.
//!
//! Both do the work of `RdfView::sparql` around evaluation: they run each query on a new thread
//! with the stack `sparql` gives it, and collect every result (the triples of `CONSTRUCT` and
//! `DESCRIBE` without duplicates) before counting them ([`collect`]).
//!
//! A case whose first run exceeds its slow threshold is timed by the best of a few single runs
//! (marked `*`); criterion measures the others ([`bench_case`]). The engines' result counts are
//! compared, and [`print_table`] gives the mean time per query of every case and engine.
//!
//! A criterion filter (`-- <regex>` or `-- --exact <id>`) is matched against
//! `<group>/<case>/<engine>` before the first runs, so excluded cases are not run at all.
use criterion::{measurement::WallTime, BenchmarkGroup, Criterion, SamplingMode};
use oxigraph::{
    sparql::{QueryResults, SparqlEvaluator},
    store::Store,
};
use raphtory::{prelude::*, rdf::SparqlResults};
use regex::Regex;
use rustc_hash::FxHashSet;
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    hint::black_box,
    sync::{Mutex, OnceLock},
    thread,
    time::{Duration, Instant},
};

/// A case whose first run takes longer than this is timed by single runs.
pub const SLOW: Duration = Duration::from_secs(1);

/// A slow case is run again until it has [`SLOW_RUNS`] runs or this much time, at least twice.
const SLOW_BUDGET: Duration = Duration::from_secs(10);

const SLOW_RUNS: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Engine {
    Raphtory,
    Store,
}

impl Engine {
    pub fn name(self) -> &'static str {
        match self {
            Engine::Raphtory => "raphtory",
            Engine::Store => "store",
        }
    }
}

/// A query as each engine runs it.
pub trait Query {
    fn raphtory(&self) -> &str;

    fn store(&self) -> &str {
        self.raphtory()
    }
}

impl Query for String {
    fn raphtory(&self) -> &str {
        self
    }
}

/// The engines a case runs on; one that is not loaded is skipped.
#[derive(Default)]
pub struct Engines {
    pub raphtory: Option<RdfView>,
    pub store: Option<Store>,
}

impl Engines {
    fn loaded(&self) -> Vec<Engine> {
        [
            (Engine::Raphtory, self.raphtory.is_some()),
            (Engine::Store, self.store.is_some()),
        ]
        .into_iter()
        .filter_map(|(engine, loaded)| loaded.then_some(engine))
        .collect()
    }

    /// Runs a query and returns its number of solutions or triples (1 for `ASK`).
    pub fn run(&self, engine: Engine, q: &impl Query) -> usize {
        let results = match engine {
            Engine::Raphtory => {
                let view = self.raphtory.as_ref().expect("a graph");
                view.sparql(q.raphtory()).expect("Raphtory query")
            }
            Engine::Store => {
                let store = self.store.as_ref().expect("a Store");
                let query = q.store();
                // the stack `RdfView::sparql` gives the query
                let stack_size = (16 << 20) + query.len() * (8 << 10);
                thread::scope(|scope| {
                    thread::Builder::new()
                        .stack_size(stack_size)
                        .spawn_scoped(scope, || {
                            collect(
                                SparqlEvaluator::new()
                                    .parse_query(query)
                                    .expect("query")
                                    .on_store(store)
                                    .execute()
                                    .expect("Store query"),
                            )
                        })
                        .expect("a thread")
                        .join()
                        .expect("Store query")
                })
            }
        };
        match results {
            SparqlResults::Solutions { rows, .. } => rows.len(),
            SparqlResults::Boolean(_) => 1,
            SparqlResults::Graph(triples) => triples.len(),
        }
    }

    fn run_all<Q: Query>(&self, engine: Engine, queries: &[Q]) -> usize {
        queries.iter().map(|q| self.run(engine, q)).sum()
    }
}

/// Collects the results of the Store as `RdfView::sparql` collects its own.
fn collect(results: QueryResults<'_>) -> SparqlResults {
    match results {
        QueryResults::Solutions(solutions) => {
            let variables = solutions.variables().to_vec();
            let rows = solutions
                .map(|solution| solution.expect("solution").values().to_vec())
                .collect();
            SparqlResults::Solutions { variables, rows }
        }
        QueryResults::Boolean(value) => SparqlResults::Boolean(value),
        QueryResults::Graph(triples) => {
            let mut seen = FxHashSet::default();
            SparqlResults::Graph(
                triples
                    .map(|triple| triple.expect("triple"))
                    .filter(|triple| seen.insert(triple.clone()))
                    .collect(),
            )
        }
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
    /// The best of this many single runs instead of criterion's mean.
    runs: usize,
}

impl Timing {
    fn per_query(&self) -> f64 {
        self.total.as_secs_f64() / (self.iterations as f64 * self.queries as f64)
    }
}

/// (group, case)
type Key = (String, String);

static TABLE: Mutex<BTreeMap<Key, BTreeMap<Engine, Timing>>> = Mutex::new(BTreeMap::new());

/// What each case asks.
static WHAT: Mutex<BTreeMap<Key, String>> = Mutex::new(BTreeMap::new());

/// The number of results of each engine.
static COUNTS: Mutex<BTreeMap<Key, Vec<(Engine, usize)>>> = Mutex::new(BTreeMap::new());

/// Printed after the table.
static NOTES: Mutex<Vec<String>> = Mutex::new(Vec::new());

pub fn note(line: String) {
    println!("{line}");
    NOTES.lock().unwrap().push(line);
}

/// A group with 10 samples over about `measure`.
pub fn small_group<'a>(
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

/// Criterion configured from the command line, which also sets the [`Selection`].
pub fn criterion() -> Criterion {
    // many cases: plotting each would take longer than measuring it
    let c = Criterion::default().without_plots().configure_from_args();
    // after criterion, which exits on `--help` and rejects a bad command line
    SELECTION.get_or_init(|| Selection::from_args(std::env::args().skip(1)));
    c
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

/// Set by [`criterion`], after criterion has read the command line.
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

/// Benchmarks `queries` as the case `case` of `group` on every loaded engine the command line
/// selects ([`Selection`]), and records what the case asks and each engine's number of results.
///
/// Each engine first runs the queries once; one that exceeds `slow` stops there, and later
/// engines run only the queries it ran. On those queries, an engine whose first run exceeded
/// `slow` is timed by the best of up to [`SLOW_RUNS`] single runs (marked `*`); criterion
/// measures the others.
pub fn bench_case<Q: Query>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    group_name: &str,
    case: &str,
    what: &str,
    engines: &Engines,
    queries: &[Q],
    slow: Duration,
) {
    let selection = Selection::get();
    let id = |engine: Engine| format!("{case}/{}", engine.name());
    let selected: Vec<Engine> = engines
        .loaded()
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
    WHAT.lock().unwrap().insert(key.clone(), what.to_owned());
    // per engine, the time and number of results of each query of its first run
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
        first_runs.push((engine, runs));
    }
    let done = queries.len();
    for (engine, runs) in first_runs {
        let runs = &runs[..done];
        let results = runs.iter().map(|(_, n)| n).sum();
        COUNTS
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .push((engine, results));
        let mut timing = Timing {
            queries: done,
            results,
            ..Timing::default()
        };
        let first: Duration = runs.iter().map(|(time, _)| *time).sum();
        if first > slow {
            let mut times = vec![first];
            while times.len() < 2
                || (times.len() < SLOW_RUNS && times.iter().sum::<Duration>() < SLOW_BUDGET)
            {
                let start = Instant::now();
                black_box(engines.run_all(engine, queries));
                times.push(start.elapsed());
            }
            let secs: Vec<String> = times
                .iter()
                .map(|t| format!("{:.2}", t.as_secs_f64()))
                .collect();
            println!(
                "{group_name}/{}: slow, timed by single runs of {done} queries ({} s)",
                id(engine),
                secs.join(", ")
            );
            timing.iterations = 1;
            timing.total = times.iter().min().copied().expect("a run");
            timing.runs = times.len();
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
            .entry(key.clone())
            .or_default()
            .insert(engine, timing);
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

/// Prints the time per query of every case, the geometric mean of raphtory / store per group,
/// the notes and the cases whose engines return different numbers of results.
pub fn print_table() {
    let table = TABLE.lock().unwrap();
    if table.is_empty() {
        return;
    }
    let what = WHAT.lock().unwrap();
    let mut out = String::new();
    let _ = writeln!(
        out,
        "\n| group | case | what | raphtory ms | store ms | raphtory / store | results |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|---|---|");
    // group -> logs of raphtory / store
    let mut ratios: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for (key, timings) in table.iter() {
        let (r, s) = (timings.get(&Engine::Raphtory), timings.get(&Engine::Store));
        let ratio = match (r, s) {
            (Some(r), Some(s)) => {
                let ratio = r.per_query() / s.per_query();
                ratios.entry(key.0.as_str()).or_default().push(ratio.ln());
                format!("{ratio:.2}")
            }
            _ => "-".to_owned(),
        };
        let results = timings
            .values()
            .next()
            .map_or(0, |t| t.results / t.queries.max(1));
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {ratio} | {results} |",
            key.0,
            key.1,
            what.get(key).map_or("", String::as_str),
            ms(r),
            ms(s),
        );
    }
    println!("{out}");
    println!(
        "(ms per query, mean over every iteration; * = best of 2 or 3 single runs; results = per query)"
    );
    for (group, logs) in ratios {
        println!(
            "{group}: raphtory / store, geometric mean {:.2} over {} cases",
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
