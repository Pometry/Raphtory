//! Window counts over a disk graph with JIT-compiled kernels (`relational/jit_stage0.md`).
//!
//! ```text
//! cargo run --release -p raphtory --features io,jit --example jit_window_count -- \
//!     <graph-dir> <start> <end> [--layer NAME] [--count WHAT] [--backend B]
//!     [--morsel ROWS|segment] [--repeat N] [--check]
//!
//! # with the LLVM backend too:
//! LIBRARY_PATH=/opt/homebrew/lib cargo run --release -p raphtory --features io,jit-llvm \
//!     --example jit_window_count -- ...
//! ```
//!
//! Counts, in one layer and the window `[start, end)`, any of `active-nodes`,
//! `node-events`, `active-edges`, `edge-additions` (default: all). Each count is a
//! compiled kernel run once per segment, segments in parallel, with every backend the
//! build has (`--backend cranelift|llvm` picks one). Segments are split into morsels of
//! `--morsel` rows (default 32k; `segment` for one per segment), run in parallel.
//!
//! Every kernel is compared with `storage`: the same per-segment scan written in Rust
//! against the storage API (`ffi::storage_count`), also in parallel over segments. Its
//! count must match exactly, and both are timed the same way. Prints how long loading,
//! locking, preparing, compiling and running took. `--check` also runs Raphtory's
//! windowed view. That goes through `node.history()`, which includes a node's own
//! updates, so `active-nodes` only agrees on graphs whose nodes have none.

use raphtory::{prelude::*, storage::core_ops::CoreGraphOps};
use std::{
    path::PathBuf,
    process::exit,
    time::{Duration, Instant},
};
use storage::ffi::{
    storage_count, Counted, FfiWindow, JitBackend, Morsels, PreparedGraph, WindowCounter, BACKENDS,
};

#[cfg(target_os = "macos")]
use tikv_jemallocator::Jemalloc;

#[cfg(target_os = "macos")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

struct Args {
    graph_dir: PathBuf,
    window: FfiWindow,
    layer: String,
    counts: Vec<Counted>,
    backends: Vec<JitBackend>,
    morsels: Morsels,
    repeat: usize,
    check: bool,
}

const USAGE: &str = "usage: jit_window_count <graph-dir> <start> <end> \
    [--layer NAME] [--count active-nodes|node-events|active-edges|edge-additions] \
    [--backend cranelift|llvm] [--morsel ROWS|segment] [--repeat N] [--check]";

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let mut positional = Vec::new();
    let mut layer = "_default".to_string();
    let mut counts = Vec::new();
    let mut backends = Vec::new();
    let mut morsels = Morsels::default();
    let mut repeat = 5;
    let mut check = false;

    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--layer" => layer = value()?,
            "--count" => counts.push(parse_counted(&value()?)?),
            "--backend" => backends.push(parse_backend(&value()?)?),
            "--morsel" => morsels = parse_morsels(&value()?)?,
            "--repeat" => repeat = value()?.parse().map_err(|e| format!("--repeat: {e}"))?,
            "--check" => check = true,
            "-h" | "--help" => return Err(USAGE.to_string()),
            _ => positional.push(arg),
        }
    }

    let [graph_dir, start, end] = positional.as_slice() else {
        return Err(USAGE.to_string());
    };
    let parse_time = |name, value: &str| value.parse::<i64>().map_err(|e| format!("{name}: {e}"));
    Ok(Args {
        graph_dir: graph_dir.into(),
        window: FfiWindow::new(parse_time("start", start)?, parse_time("end", end)?),
        layer,
        counts: if counts.is_empty() {
            Counted::ALL.to_vec()
        } else {
            counts
        },
        backends: if backends.is_empty() {
            BACKENDS.to_vec()
        } else {
            backends
        },
        morsels,
        repeat: repeat.max(1),
        check,
    })
}

fn parse_morsels(value: &str) -> Result<Morsels, String> {
    match value {
        "segment" => Ok(Morsels::Segment),
        rows => match rows.parse::<u64>() {
            Ok(0) | Err(_) => Err(format!(
                "--morsel: expected rows > 0 or `segment`, got {rows:?}"
            )),
            Ok(rows) => Ok(Morsels::Rows(rows)),
        },
    }
}

fn parse_backend(name: &str) -> Result<JitBackend, String> {
    let backend = match name {
        "cranelift" => JitBackend::Cranelift,
        #[cfg(feature = "jit-llvm")]
        "llvm" => JitBackend::Llvm,
        #[cfg(not(feature = "jit-llvm"))]
        "llvm" => return Err("built without the `jit-llvm` feature".to_string()),
        _ => return Err(format!("unknown backend {name:?}\n{USAGE}")),
    };
    Ok(backend)
}

fn parse_counted(name: &str) -> Result<Counted, String> {
    match name {
        "active-nodes" => Ok(Counted::ActiveNodes),
        "node-events" => Ok(Counted::NodeEvents),
        "active-edges" => Ok(Counted::ActiveEdges),
        "edge-additions" => Ok(Counted::EdgeAdditions),
        _ => Err(format!("unknown count {name:?}\n{USAGE}")),
    }
}

/// Runs `f` once, returning its result and how long it took.
fn timed<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let start = Instant::now();
    let result = f();
    (result, start.elapsed())
}

/// Runs a count `repeat` times: the count, the fastest run and the mean.
fn time_runs(
    name: &str,
    repeat: usize,
    mut count: impl FnMut() -> u64,
) -> (u64, Duration, Duration) {
    let runs: Vec<(u64, Duration)> = (0..repeat).map(|_| timed(&mut count)).collect();
    let value = runs[0].0;
    assert!(
        runs.iter().all(|(c, _)| *c == value),
        "{name}: runs disagree"
    );
    let fastest = runs.iter().map(|(_, t)| *t).min().unwrap();
    let mean = runs.iter().map(|(_, t)| *t).sum::<Duration>() / repeat as u32;
    (value, fastest, mean)
}

/// The same count through Raphtory's windowed view, where it has one.
fn raphtory_count(graph: &Graph, counted: Counted, layer: &str, w: FfiWindow) -> Option<u64> {
    let range = w.range();
    let view = graph.layers(layer).ok()?.window(range.start, range.end);
    let count = match counted {
        Counted::ActiveNodes => view.count_nodes(),
        Counted::ActiveEdges => view.count_edges(),
        Counted::EdgeAdditions => view.count_temporal_edges(),
        Counted::NodeEvents => return None,
    };
    Some(count as u64)
}

fn main() {
    let args = parse_args().unwrap_or_else(|message| {
        eprintln!("{message}");
        exit(2);
    });
    let w = args.window;
    println!("window {:?}, layer {:?}", w.range(), args.layer);

    let (graph, load_time) = timed(|| Graph::load(args.graph_dir.as_path()));
    let graph = graph.unwrap_or_else(|err| {
        eprintln!("failed to load {}: {err}", args.graph_dir.display());
        exit(1);
    });
    println!("load      {load_time:>12.3?}");

    let core = graph.core_graph();
    let storage = core.mutable().expect("an unlocked disk graph").storage();
    let Some(layer) = storage.edge_meta().get_layer_id(&args.layer) else {
        let layers: Vec<_> = storage
            .edge_meta()
            .all_layer_iter()
            .map(|(_, name)| name)
            .collect();
        eprintln!("no layer {:?}; layers: {layers:?}", args.layer);
        exit(1);
    };

    let ((nodes, edges), lock_time) =
        timed(|| (storage.nodes().locked(), storage.edges().locked()));
    println!("lock      {lock_time:>12.3?}");
    let (prepared, prepare_time) = timed(|| PreparedGraph::new(&nodes, &edges));
    println!(
        "prepare   {prepare_time:>12.3?}   {} node segments, {} edge segments",
        prepared.num_node_segments(),
        prepared.num_edge_segments()
    );

    for counted in args.counts {
        let (expected, fastest, mean) = time_runs("storage", args.repeat, || {
            storage_count(counted, &nodes, &edges, layer, w)
        });
        let num_morsels = prepared
            .morsels(counted.scan(), layer.0, args.morsels)
            .len();
        println!(
            "\n{counted:?} = {expected}   ({num_morsels} morsels, {:?})",
            args.morsels
        );
        println!(
            "  storage          run {fastest:>12.3?} fastest, {mean:.3?} mean of {}",
            args.repeat
        );
        let baseline = fastest;

        for &backend in &args.backends {
            let (counter, compile_time) = timed(|| WindowCounter::compile_with(counted, backend));
            let counter = counter.unwrap_or_else(|err| {
                eprintln!("failed to compile {counted:?} with {backend:?}: {err:?}");
                exit(1);
            });
            let name = format!("{backend:?}");
            let (count, fastest, mean) = time_runs(&name, args.repeat, || {
                counter.count_with(&prepared, layer.0, w, args.morsels)
            });
            let speedup = baseline.as_secs_f64() / fastest.as_secs_f64();
            println!(
                "  {name:<16} run {fastest:>12.3?} fastest, {mean:.3?} mean, {speedup:.1}x storage; compile {compile_time:.3?}"
            );
            if count != expected {
                eprintln!("{counted:?} with {backend:?}: {count}, but storage says {expected}");
                exit(1);
            }
        }

        if args.check {
            let (raphtory, raphtory_time) =
                timed(|| raphtory_count(&graph, counted, &args.layer, w));
            match raphtory {
                Some(raphtory) => {
                    let verdict = if raphtory == expected {
                        "same"
                    } else {
                        "DIFFERENT"
                    };
                    println!(
                        "  raphtory view    run {raphtory_time:>12.3?}   {raphtory} ({verdict})"
                    );
                }
                None => println!("  raphtory view: no equivalent count"),
            }
        }
    }
}
