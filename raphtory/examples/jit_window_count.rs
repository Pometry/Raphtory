//! Window counts over a disk graph with JIT-compiled kernels (`relational/jit_stage0.md`).
//!
//! ```text
//! cargo run --release -p raphtory --features io,jit --example jit_window_count -- \
//!     <graph-dir> <start> <end> [--layer NAME] [--count WHAT] [--repeat N] [--check]
//! ```
//!
//! Counts, in one layer and the window `[start, end)`, any of `active-nodes`,
//! `node-events`, `active-edges`, `edge-additions` (default: all). Each count is a
//! compiled kernel run once per segment, segments in parallel. Prints how long loading,
//! locking, preparing, compiling and running took; `--check` also runs the same count
//! through Raphtory's windowed view and compares.

use raphtory::{prelude::*, storage::core_ops::CoreGraphOps};
use std::{
    path::PathBuf,
    process::exit,
    time::{Duration, Instant},
};
use storage::ffi::{Counted, FfiWindow, PreparedGraph, WindowCounter};

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
    repeat: usize,
    check: bool,
}

const USAGE: &str = "usage: jit_window_count <graph-dir> <start> <end> \
    [--layer NAME] [--count active-nodes|node-events|active-edges|edge-additions] \
    [--repeat N] [--check]";

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let mut positional = Vec::new();
    let mut layer = "_default".to_string();
    let mut counts = Vec::new();
    let mut repeat = 5;
    let mut check = false;

    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--layer" => layer = value()?,
            "--count" => counts.push(parse_counted(&value()?)?),
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
        window: FfiWindow {
            lo: parse_time("start", start)?,
            hi: parse_time("end", end)?,
        },
        layer,
        counts: if counts.is_empty() {
            Counted::ALL.to_vec()
        } else {
            counts
        },
        repeat: repeat.max(1),
        check,
    })
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

/// The same count through Raphtory's windowed view, where it has one.
fn raphtory_count(graph: &Graph, counted: Counted, layer: &str, w: FfiWindow) -> Option<u64> {
    let view = graph.layers(layer).ok()?.window(w.lo, w.hi);
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
    println!("window [{}, {}), layer {:?}", w.lo, w.hi, args.layer);

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
        let (counter, compile_time) = timed(|| WindowCounter::compile(counted));
        let counter = counter.unwrap_or_else(|err| {
            eprintln!("failed to compile {counted:?}: {err:?}");
            exit(1);
        });

        let runs: Vec<(u64, Duration)> = (0..args.repeat)
            .map(|_| timed(|| counter.count(&prepared, layer.0, w)))
            .collect();
        let count = runs[0].0;
        assert!(
            runs.iter().all(|(c, _)| *c == count),
            "{counted:?}: runs disagree"
        );
        let fastest = runs.iter().map(|(_, t)| *t).min().unwrap();
        let mean = runs.iter().map(|(_, t)| *t).sum::<Duration>() / runs.len() as u32;

        println!("\n{counted:?} = {count}");
        println!("  compile {compile_time:>12.3?}");
        println!(
            "  run     {fastest:>12.3?} fastest, {mean:.3?} mean of {}",
            args.repeat
        );

        if args.check {
            let (expected, raphtory_time) =
                timed(|| raphtory_count(&graph, counted, &args.layer, w));
            match expected {
                Some(expected) => {
                    let verdict = if expected == count { "ok" } else { "MISMATCH" };
                    println!("  raphtory {raphtory_time:>11.3?}   {expected} ({verdict})");
                    if expected != count {
                        exit(1);
                    }
                }
                None => println!("  raphtory: no equivalent view count"),
            }
        }
    }
}
