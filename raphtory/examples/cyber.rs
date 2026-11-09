//! The cyber queries (LANL-style host events and netflow) on a db4 graph: loads the
//! three tables into a graph, one layer each, then runs Q1–Q4 natively, through the
//! storage APIs `graph.core_graph()` reaches, and prints each count and time.
//!
//! Usage: `cyber <parquet_root> [graph_dir]`. `parquet_root` holds `v1_sorted/`,
//! `v2_sorted/` and `nft_sorted/` (directories of `*.parquet`); `graph_dir` defaults to
//! `<parquet_root>/../graph`. The graph is loaded from parquet when `graph_dir` doesn't
//! exist, and opened otherwise.
//!
//! The layers are `events_1v`, `events_2v` and `netflow`. Hosts are nodes, by their
//! `src`/`dst` names; every other column but the hashes is a temporal edge property,
//! at `epoch_time`.
//!
//! The queries follow their Cypher: `(a)-[..]->(a)` is a self-loop, checked. A
//! property's updates are those where it has a value.

use raphtory::{
    arrow_loader::df_loaders::edges::ColumnNames,
    errors::GraphError,
    io::parquet_loaders::load_edges_from_parquet,
    prelude::*,
    storage::{
        core_ops::CoreGraphOps,
        graph::{
            edges::{edge_storage_ops::EdgeStorageOps, edges::EdgesStorage},
            graph::GraphStorage,
        },
    },
};
use raphtory_api::core::entities::{
    properties::{prop::PropUnwrap, tprop::TPropOps},
    LayerId, LayerIds, VID,
};
use rayon::prelude::*;
use std::{
    path::{Path, PathBuf},
    time::Instant,
};
use storage::api::nodes::NodeRefOps;

#[cfg(target_os = "macos")]
use tikv_jemallocator::Jemalloc;

#[cfg(target_os = "macos")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const EVENTS_1V: &str = "events_1v";
const EVENTS_2V: &str = "events_2v";
const NETFLOW: &str = "netflow";

const BOOT: i64 = 4608;
const PROGRAM: i64 = 4688;
const PROXY_PORT: i64 = 3128;
/// `nf1.epochtime - boot.epochtime < WINDOW`.
const WINDOW: i64 = 4;
/// `nf2.duration >= SESSION`.
const SESSION: i64 = 3600;

// ── Loading ─────────────────────────────────────────────────────────────────

/// One table: its directory under the parquet root, its layer, and its property
/// columns (all but `epoch_time`, `src`, `dst` and their hashes).
struct Table {
    dir: &'static str,
    layer: &'static str,
    props: &'static [&'static str],
}

const TABLES: [Table; 3] = [
    Table {
        dir: "v1_sorted",
        layer: EVENTS_1V,
        props: &[
            "event_id",
            "user_name",
            "domain_name",
            "logon_id",
            "process_name",
            "process_id",
            "parent_process_name",
            "parent_process_id",
        ],
    },
    Table {
        dir: "v2_sorted",
        layer: EVENTS_2V,
        props: &[
            "event_id",
            "log_host",
            "logon_type",
            "logon_type_description",
            "user_name",
            "domain_name",
            "logon_id",
            "subject_user_name",
            "subject_domain_name",
            "subject_logon_id",
            "status",
            "service_name",
            "authentication_package",
            "failure_reason",
            "process_name",
            "process_id",
            "parent_process_name",
            "parent_process_id",
        ],
    },
    Table {
        dir: "nft_sorted",
        layer: NETFLOW,
        props: &[
            "duration",
            "protocol",
            "src_port",
            "dst_port",
            "src_packets",
            "dst_packets",
            "src_bytes",
            "dst_bytes",
        ],
    },
];

/// Loads the three tables into `graph`, a layer each, flushing after each.
fn load_cyber_graph(parquet_root: &Path, graph: &Graph) -> Result<(), GraphError> {
    for table in &TABLES {
        let now = Instant::now();
        load_edges_from_parquet(
            graph,
            parquet_root.join(table.dir),
            ColumnNames::new("epoch_time", None, "src", "dst", None),
            true,
            table.props,
            &[],
            None,
            Some(table.layer),
            None,
            None,
        )?;
        graph.flush()?;
        println!("  ✓ {} ({:?})", table.layer, now.elapsed());
    }
    println!(
        "\n✅ Graph loaded: {} nodes, {} edges",
        graph.count_nodes(),
        graph.count_edges()
    );
    Ok(())
}

// ── Queries ─────────────────────────────────────────────────────────────────

/// The locked graph and the ids the queries read.
struct Cyber {
    core: GraphStorage,
    edges: EdgesStorage,
    events: LayerId,
    events_only: LayerIds,
    netflow: LayerId,
    event_id: usize,
    src_port: usize,
    duration: usize,
}

type Window = std::ops::Range<i64>;
const ALL: Window = i64::MIN..i64::MAX;

/// The times of the updates of `updates` with value `id`, in `w`, in time order.
fn times_of<'a>(
    updates: impl TPropOps<'a>,
    id: i64,
    w: Window,
) -> impl Iterator<Item = i64> + 'a {
    updates
        .iter_window_t(w)
        .filter(move |(_, v)| v.clone().into_i64() == Some(id))
        .map(|(t, _)| t)
}

impl Cyber {
    fn new(graph: &Graph) -> Self {
        let core = graph.core_graph().lock();
        let meta = core.edge_meta();
        let layer = |name| {
            meta.get_layer_id(name)
                .unwrap_or_else(|| panic!("no layer {name}"))
        };
        let prop = |name| {
            meta.temporal_prop_mapper()
                .get_id(name)
                .unwrap_or_else(|| panic!("no edge property {name}"))
        };
        let events = layer(EVENTS_1V);
        Self {
            events,
            events_only: LayerIds::One(events),
            netflow: layer(NETFLOW),
            event_id: prop("event_id"),
            src_port: prop("src_port"),
            duration: prop("duration"),
            edges: core.owned_edges(),
            core,
        }
    }

    /// The source port-3128 flows into `a` from another host `b` in `w`: (b, nf1.t).
    fn proxy_flows(&self, a: VID, w: Window) -> Vec<(VID, i64)> {
        let mut flows = Vec::new();
        for (b, eid) in self.core.nodes().node(a).inb_edges(self.netflow) {
            if b == a {
                continue;
            }
            let edge = self.edges.edge(either::Either::Left(eid));
            let ports = edge.temporal_prop_layer(self.netflow, self.src_port);
            flows.extend(times_of(ports, PROXY_PORT, w.clone()).map(|t| (b, t)));
        }
        flows
    }

    /// Q1: the boots of every self-loop.
    fn q1(&self) -> u64 {
        self.edges
            .par_iter(&self.events_only)
            .filter(|e| e.src() == e.dst())
            .map(|e| {
                let event_id = e.temporal_prop_layer(self.events, self.event_id);
                times_of(event_id, BOOT, ALL).count() as u64
            })
            .sum()
    }

    /// Q2: (boot, program) pairs of a self-loop, `program.t` in `[boot.t, boot.t + WINDOW)`.
    fn q2(&self) -> u64 {
        self.edges
            .par_iter(&self.events_only)
            .filter(|e| e.src() == e.dst())
            .map(|e| {
                let event_id = e.temporal_prop_layer(self.events, self.event_id);
                times_of(event_id.clone(), BOOT, ALL)
                    .map(|t| times_of(event_id.clone(), PROGRAM, t..t + WINDOW).count() as u64)
                    .sum::<u64>()
            })
            .sum()
    }

    /// Q3's matches by `(b, a, nf1.t)`: how many (boot, program) pairs each proxy flow
    /// `nf1` from `b` into `a` closes, `boot.t <= program.t <= nf1.t < boot.t + WINDOW`.
    fn q3_matches(&self) -> Vec<((VID, VID, i64), u64)> {
        self.edges
            .par_iter(&self.events_only)
            .filter(|e| e.src() == e.dst())
            .flat_map_iter(|e| {
                let a = e.src();
                let event_id = e.temporal_prop_layer(self.events, self.event_id);
                let programs = event_id.clone();
                times_of(event_id, BOOT, ALL)
                    .flat_map(move |boot| {
                        self.proxy_flows(a, boot..boot + WINDOW)
                            .into_iter()
                            .map(move |(b, nf1)| (boot, b, nf1))
                    })
                    .filter_map(move |(boot, b, nf1)| {
                        let programs =
                            times_of(programs.clone(), PROGRAM, boot..nf1 + 1).count() as u64;
                        (programs > 0).then_some(((b, a, nf1), programs))
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Q3: (boot, program, nf1) matches.
    fn q3(&self) -> u64 {
        self.q3_matches().iter().map(|(_, n)| n).sum()
    }

    /// Q4, grouped as the old `run2`: Q3's matches counted per `(b, a, nf1.t)`, then
    /// each group times the sessions from `b` that are open at `nf1.t`.
    fn q4(&self) -> u64 {
        let mut matches = self.q3_matches();
        matches.par_sort_unstable_by_key(|(key, _)| *key);
        let mut groups: Vec<((VID, VID, i64), u64)> = Vec::with_capacity(matches.len());
        for (key, n) in matches {
            match groups.last_mut() {
                Some((last, total)) if *last == key => *total += n,
                _ => groups.push((key, n)),
            }
        }
        groups
            .par_iter()
            .map(|&((b, a, nf1), n)| n * self.sessions(b, a, nf1))
            .sum()
    }

    /// The sessions `nf2` from `b` to a host `c` other than `a` and `b`, at least
    /// `SESSION` long, started before `nf1` and lasting until it.
    fn sessions(&self, b: VID, a: VID, nf1: i64) -> u64 {
        let mut count = 0;
        for (c, eid) in self.core.nodes().node(b).out_edges(self.netflow) {
            if c == a || c == b {
                continue;
            }
            let edge = self.edges.edge(either::Either::Left(eid));
            let durations = edge.temporal_prop_layer(self.netflow, self.duration);
            count += durations
                .iter_window_t(i64::MIN..nf1)
                .filter(|(t, d)| d.clone().into_i64().is_some_and(|d| d >= SESSION && t + d >= nf1))
                .count() as u64;
        }
        count
    }
}

fn timed(name: &str, query: impl FnOnce() -> u64) {
    let now = Instant::now();
    let count = query();
    println!("{name}: {count} ({:?})", now.elapsed());
}

fn main() {
    let parquet_root = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("Usage: cyber <parquet_root> [graph_dir]"));
    let graph_path = std::env::args()
        .nth(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| parquet_root.join("..").join("graph"));
    let now = Instant::now();
    let graph = if graph_path.exists() {
        let graph = Graph::load(&graph_path).unwrap();
        println!(
            "Graph loaded from {graph_path:?}: {} nodes, {} edges ({:?})",
            graph.count_nodes(),
            graph.count_edges(),
            now.elapsed()
        );
        graph
    } else {
        let graph = Graph::new_at_path(&graph_path).unwrap();
        load_cyber_graph(&parquet_root, &graph).unwrap();
        println!("Time taken: {:?}", now.elapsed());
        graph
    };

    let cyber = Cyber::new(&graph);
    timed("Q1", || cyber.q1());
    timed("Q2", || cyber.q2());
    timed("Q3", || cyber.q3());
    timed("Q4", || cyber.q4());
}

#[cfg(test)]
mod tests {
    //! Q1–Q4 against counts from the updates as added, on small random graphs like
    //! `integration-tests/src/tests/jit_cyber.rs`'s. Times are 0..30, so durations are
    //! drawn around `SESSION`: some sessions are too short, some long enough.
    use super::*;

    /// A seeded splitmix64.
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            (z ^ (z >> 31)) % n
        }
    }

    struct Update {
        src: u64,
        dst: u64,
        layer: &'static str,
        t: i64,
        event_id: Option<i64>,
        src_port: Option<i64>,
        duration: Option<i64>,
    }

    fn graph(seed: u64, dir: &Path) -> (Graph, Vec<Update>) {
        let mut rng = Rng(seed);
        let hosts = 8u64;
        let mut pairs: Vec<(&'static str, u64, u64)> =
            (0..hosts).map(|h| (EVENTS_1V, h, h)).collect();
        for _ in 0..4 {
            let (a, b) = (rng.below(hosts), rng.below(hosts));
            pairs.push((EVENTS_1V, a, b));
            pairs.push((NETFLOW, a, b));
        }
        for _ in 0..8 {
            pairs.push((NETFLOW, rng.below(hosts), rng.below(hosts)));
        }
        let mut updates = Vec::new();
        for &(layer, src, dst) in &pairs {
            let base = rng.below(20) as i64;
            for _ in 0..rng.below(12) {
                let t = base + rng.below(10) as i64;
                let ids = [BOOT, PROGRAM, 4624, 1];
                let event_id = (rng.below(8) != 0).then(|| ids[rng.below(4) as usize]);
                let ports = [PROXY_PORT, 80, 443];
                let src_port =
                    (layer == NETFLOW && rng.below(6) != 0).then(|| ports[rng.below(3) as usize]);
                // Around `SESSION`, so some sessions are long enough and some last
                // until a later flow.
                let duration = (layer == NETFLOW && rng.below(3) != 0)
                    .then(|| SESSION - 5 + rng.below(10) as i64);
                updates.push(Update {
                    src,
                    dst,
                    layer,
                    t,
                    event_id,
                    src_port,
                    duration,
                });
            }
        }
        let graph = Graph::new_at_path(dir).unwrap();
        for (i, u) in updates.iter().enumerate() {
            let mut props = match u.event_id {
                Some(id) => vec![("event_id", Prop::I64(id))],
                None => vec![("other", Prop::I64(0))],
            };
            props.extend(u.src_port.map(|p| ("src_port", Prop::I64(p))));
            props.extend(u.duration.map(|d| ("duration", Prop::I64(d))));
            graph
                .add_edge(u.t, u.src, u.dst, props, Some(u.layer))
                .unwrap();
            if i == updates.len() / 2 {
                graph.flush().unwrap();
            }
        }
        graph.flush().unwrap();
        drop(graph);
        (Graph::load(dir).unwrap(), updates)
    }

    /// Q3's matches, each weighted by `weight(boot, nf1)`; `weight` 1 is Q3.
    fn q3_expected(updates: &[Update], weight: impl Fn(&Update, &Update) -> u64) -> u64 {
        let boots = updates.iter().filter(|u| {
            u.layer == EVENTS_1V && u.src == u.dst && u.event_id == Some(BOOT)
        });
        boots
            .map(|boot| {
                let a = boot.src;
                updates
                    .iter()
                    .filter(|nf1| {
                        nf1.layer == NETFLOW
                            && nf1.dst == a
                            && nf1.src != a
                            && nf1.src_port == Some(PROXY_PORT)
                            && (boot.t..boot.t + WINDOW).contains(&nf1.t)
                    })
                    .map(|nf1| {
                        let programs = updates
                            .iter()
                            .filter(|p| {
                                (p.src, p.dst, p.layer) == (a, a, EVENTS_1V)
                                    && p.event_id == Some(PROGRAM)
                                    && (boot.t..=nf1.t).contains(&p.t)
                            })
                            .count() as u64;
                        programs * weight(boot, nf1)
                    })
                    .sum::<u64>()
            })
            .sum()
    }

    #[test]
    fn queries_count_what_the_updates_hold() {
        let mut totals = [0u64; 4];
        for seed in 0..32 {
            let dir = tempfile::tempdir().unwrap();
            let (graph, updates) = graph(seed, dir.path());
            let cyber = Cyber::new(&graph);
            let boots: Vec<&Update> = updates
                .iter()
                .filter(|u| u.layer == EVENTS_1V && u.src == u.dst && u.event_id == Some(BOOT))
                .collect();
            let q1 = boots.len() as u64;
            let q2 = boots
                .iter()
                .map(|boot| {
                    updates
                        .iter()
                        .filter(|p| {
                            (p.src, p.dst, p.layer) == (boot.src, boot.dst, EVENTS_1V)
                                && p.event_id == Some(PROGRAM)
                                && (boot.t..boot.t + WINDOW).contains(&p.t)
                        })
                        .count() as u64
                })
                .sum();
            let q3 = q3_expected(&updates, |_, _| 1);
            let q4 = q3_expected(&updates, |boot, nf1| {
                let (a, b) = (boot.src, nf1.src);
                updates
                    .iter()
                    .filter(|nf2| {
                        nf2.layer == NETFLOW
                            && nf2.src == b
                            && nf2.dst != a
                            && nf2.dst != b
                            && nf2.t < nf1.t
                            && nf2
                                .duration
                                .is_some_and(|d| d >= SESSION && nf2.t + d >= nf1.t)
                    })
                    .count() as u64
            });
            let expected = [q1, q2, q3, q4];
            let got = [cyber.q1(), cyber.q2(), cyber.q3(), cyber.q4()];
            assert_eq!(got, expected, "seed {seed}");
            for (total, n) in totals.iter_mut().zip(expected) {
                *total += n;
            }
        }
        assert!(totals.iter().all(|&n| n > 0), "every query has matches: {totals:?}");
    }
}
