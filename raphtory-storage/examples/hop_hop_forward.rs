use ahash::{HashMap, HashSet};
use dashmap::DashMap;
use itertools::Itertools;
use raphtory::{
    arrow::{graph::TemporalGraph, loader::ExternalEdgeList, prelude::*, Time},
    core::{entities::VID, Direction},
};
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelIterator},
    prelude::{IntoParallelRefIterator, ParallelIterator},
    slice::ParallelSlice,
    ThreadPoolBuilder,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

fn query1(g: &TemporalGraph) -> Option<usize> {
    // layer
    let nft = g.find_layer_id("netflow")?;
    let events_1v = g.find_layer_id("events_1v")?;
    let events_2v = g.find_layer_id("events_2v")?;

    // properties
    let bytes_prop_id = g.edge_property_id("dst_bytes", nft)?;
    let event_id_prop_id_2v = g.edge_property_id("event_id", events_2v)?;
    let event_id_prop_id_1v = g.edge_property_id("event_id", events_1v)?;

    let res = query1_v5(
        g,
        nft,
        events_2v,
        events_1v,
        bytes_prop_id,
        event_id_prop_id_1v,
        event_id_prop_id_2v,
    );
    println!("vertex count: {:?}", g.num_vertices());
    Some(res)
}

fn query1_v5(
    g: &TemporalGraph,
    nft: usize,
    events_2v: usize,
    events_1v: usize,
    bytes_prop_id: usize,
    event_id_prop_id_1v: usize,
    event_id_prop_id_2v: usize,
) -> usize {
    let pool = ThreadPoolBuilder::new()
        .build()
        .expect("failed to build pool");

    let count = pool.install(|| {
        let now = Instant::now();

        let b_prog1_filter: HashSet<VID> = {
            g.all_edges_par(events_1v)
                .filter(|edge| {
                    edge.par_prop_items::<i64>(event_id_prop_id_1v)
                        .unwrap()
                        .any(|(_, event_id)| event_id == Some(4688))
                })
                .map(|e| e.dst())
                .collect()
        };

        println!(
            "b_prog1_filter: {:?}, count: {}",
            now.elapsed(),
            b_prog1_filter.len()
        );

        let now = Instant::now();
        let b_login1_filter: HashSet<VID> = {
            g.all_edges_par(events_2v)
                .filter(|edge| b_prog1_filter.contains(&edge.dst()))
                .filter(|edge| {
                    edge.par_prop_items::<i64>(event_id_prop_id_2v)
                        .unwrap()
                        .any(|(_, event_id)| event_id == Some(4624))
                })
                .map(|e| e.dst())
                .collect()
        };
        println!(
            "b_login1_filter: {:?}, count: {}",
            now.elapsed(),
            b_login1_filter.len()
        );

        // let mut count = 0;

        let count = AtomicUsize::new(0);
        let vs = b_login1_filter.iter().copied().collect_vec();
        let probe_map: Arc<DashMap<VID, Vec<(i32, i32, u32)>>> = Arc::new(DashMap::default());

        let now = Instant::now();
        pool.install(|| {
            vs.into_par_iter().for_each(|b| {
                g.edges_par(b, Direction::OUT, nft).for_each(|(b2e, e)| {
                    if e != b {
                        let nf1 = g.edge(b2e, nft);

                        nf1.par_prop_items::<i64>(bytes_prop_id)
                            .unwrap()
                            .filter(|(_, v)| v > &Some(100_000_000))
                            .for_each(|(nf1_t, _)| {
                                g.edges_par(b, Direction::OUT, events_1v)
                                    .filter(|(_, v)| {
                                        v == &b
                                            // && b_login1_filter.contains(v)
                                            && b_prog1_filter.contains(v)
                                    })
                                    .for_each(|(b2b, _)| {
                                        let prog1 = g.edge(b2b, events_1v);

                                        prog1
                                            .par_prop_items::<i64>(event_id_prop_id_1v)
                                            .unwrap()
                                            .filter_map(|(t, v)| v.map(|v| (v, t)))
                                            .for_each(|(prog1_event_id, prog1_t)| {
                                                if prog1_event_id == 4688
                                                    && prog1_t < nf1_t
                                                    && nf1_t - prog1_t <= 30
                                                {
                                                    let e_small: u32 =
                                                        Into::<usize>::into(e) as u32;
                                                    probe_map
                                                        .entry(b)
                                                        .and_modify(|v| {
                                                            v.push((
                                                                prog1_t as i32,
                                                                nf1_t as i32,
                                                                e_small,
                                                            ))
                                                        })
                                                        .or_insert_with(|| {
                                                            vec![(
                                                                prog1_t as i32,
                                                                nf1_t as i32,
                                                                e_small,
                                                            )]
                                                        });
                                                }
                                            });
                                    });
                            });
                    }
                });
            });
        });

        println!("Done probe map build: {:?}", now.elapsed());

        let now = Instant::now();

        probe_map.iter_mut().for_each(|mut entry| {
            entry.value_mut().sort();
        });

        println!(
            "Done sorting map len: {:?}, duration: {:?}",
            probe_map.len(),
            now.elapsed()
        );

        let now: Instant = Instant::now();

        pool.install(|| {
            probe_map.par_iter().for_each(|entry| {
                let b: VID = *entry.key();
                let edges = entry.value();

                g.edges_par(b, Direction::IN, events_2v)
                    .for_each(|(a2b, a)| {
                        if a != b {
                            let login1 = g.edge(a2b, events_2v);

                            let probe_value = &edges;

                            let mut skip = false;
                            if !probe_value.is_empty() {
                                let max_prog1 = probe_value.last().unwrap().0 as i64;

                                let login1_ts = login1.timestamps();

                                let min_login1 = login1_ts.iter().flatten().next().unwrap();
                                if min_login1 > max_prog1 {
                                    skip = true;
                                }
                            }

                            if !skip {
                                let iter = login1
                                    .par_prop_items::<i64>(event_id_prop_id_2v)
                                    .unwrap()
                                    .filter_map(|(t, v)| v.map(|v| (v, t)));

                                binary_search_join_par_small_2(iter, &edges, &a, &count);
                            }
                        }
                    });
            })
        });

        println!("Done 3rd join duration: {:?}", now.elapsed());

        count.load(Ordering::Relaxed)
    });

    count
}

fn query1_v4(
    g: &TemporalGraph,
    nft: usize,
    events_2v: usize,
    events_1v: usize,
    bytes_prop_id: usize,
    event_id_prop_id_1v: usize,
    event_id_prop_id_2v: usize,
) -> usize {
    let c1 = 0usize;
    let mut count = 0;
    let mut dream_map: HashMap<VID, Vec<(i64, i64, VID)>> = HashMap::default();

    for b in g.all_vertices() {
        for (b2e, e) in g.edges(b, Direction::OUT, nft) {
            if e != b {
                let nf1 = g.edge(b2e, nft);

                for (nf1_t, _) in nf1
                    .prop_items::<i64>(bytes_prop_id)
                    .unwrap()
                    .filter(|(_, v)| v.as_ref().filter(|&&v| v > 100_000_000).is_some())
                {
                    for (b2b, _) in g
                        .edges(b, Direction::OUT, events_1v)
                        .filter(|(_, v)| v == &b)
                    {
                        let prog1 = g.edge(b2b, events_1v);

                        for (prog1_event_id, prog1_t) in prog1
                            .prop_items::<i64>(event_id_prop_id_1v)
                            .unwrap()
                            .filter_map(|(t, v)| v.map(|v| (v, t)))
                        {
                            if prog1_event_id == 4688 && prog1_t < nf1_t && nf1_t - prog1_t <= 30 {
                                dream_map
                                    .entry(b)
                                    .and_modify(|v| v.push((prog1_t, nf1_t, e)))
                                    .or_insert_with(|| vec![(prog1_t, nf1_t, e)]);
                            }
                        }
                    }
                }
            }
        }
    }

    dream_map.values_mut().for_each(|v| {
        v.sort();
    });

    for (b, edges) in dream_map {
        for (a2b, a) in g.edges(b, Direction::IN, events_2v) {
            if a != b {
                let login1 = g.edge(a2b, events_2v);

                let probe_value = &edges;

                if !probe_value.is_empty() {
                    let max_prog1 = probe_value.last().unwrap().0;

                    let login1_ts = login1.timestamps();
                    let min_login1 = login1_ts.iter().flatten().next().unwrap();
                    if min_login1 > max_prog1 {
                        continue;
                    }
                }

                let iter = login1
                    .prop_items::<i64>(event_id_prop_id_2v)
                    .unwrap()
                    .filter_map(|(t, v)| v.map(|v| (v, t)));

                binary_search_join(iter, &edges, &a, &mut count);
            }
        }
    }
    println!("c1: {:?}", c1);
    count
}

fn query1_v4_par(
    g: &TemporalGraph,
    nft: usize,
    events_2v: usize,
    events_1v: usize,
    bytes_prop_id: usize,
    event_id_prop_id_1v: usize,
    event_id_prop_id_2v: usize,
) -> usize {
    // let mut count = 0;
    let probe_map: Arc<DashMap<VID, Vec<(i32, i32, u32)>>> = Arc::new(DashMap::default());

    let count = AtomicUsize::new(0);
    let vs = (0..g.num_vertices()).into_iter().collect_vec();

    let pool: rayon::ThreadPool = ThreadPoolBuilder::new()
        .num_threads(24)
        .build()
        .expect("failed to build pool");

    let now = Instant::now();
    pool.install(|| {
        vs.par_chunks(1024).for_each(|b_ids| {
            for b_id in b_ids {
                let b = VID(*b_id);
                g.edges_par(b, Direction::OUT, nft).for_each(|(b2e, e)| {
                    if e != b {
                        let nf1 = g.edge(b2e, nft);

                        for (nf1_t, _) in nf1
                            .prop_items::<i64>(bytes_prop_id)
                            .unwrap()
                            .filter(|(_, v)| v.as_ref().filter(|&&v| v > 100_000_000).is_some())
                        {
                            g.edges_par(b, Direction::OUT, events_1v)
                                .filter(|(_, v)| v == &b)
                                .for_each(|(b2b, _)| {
                                    let prog1 = g.edge(b2b, events_1v);

                                    // let ts = prog1.timestamps()
                                    for (prog1_event_id, prog1_t) in prog1
                                        .prop_items::<i64>(event_id_prop_id_1v)
                                        .unwrap()
                                        .filter_map(|(t, v)| v.map(|v| (v, t)))
                                    {
                                        if prog1_event_id == 4688
                                            && prog1_t < nf1_t
                                            && nf1_t - prog1_t <= 30
                                        {
                                            let e_small: u32 = Into::<usize>::into(e) as u32;
                                            probe_map
                                                .entry(b)
                                                .and_modify(|v| {
                                                    v.push((prog1_t as i32, nf1_t as i32, e_small))
                                                })
                                                .or_insert_with(|| {
                                                    vec![(prog1_t as i32, nf1_t as i32, e_small)]
                                                });
                                        }
                                    }
                                });
                        }
                    }
                });
            }
        });
    });

    println!("Done probe map build: {:?}", now.elapsed());

    let now = Instant::now();

    probe_map.iter_mut().for_each(|mut entry| {
        entry.value_mut().sort();
    });

    println!(
        "Done sorting map len: {:?}, duration: {:?}",
        probe_map.len(),
        now.elapsed()
    );

    let min_edge_len = probe_map
        .iter()
        .map(|entry| entry.value().len())
        .min()
        .unwrap();
    let max_edge_len = probe_map
        .iter()
        .map(|entry| entry.value().len())
        .max()
        .unwrap();
    let avg_edge_len = probe_map
        .iter()
        .map(|entry| entry.value().len())
        .sum::<usize>() as f64
        / probe_map.len() as f64;
    let one_len = probe_map
        .iter()
        .filter(|entry| entry.value().len() == 1)
        .count();

    let median = probe_map
        .iter()
        .map(|entry| entry.value().len())
        .sorted()
        .nth(probe_map.len() / 2)
        .unwrap();

    println!(
        "min_edge_len: {:?}, max_edge_len: {:?}, avg_edge_len: {:?}, one_len: {:?}, median: {:?}",
        min_edge_len, max_edge_len, avg_edge_len, one_len, median
    );

    let now: Instant = Instant::now();

    pool.install(|| {
        probe_map.par_iter().for_each(|entry| {
            let b = *entry.key();
            let edges = entry.value();
            g.edges_par(b, Direction::IN, events_2v)
                .for_each(|(a2b, a)| {
                    if a != b {
                        let login1 = g.edge(a2b, events_2v);

                        let probe_value = &edges;

                        let mut skip = false;
                        if !probe_value.is_empty() {
                            let max_prog1 = probe_value.last().unwrap().0 as i64;
                            // let min_prog1 = probe_value.first().unwrap().0;

                            let login1_ts = login1.timestamps();
                            let min_login1 = login1_ts.iter().flatten().next().unwrap();
                            if min_login1 > max_prog1 {
                                skip = true;
                            }
                        }

                        if !skip {
                            let iter = login1
                                .prop_items::<i64>(event_id_prop_id_2v)
                                .unwrap()
                                .filter_map(|(t, v)| v.map(|v| (v, t)));

                            binary_search_join_par_small(iter, &edges, &a, &count);
                        }
                    }
                });
        })
    });

    println!("Done 3rd join duration: {:?}", now.elapsed());

    count.load(Ordering::Relaxed)
}

fn binary_search_join_par_small<'a>(
    iter: impl IntoIterator<Item = (Time, Time)>,
    edges: &'a Vec<(i32, i32, u32)>,
    a: &VID,
    count: &'a AtomicUsize,
) -> Vec<(&'a Time, &'a Time, &'a Time)> {
    let out = vec![]; // use this if we have to output the data

    'outer: for (login1_event_id, login1_t) in iter {
        if login1_event_id == 4624 {
            let login1_t_small: i32 = login1_t as i32;

            let pos = edges.binary_search_by(|probe| probe.0.cmp(&login1_t_small));

            let from = match pos {
                Ok(i) => {
                    i + 1 // this one is smaller than all the prog_t
                }
                Err(i) => {
                    if i >= edges.len() {
                        break 'outer;
                    } else {
                        i
                    }
                }
            };

            for (prog1_t, nft_t, e) in &edges[from..] {
                let e_vid = VID(*e as usize);
                if nft_t - login1_t_small <= 30 && &login1_t_small < prog1_t && a != &e_vid {
                    count.fetch_add(1, Ordering::Relaxed);
                    // out.push((login1_t, prog1_t, nft1_1));
                }
            }
        }
    }
    out
}

fn binary_search_join_par<'a>(
    iter: impl IntoIterator<Item = (&'a Time, &'a Time)>,
    edges: &'a Vec<(Time, Time, VID)>,
    a: &VID,
    count: &'a AtomicUsize,
) -> Vec<(&'a Time, &'a Time, &'a Time)> {
    let out = vec![]; // use this if we have to output the data

    'outer: for (login1_event_id, login1_t) in iter {
        if *login1_event_id == 4624 {
            let pos = edges.binary_search_by(|probe| probe.0.cmp(login1_t));

            let from = match pos {
                Ok(i) => {
                    i + 1 // this one is smaller than all the prog_t
                }
                Err(i) => {
                    if i >= edges.len() {
                        break 'outer;
                    } else {
                        i
                    }
                }
            };

            for (prog1_t, nft_t, e) in &edges[from..] {
                if nft_t - login1_t <= 30 && login1_t < prog1_t && a != e {
                    count.fetch_add(1, Ordering::Relaxed);
                    // out.push((login1_t, prog1_t, nft1_1));
                }
            }
        }
    }
    out
}

fn binary_search_join_par_small_2<'a>(
    iter: impl ParallelIterator<Item = (Time, Time)>,
    edges: &'a Vec<(i32, i32, u32)>,
    a: &VID,
    count: &'a AtomicUsize,
) {
    let c = iter
        .filter(|(login1_event_id, _)| login1_event_id == &4624)
        .filter_map(|(_, login1_t)| {
            let login1_t_small: i32 = login1_t as i32;

            let pos = edges.binary_search_by(|probe| probe.0.cmp(&login1_t_small));

            let from = match pos {
                Ok(i) => {
                    Some(i + 1) // this one is smaller than all the prog_t
                }
                Err(i) => {
                    if i >= edges.len() {
                        None
                    } else {
                        Some(i)
                    }
                }
            };
            from.map(|from| (from, login1_t_small))
        })
        .flat_map_iter(|(from, login1_t_small)| {
            (&edges[from..]).iter().filter(move |(prog1_t, nft_t, e)| {
                let e_vid = VID(*e as usize);
                nft_t - login1_t_small <= 30 && &login1_t_small < prog1_t && a != &e_vid
            })
        })
        .count();
    // .sum::<usize>();

    count.fetch_add(c, Ordering::Relaxed);
}

fn binary_search_join<'a>(
    iter: impl IntoIterator<Item = (Time, Time)>,
    edges: &'a Vec<(Time, Time, VID)>,
    a: &VID,
    count: &'a mut usize,
) -> Vec<(&'a Time, &'a Time, &'a Time)> {
    let out = vec![];

    'outer: for (login1_t, login1_event_id) in iter {
        if login1_event_id == 4624 {
            let pos = edges.binary_search_by(|probe| probe.0.cmp(&login1_t));

            let from = match pos {
                Ok(i) => {
                    i + 1 // this one is smaller than all the prog_t
                }
                Err(i) => {
                    if i >= edges.len() {
                        break 'outer;
                    } else {
                        i
                    }
                }
            };

            for (prog1_t, nft_t, e) in &edges[from..] {
                if nft_t - login1_t <= 30 && &login1_t < prog1_t && a != e {
                    *count += 1;
                    // out.push((login1_t, prog1_t, nft1_1));
                }
            }
        }
    }
    out
}

fn main() {
    let graph_dir = std::env::args()
        .nth(1)
        .expect("please supply a graph directory");

    let netflow_dir = std::env::args()
        .nth(2)
        .expect("please supply a wls directory");

    let v1_dir = std::env::args()
        .nth(3)
        .expect("please supply a v1 directory");

    let v2_dir = std::env::args()
        .nth(4)
        .expect("please supply a v2 directory");

    println!("graph_dir: {:?}", graph_dir);
    println!("netflow_dir: {:?}", netflow_dir);
    println!("v1_dir: {:?}", v1_dir);
    println!("v2_dir: {:?}", v2_dir);

    let now = Instant::now();
    let graph = if std::fs::read_dir(&graph_dir).is_ok() {
        TemporalGraph::new(&graph_dir).expect("failed to load graph")
    } else {
        let layered_edge_list = [
            ExternalEdgeList::new(
                "netflow",
                netflow_dir,
                "src",
                "src_hash",
                "dst",
                "dst_hash",
                "epoch_time",
            )
            .expect("failed to load netflow"),
            ExternalEdgeList::new(
                "events_1v",
                v1_dir,
                "src",
                "src_hash",
                "dst",
                "dst_hash",
                "epoch_time",
            )
            .expect("failed to load events_v1"),
            ExternalEdgeList::new(
                "events_2v",
                v2_dir,
                "src",
                "src_hash",
                "dst",
                "dst_hash",
                "epoch_time",
            )
            .expect("failed to load events_v2"),
        ];
        let chunk_size = 8_388_608;
        let t_props_chunk_size = 20_970_100;
        let graph = TemporalGraph::from_edge_lists(
            8,
            chunk_size,
            chunk_size,
            t_props_chunk_size,
            graph_dir,
            layered_edge_list,
        )
        .expect("failed to load graph");
        graph
    };
    println!("Time taken to load graph: {:?}", now.elapsed());

    let now = Instant::now();

    //     MATCH
    //     (E)<-[nf1:Netflow]-(B)<-[login1:Events2v]-(A), (B)<-[prog1:Events1v]-(B)
    //   WHERE A <> B AND B <> E AND A <> E
    //     AND login1.eventID = 4624
    //     AND prog1.eventID = 4688
    //     AND nf1.dstBytes > 100000000
    //     // time constraints within each path
    //     AND login1.epochtime < prog1.epochtime
    //     AND prog1.epochtime < nf1.epochtime
    //     AND nf1.epochtime - login1.epochtime <= 30
    //   RETURN count(*)

    // Launched job 7
    // Number of answers: 2,992,551
    // CPU times: user 25.9 ms, sys: 11.7 ms, total: 37.6 ms
    // Wall time: 21.8 s

    // Devices (vertices): 159,245
    // Netflow (edges): 317,164,045
    // Host event 1-vertex (edges): 33,480,483
    // Host event 2-vertex (edges): 97,716,529
    // Total (edges): 448,361,057

    let count = query1(&graph);

    println!("Time taken: {:?}, count: {:?}", now.elapsed(), count);
}
