use ahash::HashMap;
use arrow2::{
    array::{Array, PrimitiveArray},
    chunk::{self, Chunk},
    compute::sort::{SortColumn, SortOptions},
    datatypes::{Field, Schema},
};
use dashmap::DashMap;
use itertools::Itertools;
use raphtory::{
    arrow::{
        graph::TemporalGraph,
        loader::ExternalEdgeList,
        mmap::{mmap_batch, write_batches},
        prelude::*,
        Time,
    },
    core::{entities::VID, Direction},
};
use rayon::{
    prelude::{IntoParallelRefIterator, ParallelIterator},
    slice::ParallelSlice,
    ThreadPoolBuilder,
};
use std::{
    path::Path,
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

    let res = query1_v4_par(
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

fn query1_v3_par(
    g: &TemporalGraph,
    nft: usize,
    events_2v: usize,
    events_1v: usize,
    bytes_prop_id: usize,
    event_id_prop_id_1v: usize,
    event_id_prop_id_2v: usize,
) -> usize {
    let count = AtomicUsize::new(0);
    let vs = (0..g.num_vertices()).into_iter().collect_vec();

    let pool = ThreadPoolBuilder::new()
        .num_threads(47)
        .build()
        .expect("failed to build pool");

    pool.install(|| {
        vs.par_chunks(2048).for_each(|b_ids| {
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
                            g.edges(b, Direction::OUT, events_1v)
                                .filter(|(_, v)| v == &b)
                                .for_each(|(b2b, _)| {
                                    let prog1 = g.edge(b2b, events_1v);

                                    for (prog1_event_id, prog1_t) in prog1
                                        .prop_items::<i64>(event_id_prop_id_1v)
                                        .unwrap()
                                        .filter_map(|(t, v)| v.map(|v| (v, t)))
                                    {
                                        if prog1_event_id == 4688
                                            && prog1_t < nf1_t
                                            && nf1_t - prog1_t <= 30
                                        // AND prog1.epochtime < nf1.epochtime
                                        {
                                            g.edges_par(b, Direction::IN, events_2v).for_each(
                                                |(a2b, a)| {
                                                    if a != e && a != b {
                                                        let login1 = g.edge(a2b, events_2v);
                                                        for (login1_event_id, login1_t) in login1
                                                            .prop_items::<i64>(event_id_prop_id_2v)
                                                            .unwrap()
                                                            .filter_map(|(t, v)| v.map(|v| (v, t)))
                                                        {
                                                            if login1_event_id == 4624
                                                                && nf1_t - login1_t <= 30
                                                                && login1_t < prog1_t
                                                            {
                                                                count.fetch_add(
                                                                    1,
                                                                    Ordering::Relaxed,
                                                                );
                                                            }
                                                        }
                                                    }
                                                },
                                            );
                                        }
                                    }
                                });
                        }
                    }
                });
            }
        })
    });

    count.load(Ordering::Relaxed)
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
    let mut c1 = 0usize;
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
    let mut out = vec![]; // use this if we have to output the data

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
    let mut out = vec![]; // use this if we have to output the data

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

fn binary_search_join<'a>(
    iter: impl IntoIterator<Item = (Time, Time)>,
    edges: &'a Vec<(Time, Time, VID)>,
    a: &VID,
    count: &'a mut usize,
) -> Vec<(&'a Time, &'a Time, &'a Time)> {
    let mut out = vec![];

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

#[cfg(test)]
mod test {
    use raphtory::core::entities::VID;

    use crate::{binary_search_join, SpillMap};

    // #[test]
    // fn bin_search_join_1_1() {
    //     let mut count = 0;
    //     let prog1_nft = vec![(2, 2, 2)];
    //     let login = vec![(&4624, &1)];
    //     let actual = binary_search_join(login, &prog1_nft, &VID(0), &mut count);
    //     // assert_eq!(actual, vec![(&1, &2, &2)]);
    //     assert_eq!(count, 1);
    // }

    // #[test]
    // fn bin_search_join_1_2() {
    //     let mut count = 0;
    //     let prog1_nft = vec![(1, 1), (2, 2, 2)];
    //     let login = vec![(&4624, &1)];
    //     let actual = binary_search_join(login, &prog1_nft, &VID(0), &mut count);
    //     // assert_eq!(actual, vec![(&1, &2, &2)]);
    //     assert_eq!(count, 1);
    // }

    // #[test]
    // fn bin_search_join_2_2() {
    //     let mut count = 0;
    //     let prog1_nft = vec![(1, 1), (2, 2, 2)];
    //     let login = vec![(&4624, &1), (&4624, &2)];
    //     let actual = binary_search_join(login, &prog1_nft, &VID(0), &mut count);
    //     // assert_eq!(actual, vec![(&1, &2, &2)]);
    //     assert_eq!(count, 1);
    // }

    // #[test]
    // fn bin_search_join_cut_half() {
    //     let mut count = 0;
    //     let prog1_nft = vec![(1, 1, 3), (2, 2, 3), (3, 4, 4), (19, 12, 4)];
    //     let login = vec![(&4624, &2)];
    //     let actual = binary_search_join(login, &prog1_nft, &VID(0), &mut count);
    //     // assert_eq!(actual, vec![(&2, &3, &4), (&2, &19, &12)]);
    //     assert_eq!(count, 2);
    // }

    // #[test]
    // fn bin_search_join_cut_half_2() {
    //     let mut count = 0;
    //     let prog1_nft = vec![(1, 1, 4), (2, 2, 4), (3, 4, 6), (19, 12, 7)];
    //     let login = vec![(&4624, &2), (&4624, &3)];
    //     let actual = binary_search_join(login, &prog1_nft, &VID(0), &mut count);
    //     // assert_eq!(actual, vec![(&2, &3, &4), (&2, &19, &12), (&3, &19, &12)]);
    //     assert_eq!(count, 3);
    // }

    #[test]
    fn spill_map_one_key() {
        let tmp = tempfile::tempdir().unwrap();
        let m = SpillMap::new(tmp.path(), 2);
        m.push(VID(3), &12, &3, VID(4));
        m.push(VID(3), &13, &4, VID(5));
        m.push(VID(3), &1, &5, VID(6));

        m.sort_merge()
    }
}

fn query1_v3(
    g: &TemporalGraph,
    nft: usize,
    events_2v: usize,
    events_1v: usize,
    bytes_prop_id: usize,
    event_id_prop_id_1v: usize,
    event_id_prop_id_2v: usize,
) -> usize {
    let mut count = 0;
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
                            if prog1_event_id == 4688 && prog1_t < nf1_t && nf1_t - prog1_t <= 30
                            // AND prog1.epochtime < nf1.epochtime
                            {
                                for (a2b, a) in g.edges(b, Direction::IN, events_2v) {
                                    if a != e && a != b {
                                        let login1 = g.edge(a2b, events_2v);
                                        for (login1_event_id, login1_t) in login1
                                            .prop_items::<i64>(event_id_prop_id_2v)
                                            .unwrap()
                                            .filter_map(|(t, v)| v.map(|v| (v, t)))
                                        {
                                            if login1_event_id == 4624
                                                && nf1_t - login1_t <= 30
                                                && login1_t < prog1_t
                                            {
                                                count += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    count
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

struct SpillRow<P> {
    max_size: usize,
    key: usize,
    root_path: P,
    prog1_t: Vec<i32>,
    nft_t: Vec<i32>,
    e: Vec<u32>,
    rest: Vec<Chunk<Box<dyn Array>>>,
}

impl<P: AsRef<Path> + Send + Sync> SpillRow<P> {
    fn arrays_at(
        &self,
        i: usize,
    ) -> Option<(
        PrimitiveArray<i32>,
        PrimitiveArray<i32>,
        PrimitiveArray<u32>,
    )> {
        let c = &self.rest[i];

        let prog1_t = c[0]
            .as_ref()
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()?;
        let nft_t = c[1]
            .as_ref()
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()?;
        let e = c[2]
            .as_ref()
            .as_any()
            .downcast_ref::<PrimitiveArray<u32>>()?;

        Some((prog1_t.clone(), nft_t.clone(), e.clone()))
    }

    fn new(key: VID, max_size: usize, root_path: P, prog1_t: &Time, nft_t: &Time, e: VID) -> Self {
        let e_small: u32 = Into::<usize>::into(e) as u32;
        Self {
            max_size,
            key: key.into(),
            root_path,
            prog1_t: vec![*prog1_t as i32],
            nft_t: vec![*nft_t as i32],
            e: vec![e_small],
            rest: vec![],
        }
    }

    fn spill(&mut self) {
        // spill
        let prog1_t =
            PrimitiveArray::from_vec(std::mem::replace(&mut self.prog1_t, vec![])).boxed();
        let nft_t = PrimitiveArray::from_vec(std::mem::replace(&mut self.nft_t, vec![])).boxed();
        let e = PrimitiveArray::from_vec(std::mem::replace(&mut self.e, vec![])).boxed();

        arrow2::compute::sort::lexsort::<i32>(
            &[
                SortColumn {
                    values: prog1_t.as_ref(),
                    options: None,
                },
                SortColumn {
                    values: nft_t.as_ref(),
                    options: None,
                },
                SortColumn {
                    values: e.as_ref(),
                    options: None,
                },
            ],
            None,
        )
        .unwrap();

        let schema = Schema::from(vec![
            Field::new("prog1_t", prog1_t.data_type().clone(), false),
            Field::new("nft_t", nft_t.data_type().clone(), false),
            Field::new("e", e.data_type().clone(), false),
        ]);

        let chunk = [Chunk::try_new(vec![prog1_t, nft_t, e]).unwrap()];

        let file_path =
            self.root_path
                .as_ref()
                .join(format!("{}_{}.arrow", self.key, self.rest.len()));

        write_batches(&file_path, schema, &chunk).unwrap();
        let mmapped_chunk =
            unsafe { mmap_batch(file_path.as_path(), 0).expect("failed to remap chunk") };
        self.rest.push(mmapped_chunk);
    }

    fn push(&mut self, prog1_t: &Time, nft_t: &Time, e: VID) {
        let e_small: u32 = Into::<usize>::into(e) as u32;
        self.prog1_t.push(*prog1_t as i32);
        self.nft_t.push(*nft_t as i32);
        self.e.push(e_small);

        if self.prog1_t.len() == self.max_size {
            self.spill()
        }
    }

    fn merge_sort(&mut self) {
        if self.prog1_t.len() > 0 {
            self.spill()
        }
        self.rest
            .par_iter()
            .map(|c| c.clone())
            .reduce_with(|chunk1, chunk2| Self::merge_array_slices(chunk1, chunk2, &[0, 1]));
    }

    fn merge_array_slices(
        l_chunk: Chunk<Box<dyn Array>>,
        r_chunk: Chunk<Box<dyn Array>>,
        cols: &[usize],
    ) -> Chunk<Box<dyn Array>> {
        let mut pairs = vec![];
        let options = SortOptions::default();

        for col in cols {
            let lhs = l_chunk[*col].as_ref();
            let rhs = r_chunk[*col].as_ref();
            let tuple = vec![lhs, rhs];
            pairs.push(tuple);
        }

        let mut type_fix = vec![];
        for arr in &pairs {
            type_fix.push((&arr[..], &options));
        }

        let slices = arrow2::compute::merge_sort::slices(&type_fix[..]).unwrap();

        let arrays = type_fix
            .into_iter()
            .map(|(arrays, _)| {
                arrow2::compute::merge_sort::take_arrays(arrays, slices.iter().copied(), None)
            })
            .collect_vec();

        Chunk::try_new(arrays).unwrap()
    }
}

struct SpillMap<P> {
    max_size: usize,
    root_path: P,
    map: DashMap<VID, SpillRow<P>>,
}

impl<P: AsRef<Path> + Clone + Send + Sync> SpillMap<P> {
    fn new(path: P, max_size: usize) -> Self {
        Self {
            max_size,
            root_path: path,
            map: DashMap::new(),
        }
    }

    fn push(&self, key: VID, prog1_t: &Time, nft_t: &Time, e: VID) {
        self.map
            .entry(key)
            .and_modify(|entry| entry.push(prog1_t, nft_t, e))
            .or_insert_with(|| {
                SpillRow::new(
                    key,
                    self.max_size,
                    self.root_path.clone(),
                    prog1_t,
                    nft_t,
                    e,
                )
            });
    }

    fn sort_merge(&self) {
        self.map.par_iter_mut().for_each(|mut entry| {
            let entry = entry.value_mut();
            entry.merge_sort()
        })
    }
}
