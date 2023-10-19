use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

use ahash::{HashMap, HashSet};
use itertools::Itertools;
use raphtory::{
    arrow::{graph::TemporalGraph, loader::ExternalEdgeList, Time},
    core::{entities::VID, Direction},
};
use rayon::{
    prelude::{IntoParallelIterator, IntoParallelRefIterator, ParallelBridge, ParallelIterator},
    slice::ParallelSlice,
    ThreadPoolBuilder,
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

    let res = query1_v3_par(
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

const STOP_AT: usize = 10;

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

                        for (_, nf1_t) in nf1
                            .prop_items::<i64>(bytes_prop_id)
                            .unwrap()
                            .filter(|(v, _)| v.as_ref().filter(|&&v| v > &100_000_000).is_some())
                        {
                            g.edges(b, Direction::OUT, events_1v)
                                .filter(|(_, v)| v == &b)
                                .for_each(|(b2b, _)| {
                                    let prog1 = g.edge(b2b, events_1v);

                                    for (prog1_event_id, prog1_t) in prog1
                                        .prop_items::<i64>(event_id_prop_id_1v)
                                        .unwrap()
                                        .filter_map(|(v, t)| v.map(|v| (v, t)))
                                    {
                                        if *prog1_event_id == 4688 && prog1_t < nf1_t && nf1_t - prog1_t <= 30
                                        // AND prog1.epochtime < nf1.epochtime
                                        {
                                            g.edges_par(b, Direction::IN, events_2v).for_each(
                                                |(a2b, a)| {
                                                    if a != e && a != b {
                                                        let login1 = g.edge(a2b, events_2v);
                                                        for (login1_event_id, login1_t) in login1
                                                            .prop_items::<i64>(event_id_prop_id_2v)
                                                            .unwrap()
                                                            .filter_map(|(v, t)| v.map(|v| (v, t)))
                                                        {
                                                            if *login1_event_id == 4624
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
    let mut count = 0;
    let mut dream_map: HashMap<VID, Vec<(i64, i64)>> = HashMap::default();

    for b in g.all_vertices() {
        for (b2e, e) in g.edges(b, Direction::OUT, nft) {
            if e != b {
                let nf1 = g.edge(b2e, nft);

                for (_, nf1_t) in nf1
                    .prop_items::<i64>(bytes_prop_id)
                    .unwrap()
                    .filter(|(v, _)| v.as_ref().filter(|&&v| v > &100_000_000).is_some())
                {
                    for (b2b, _) in g
                        .edges(b, Direction::OUT, events_1v)
                        .filter(|(_, v)| v == &b)
                    {
                        let prog1 = g.edge(b2b, events_1v);

                        for (prog1_event_id, prog1_t) in prog1
                            .prop_items::<i64>(event_id_prop_id_1v)
                            .unwrap()
                            .filter_map(|(v, t)| v.map(|v| (v, t)))
                        {
                            if *prog1_event_id == 4688 && prog1_t < nf1_t && nf1_t - prog1_t <= 30
                            {
                                count += 1;
                                dream_map
                                    .entry(b)
                                    .and_modify(|v| v.push((*prog1_t, *nf1_t)))
                                    .or_insert(vec![(*prog1_t, *nf1_t)]);
                            }
                        }
                    }
                }
            }
        }
    }
    println!("NEW count: {:?}", count);

    dream_map.values_mut().for_each(|v| {
        v.sort();
    });

    // for (b, edges) in dream_map {
    //     for (a2b, a) in g.edges(b, Direction::IN, events_2v) {
    //         if a != b {
    //             let login1 = g.edge(a2b, events_2v);

    //             let probe_value = &edges;

    //             if !probe_value.is_empty() {
    //                 let max_prog1 = probe_value.last().unwrap().0;
    //                 // let min_prog1 = probe_value.first().unwrap().0;

    //                 let login1_ts = login1.timestamps();
    //                 let min_login1 = login1_ts.flat_map(|ts| ts.first()).copied().min().unwrap();
    //                 if min_login1 > max_prog1 {
    //                     continue;
    //                 }
    //             }

    //             // nft_t [ 4, 7, 145, 290, 570 ]
    //             // prog_t [ 5, 7, 9, 19, 23 ]
    //             // login_t [ 1, 3, 4, 17, 37 ]

    //             let iter = login1
    //                 .prop_items::<i64>(event_id_prop_id_2v)
    //                 .unwrap()
    //                 .filter_map(|(v, t)| v.map(|v| (v, t)));

    //             // for (login1_event_id, login1_t) in iter {

    //             // }
                

    //             // binary_search_join(iter, &edges, &mut count);
    //         }
    //     }
    // }
    count
}

fn binary_search_join<'a>(
    iter: impl IntoIterator<Item = (&'a Time, &'a Time)>,
    edges: &'a Vec<(Time, Time)>,
    count: &'a mut usize,
) -> Vec<(&'a Time, &'a Time, &'a Time)> {
    let mut out = vec![];
    'outer: for (login1_event_id, login1_t) in iter {
        if *login1_event_id == 4624
        /*&& nf1_t - login1_t <= 30 && login1_t < prog1_t*/
        {
            let pos = edges.binary_search_by(|probe| probe.0.cmp(login1_t));

            match pos {
                Ok(i) => {
                    // this one is smaller than all the prog_t
                    let sub_slice = &edges[(i+1)..]; // less than not equal

                    for (prog1_t, nft1_1) in sub_slice {
                        if nft1_1 - login1_t <= 30 {
                            *count += 1;
                            out.push((login1_t, prog1_t, nft1_1));
                        }
                    }
                }
                Err(i) => {
                    if i >= edges.len() {
                        break 'outer;
                    } else {
                        // this one is smaller than all the prog_t
                        let sub_slice = &edges[i..];

                        for (prog1_t, nft1_1) in sub_slice {
                            if nft1_1 - login1_t <= 30 {
                                *count += 1;
                                out.push((login1_t, prog1_t, nft1_1));
                            } // maybe break on else
                        }
                    }
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod test {
    use crate::binary_search_join;

    #[test]
    fn bin_search_join_1_1() {
        let mut count = 0;
        let prog1_nft = vec![(2, 2)];
        let login = vec![(&4624, &1)];
        let actual = binary_search_join(login, &prog1_nft, &mut count);
        assert_eq!(actual, vec![(&1, &2, &2)]);
        assert_eq!(count, 1);
    }

    #[test]
    fn bin_search_join_1_2() {
        let mut count = 0;
        let prog1_nft = vec![(1, 1), (2, 2)];
        let login = vec![(&4624, &1)];
        let actual = binary_search_join(login, &prog1_nft, &mut count);
        assert_eq!(actual, vec![(&1, &2, &2)]);
        assert_eq!(count, 1);
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

                for (_, nf1_t) in nf1
                    .prop_items::<i64>(bytes_prop_id)
                    .unwrap()
                    .filter(|(v, _)| v.as_ref().filter(|&&v| v > &100_000_000).is_some())
                {
                    for (b2b, _) in g
                        .edges(b, Direction::OUT, events_1v)
                        .filter(|(_, v)| v == &b)
                    {
                        let prog1 = g.edge(b2b, events_1v);

                        for (prog1_event_id, prog1_t) in prog1
                            .prop_items::<i64>(event_id_prop_id_1v)
                            .unwrap()
                            .filter_map(|(v, t)| v.map(|v| (v, t)))
                        {
                            if *prog1_event_id == 4688 && prog1_t < nf1_t && nf1_t - prog1_t <= 30
                            // AND prog1.epochtime < nf1.epochtime
                            {
                                for (a2b, a) in g.edges(b, Direction::IN, events_2v) {
                                    if a != e && a != b {
                                        let login1 = g.edge(a2b, events_2v);
                                        for (login1_event_id, login1_t) in login1
                                            .prop_items::<i64>(event_id_prop_id_2v)
                                            .unwrap()
                                            .filter_map(|(v, t)| v.map(|v| (v, t)))
                                        {
                                            if *login1_event_id == 4624
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
        .unwrap_or_else(|| "/mnt/work/pometry/graph_netflow_85".to_string());
    // .unwrap_or_else(|| "/mnt/work/pometry/graph_small".to_string());
    // .expect("please supply a graph directory");

    let netflow_dir = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "/mnt/work/pometry/netflow_parquet_day85".to_string());
    // .expect("please supply a wls directory");

    let v1_dir = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "/mnt/work/pometry/v1_parquet_day85".to_string());
    // .expect("please supply a v1 directory");

    let v2_dir = std::env::args()
        .nth(4)
        .unwrap_or_else(|| "/mnt/work/pometry/v2_parquet_day85".to_string());
    // .expect("please supply a v2 directory");

    let now = Instant::now();
    let graph = if std::fs::read_dir(&graph_dir).is_ok() {
        TemporalGraph::new(&graph_dir).expect("failed to load graph")
    } else {
        let layered_edge_list = [
            ExternalEdgeList::new(
                "netflow",
                netflow_dir,
                "src_device",
                "src_hash",
                "dst_device",
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
                "destination",
                "dst_hash",
                "epoch_time",
            )
            .expect("failed to load events_v2"),
        ];
        let graph =
            TemporalGraph::from_edge_lists(4096, 1024, 1_000_000, graph_dir, layered_edge_list)
                .expect("failed to load graph");
        graph
    };
    // TemporalGraph::new(graph_dir).unwrap()
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
