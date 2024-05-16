use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use ahash::HashSet;
use dashmap::DashMap;
use itertools::Itertools;
use rayon::{
    iter::IntoParallelIterator,
    prelude::{IntoParallelRefIterator, ParallelIterator},
    ThreadPoolBuilder,
};

use raphtory_arrow::{graph::TemporalGraph, interop::VID, prelude::*, Time};

pub fn run(g: &TemporalGraph) -> Option<usize> {
    // layer
    let nft = g.find_layer_id("netflow")?;
    let events_1v = g.find_layer_id("events_1v")?;
    let events_2v = g.find_layer_id("events_2v")?;

    // properties
    let bytes_prop_id = g.edge_property_id("dst_bytes", nft)?;
    let event_id_prop_id_2v = g.edge_property_id("event_id", events_2v)?;
    let event_id_prop_id_1v = g.edge_property_id("event_id", events_1v)?;

    let res = query1_v7(
        g,
        nft,
        events_2v,
        events_1v,
        bytes_prop_id,
        event_id_prop_id_1v,
        event_id_prop_id_2v,
    );
    println!("node count: {:?}", g.num_nodes());
    Some(res)
}

fn query1_v7(
    g: &TemporalGraph,
    nft: usize,
    events_2v: usize,
    events_1v: usize,
    bytes_prop_id: usize,
    event_id_prop_id_1v: usize,
    event_id_prop_id_2v: usize,
) -> usize {
    let num_threads = std::thread::available_parallelism().unwrap().get() - 1;
    println!("num_threads: {:?}", num_threads);
    let pool = ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .expect("failed to build pool");

    let count = pool.install(|| {
        let now = Instant::now();

        let b_prog1_filter: HashSet<VID> = {
            g.all_edges_par(events_1v)
                .filter(|edge| {
                    edge.par_prop_items_unchecked::<i64>(event_id_prop_id_1v)
                        .unwrap()
                        .any(|(_, event_id)| event_id == 4688)
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
                    edge.par_prop_items_unchecked::<i64>(event_id_prop_id_2v)
                        .unwrap()
                        .any(|(_, event_id)| event_id == 4624)
                })
                .map(|e| e.dst())
                .collect()
        };
        println!(
            "b_login1_filter: {:?}, count: {}",
            now.elapsed(),
            b_login1_filter.len()
        );

        let count = AtomicUsize::new(0);
        let vs = b_login1_filter.iter().copied().collect_vec();
        let probe_map: Arc<DashMap<VID, (Time, Vec<(i64, i64, VID)>)>> =
            Arc::new(DashMap::default());

        let now = Instant::now();
        pool.install(|| {
            vs.into_par_iter().for_each(|b| {
                g.out_adj_par(b, nft).for_each(|(b2e, e)| {
                    if e != b {
                        let nf1 = g.edge(b2e, nft);
                        nf1.par_prop_items_unchecked::<i64>(bytes_prop_id)
                            .unwrap()
                            .filter(|(_, v)| *v > 100_000_000)
                            .for_each(|(nf1_t, _)| {
                                g.out_adj_par(b, events_1v)
                                    .filter(|(_, v)| v == &b && b_prog1_filter.contains(v))
                                    .for_each(|(b2b, _)| {
                                        let prog1 = g.edge(b2b, events_1v);

                                        prog1
                                            .par_prop_items_unchecked::<i64>(event_id_prop_id_1v)
                                            .unwrap()
                                            .for_each(|(prog1_t, prog1_event_id)| {
                                                if prog1_event_id == 4688
                                                    && prog1_t < nf1_t
                                                    && nf1_t - prog1_t <= 30
                                                {
                                                    probe_map
                                                        .entry(b)
                                                        .and_modify(|(nft_min, v)| {
                                                            v.push((prog1_t, nf1_t - 30, e));
                                                            *nft_min = (nf1_t - 30).min(*nft_min);
                                                        })
                                                        .or_insert_with(|| {
                                                            let nf1_t_less30 = nf1_t - 30;
                                                            (
                                                                nf1_t_less30,
                                                                vec![(prog1_t, nf1_t_less30, e)],
                                                            )
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

        let probe_map: HashMap<VID, (Time, (Vec<Time>, Vec<Time>, Vec<VID>))> = probe_map
            .iter_mut()
            .map(|mut entry| {
                let key = *entry.key();
                let (min_nft_less30, rows) = entry.value_mut();
                rows.sort_unstable();

                // split the rows into columns
                let cols: (Vec<_>, (Vec<_>, Vec<_>)) =
                    rows.into_iter().map(|(a, b, c)| (*a, (*b, *c))).unzip();

                let (prog_t, (c2, c3)) = cols;
                (key, (*min_nft_less30, (prog_t, c2, c3)))
            })
            .collect();

        println!(
            "Done sorting map len: {:?}, duration: {:?}",
            probe_map.len(),
            now.elapsed()
        );

        let now: Instant = Instant::now();

        pool.install(|| {
            probe_map.par_iter().for_each(|entry| {
                let (b, (min_nft_less30, (prog1, nft_less30, e))) = entry;
                let b = *b;

                g.in_adj_par(b, events_2v).for_each(|(a2b, a)| {
                    if a != b {
                        let login1 = g.edge(a2b, events_2v);

                        let mut skip = false;
                        if !prog1.is_empty() {
                            let max_prog1 = *prog1.last().unwrap() as i64;

                            let login1_ts = login1.timestamp_slice();

                            let min_login1 = login1_ts.iter().next().unwrap() as i64;
                            if min_login1 > max_prog1 {
                                skip = true;
                            }
                        }

                        if !skip {
                            let iter = login1
                                .par_prop_items_unchecked::<i64>(event_id_prop_id_2v)
                                .unwrap()
                                .filter(|(login1_t, _)| login1_t >= &min_nft_less30);

                            binary_search_join_par_4(iter, prog1, nft_less30, e, &a, &count);
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

#[inline]
fn binary_search_join_par_4<'a>(
    login_events: impl ParallelIterator<Item = (Time, i64)>,
    prog1: &[Time],
    nft: &[Time],
    e: &[VID],
    a: &VID,
    count: &'a AtomicUsize,
) {
    let c = login_events
        .filter(|(_, login1_event_id)| *login1_event_id == 4624)
        .filter_map(|(login1_t, _)| {
            let pos = prog1.partition_point(|prog1_t| prog1_t < &login1_t);
            if pos == prog1.len() {
                None
            } else {
                Some((pos, login1_t))
            }
        })
        .map(|(from, login1_t)| {
            optimise_to_bits_small(&prog1[from..], &nft[from..], &e[from..], &login1_t, a)
        })
        .sum::<usize>();

    count.fetch_add(c, Ordering::Relaxed);
}

#[inline]
fn optimise_to_bits_small<'a>(
    prog1: impl IntoIterator<Item = &'a Time>,
    nft_less30: impl IntoIterator<Item = &'a Time>,
    e: impl IntoIterator<Item = &'a VID>,
    login1_t: &Time,
    a: &VID,
) -> usize {
    prog1
        .into_iter()
        .zip(nft_less30)
        .zip(e)
        .filter(move |((prog1_t, nft_t_less30), e)| {
            login1_t >= nft_t_less30 && login1_t < prog1_t && a != *e
        })
        .count()
}
