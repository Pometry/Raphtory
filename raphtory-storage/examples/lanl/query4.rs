use itertools::Itertools;
use raphtory::{
    arrow::graph::TemporalGraph,
    core::{entities::VID, Direction},
};
use rayon::prelude::*;

use ahash::HashMap;
use parking_lot::Mutex;

use crate::{thread_pool, NUM_THREADS};

// MATCH (a)-[boot:Events1v]->(a)-[program:Events1v]->(a)
//          <-[nf1:Netflow]-(b)-[nf2:Netflow]->(c)
// WHERE a <> b AND b <> c AND a <> c
//   AND nf1.srcPort = 3128
//   AND boot.eventID = 4608
//   AND program.eventID = 4688
//   AND program.epochtime >= boot.epochtime
//   AND nf1.epochtime >= program.epochtime
//   AND nf1.epochtime - boot.epochtime < $max_time_window
//   AND nf2.duration >= $min_session_duration
//   AND nf2.epochtime < nf1.epochtime
//   AND nf2.epochtime + nf2.duration >= nf1.epochtime
// RETURN count(*)

// Number of answers: 196
// CPU times: user 13.6 ms, sys: 5.89 ms, total: 19.5 ms
// Wall time: 4.02 s

const BOOT: i64 = 4608;
const PROGRAM: i64 = 4688;
const WINDOW: i64 = 4;
const SESSION_DURATION: i64 = 3600;

fn netflow_b_hop_c(b: VID, g: &TemporalGraph) -> Option<()> {
    // layer
    let nft = g.find_layer_id("netflow")?;
    let events_1v = g.find_layer_id("events_1v")?;

    // properties
    let event_id_prop_id_1v = g.edge_property_id("event_id", events_1v)?;
    let src_port_prop_id = g.edge_property_id("src_port", nft)?;
    let duration = g.edge_property_id("duration", nft)?;

    None
}

pub(crate) fn run(g: &TemporalGraph) -> Option<usize> {
    // layer
    let nft = g.find_layer_id("netflow")?;
    let events_1v = g.find_layer_id("events_1v")?;

    // properties
    let event_id_prop_id_1v = g.edge_property_id("event_id", events_1v)?;
    let src_port_prop_id = g.edge_property_id("src_port", nft)?;
    let duration = g.edge_property_id("duration", nft)?;

    let pool = thread_pool(NUM_THREADS);
    let count = pool.install(|| {
        g.all_edges_par(events_1v)
            .map(|edge| {
                let event_ids = edge.props::<i64>(event_id_prop_id_1v).unwrap();
                let edge_ts = edge.timestamps();
                let len = event_ids.len();

                let count: usize = g
                    .edges_par(edge.dst(), Direction::OUT, events_1v)
                    .map(|(_, a)| {
                        let nft_ts = g
                            .edges_par(a, Direction::IN, nft)
                            .filter(|(_, b)| a != *b)
                            .map(|(eid, b)| {
                                (
                                    b,
                                    g.edge(eid, nft)
                                        .prop_history::<i64>(src_port_prop_id)
                                        .filter(|(_, v)| *v == 3128)
                                        .map(|(t, _)| t)
                                        .collect_vec(),
                                )
                            })
                            .collect::<Vec<_>>();

                        let mut count = 0;

                        for (i, t) in edge
                            .prop_items::<i64>(event_id_prop_id_1v)
                            .unwrap()
                            .enumerate()
                            .filter_map(|(i, (t, v))| v.filter(|v| *v == BOOT).map(|_| (i, t)))
                        {
                            for (b, nft1_ts) in nft_ts.iter() {
                                g.edges(*b, Direction::OUT, nft)
                                    .filter(|(_, c)| b != c && a != *c)
                                    .for_each(|(e_id, _)| {
                                        let nf2 = g.edge(e_id, nft);

                                        for (nf2_t, duration) in nf2
                                            .prop_items::<i64>(duration)
                                            .unwrap()
                                            .filter_map(|(t, duration)| {
                                                duration
                                                    .filter(|d| d >= &SESSION_DURATION)
                                                    .map(|d| (t, d))
                                            })
                                        {
                                            let split_gt =
                                                nft1_ts.partition_point(|nf1_t| nf1_t <= &nf2_t);

                                            for nft1_t in &nft1_ts[split_gt..] {
                                                if nf2_t + duration < *nft1_t {
                                                    continue;
                                                }
                                                for (v, program_t) in
                                                    event_ids.slice(i + 1..len).into_iter().zip(
                                                        edge_ts
                                                            .slice(i + 1..len)
                                                            .into_iter()
                                                            .flatten(),
                                                    )
                                                {
                                                    if program_t < t
                                                        || nft1_t < &program_t
                                                        || nft1_t - t >= WINDOW
                                                    {
                                                        break;
                                                    }
                                                    if v == Some(PROGRAM) {
                                                        count += 1;
                                                    }
                                                }

                                                for i in (0..i).rev() {
                                                    let (v, program_t) =
                                                        (event_ids.get(i), edge_ts.get(i).unwrap());
                                                    if program_t != t
                                                        || nft1_t < &program_t
                                                        || nft1_t - t >= WINDOW
                                                    {
                                                        break;
                                                    }
                                                    if v == Some(PROGRAM) {
                                                        count += 1;
                                                    }
                                                }
                                            }
                                        }
                                    });
                            }
                        }
                        count
                    })
                    .sum();

                count
            })
            .sum()
    });

    Some(count)
}

pub(crate) fn run2(g: &TemporalGraph) -> Option<usize> {
    // layer
    let nft = g.find_layer_id("netflow")?;
    let events_1v = g.find_layer_id("events_1v")?;

    let intermediate_result = Mutex::new(<HashMap<(VID, VID, i64), usize>>::default());

    // properties
    let event_id_prop_id_1v = g.edge_property_id("event_id", events_1v)?;
    let src_port_prop_id = g.edge_property_id("src_port", nft)?;
    let duration = g.edge_property_id("duration", nft)?;

    let pool = thread_pool(NUM_THREADS);
    pool.install(|| {
        g.all_edges_par(events_1v).for_each(|edge| {
            let event_ids = edge.props::<i64>(event_id_prop_id_1v).unwrap();
            let edge_ts = edge.timestamps();
            let len = event_ids.len();

            g.edges_par(edge.dst(), Direction::OUT, events_1v)
                .for_each(|(_, a)| {
                    let nft_ts = g
                        .edges_par(a, Direction::IN, nft)
                        .map(|(eid, b)| {
                            (
                                b,
                                g.edge(eid, nft)
                                    .prop_history::<i64>(src_port_prop_id)
                                    .filter(|(_, v)| *v == 3128)
                                    .map(|(t, _)| t)
                                    .collect_vec(),
                            )
                        })
                        .collect::<Vec<_>>();

                    for (i, t) in edge
                        .prop_items::<i64>(event_id_prop_id_1v)
                        .unwrap()
                        .enumerate()
                        .filter_map(|(i, (t, v))| v.filter(|v| *v == BOOT).map(|_| (i, t)))
                    {
                        for (b, nft_ts) in nft_ts.iter() {
                            for nft_t in nft_ts {
                                for (v, program_t) in event_ids
                                    .slice(i + 1..len)
                                    .into_iter()
                                    .zip(edge_ts.slice(i + 1..len).into_iter().flatten())
                                {
                                    if program_t < t || nft_t < &program_t || nft_t - t >= WINDOW {
                                        break;
                                    }
                                    if v == Some(PROGRAM) {
                                        intermediate_result
                                            .lock()
                                            .entry((*b, a, *nft_t))
                                            .and_modify(|v| *v += 1usize)
                                            .or_insert_with(|| 1);
                                    }
                                }

                                for i in (0..i).rev() {
                                    let (v, program_t) =
                                        (event_ids.get(i), edge_ts.get(i).unwrap());
                                    if program_t != t || nft_t < &program_t || nft_t - t >= WINDOW {
                                        break;
                                    }
                                    if v == Some(PROGRAM) {
                                        intermediate_result
                                            .lock()
                                            .entry((*b, a, *nft_t))
                                            .and_modify(|v| *v += 1usize)
                                            .or_insert_with(|| 1);
                                    }
                                }
                            }
                        }
                    }
                })
        })
    });

    let count = intermediate_result
        .into_inner()
        .into_par_iter()
        .map(|((b, a, nft_t), count)| expand_nf2_hop(b, a, nft_t, g, nft, duration) * count)
        .sum();

    Some(count)
}

fn expand_nf2_hop(
    b: VID,
    a: VID,
    nf1_t: i64,
    g: &TemporalGraph,
    nft_layer: usize,
    duration_prop_id: usize,
) -> usize {
    let mut count = 0;
    for (e_id, c) in g
        .edges(b, Direction::OUT, nft_layer)
        .filter(|(_, c)| *c != a && *c != b)
    {
        let edge = g.edge(e_id, nft_layer);
        for (t, dur) in edge
            .prop_history::<i64>(duration_prop_id)
            .filter(|(t, duration)| duration >= &SESSION_DURATION && t + duration >= nf1_t)
        {
            if t >= nf1_t {
                break;
            }

            count += 1;
        }
    }
    count
}