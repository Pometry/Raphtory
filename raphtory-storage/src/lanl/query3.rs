use itertools::Itertools;
use raphtory::arrow::prelude::{ArrayOps, BaseArrayOps};
use raphtory_arrow::graph::TemporalGraph;
use rayon::prelude::*;

use crate::lanl::{thread_pool, NUM_THREADS};

// MATCH (a)-[boot:Events1v]->(a)-[program:Events1v]->(a)
//          <-[nf1:Netflow]-(b)
// WHERE a <> b
//   AND nf1.srcPort = 3128
//   AND boot.eventID = 4608
//   AND program.eventID = 4688
//   AND program.epochtime >= boot.epochtime
//   AND nf1.epochtime >= program.epochtime
//   AND nf1.epochtime - boot.epochtime < $max_time_window
// RETURN count(*)

// Number of answers: 98
// CPU times: user 25.3 ms, sys: 0 ns, total: 25.3 ms
// Wall time: 3.82 s

const BOOT: i64 = 4608;
const PROGRAM: i64 = 4688;
const WINDOW: i64 = 4;

pub fn run(g: &TemporalGraph) -> Option<usize> {
    // layer
    let nft = g.find_layer_id("netflow")?;
    let events_1v = g.find_layer_id("events_1v")?;

    // properties
    let event_id_prop_id_1v = g.edge_property_id("event_id", events_1v)?;
    let src_port_prop_id = g.edge_property_id("src_port", nft)?;

    let pool = thread_pool(NUM_THREADS);
    let count = pool.install(|| {
        g.all_edges_par(events_1v)
            .map(|edge| {
                let event_ids = edge.prop_values::<i64>(event_id_prop_id_1v).unwrap();
                let edge_ts = edge.timestamp_slice();
                let len = event_ids.len();

                let count: usize = g
                    .layer(events_1v)
                    .out_edges_par(edge.dst())
                    .map(|(_, a)| {
                        let nft_ts = g
                            .layer(nft)
                            .in_edges_par(a)
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
                            .enumerate()
                            .filter_map(|(i, (t, v))| v.filter(|v| *v == BOOT).map(|_| (i, t)))
                        {
                            for (_, nft_ts) in nft_ts.iter() {
                                for nft_t in nft_ts {
                                    for (v, program_t) in event_ids
                                        .slice(i + 1..len)
                                        .into_iter()
                                        .zip(edge_ts.slice(i + 1..len))
                                    {
                                        if program_t < t
                                            || nft_t < &program_t
                                            || nft_t - t >= WINDOW
                                        {
                                            break;
                                        }
                                        if v == Some(PROGRAM) {
                                            count += 1;
                                        }
                                    }

                                    for i in (0..i).rev() {
                                        let (v, program_t) = (event_ids.get(i), edge_ts.get(i));
                                        if program_t != t
                                            || nft_t < &program_t
                                            || nft_t - t >= WINDOW
                                        {
                                            break;
                                        }
                                        if v == Some(PROGRAM) {
                                            count += 1;
                                        }
                                    }
                                }
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
