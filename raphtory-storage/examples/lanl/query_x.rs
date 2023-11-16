use itertools::Itertools;
use raphtory::{arrow::graph::TemporalGraph, core::Direction};
use rayon::prelude::*;

use crate::{thread_pool, NUM_THREADS};

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

pub(crate) fn run(g: &TemporalGraph) -> Option<usize> {
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
                let event_ids = edge.props::<i64>(event_id_prop_id_1v).unwrap();
                let edge_ts = edge.timestamps();
                let len = event_ids.len();

                let count: usize = g
                    .edges_par(edge.dst(), Direction::OUT, events_1v)
                    .map(|(_, a)| {
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

                        // println!("nft_ts: {:?}", nft_ts.len());
                        let mut count = 0;

                        for i in edge
                            .prop_items::<i64>(event_id_prop_id_1v)
                            .unwrap()
                            .enumerate()
                            .filter_map(|(i, (_, v))| v.filter(|v| *v == BOOT).map(|_| i))
                        {
                            count += nft_ts
                                .par_iter()
                                .map(|(_, nft_ts)| {
                                    let mut count = 0;
                                    for _ in nft_ts {
                                        for (v, _) in event_ids
                                            .slice(i + 1..len)
                                            .into_iter()
                                            .zip(edge_ts.slice(i + 1..len).into_iter().flatten())
                                        {
                                            if v == Some(PROGRAM) {
                                                count += 1;
                                            }
                                        }

                                        for i in (0..i).rev() {
                                            let (v, _) =
                                                (event_ids.get(i), edge_ts.get(i).unwrap());
                                            if v == Some(PROGRAM) {
                                                count += 1;
                                            }
                                        }
                                    }
                                    count
                                })
                                .sum::<usize>();
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
