use std::sync::Arc;

use dashmap::DashMap;
use itertools::Itertools;
use raphtory::{
    arrow::graph::TemporalGraph,
    core::{entities::VID, Direction},
};
use rayon::prelude::*;

use crate::thread_pool;

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

pub(crate) fn run(g: &TemporalGraph) -> Option<usize> {
    // layer
    let nft = g.find_layer_id("netflow")?;
    let events_1v = g.find_layer_id("events_1v")?;

    // properties
    let event_id_prop_id_1v = g.edge_property_id("event_id", events_1v)?;
    let src_port_prop_id = g.edge_property_id("src_port", nft)?;

    let pool = thread_pool(8);
    let count = pool.install(|| {
        let probe_map: Arc<DashMap<VID, (VID, Vec<i64>)>> = Arc::new(DashMap::new());

        let now = std::time::Instant::now();
        g.all_edges_par(nft).for_each(|edge| {
            edge.prop_history::<i64>(src_port_prop_id)
                .filter(|(_, v)| *v == 3128)
                .for_each(|(t, _)| {
                    probe_map
                        .entry(edge.dst())
                        .and_modify(|(_, time)| time.push(t))
                        .or_insert((edge.src(), vec![t]));
                });
        });

        println!(
            "probe_map.len(): {}, took {:?}",
            probe_map.len(),
            now.elapsed()
        );

        g.all_edges_par(events_1v)
            .map(|edge| {
                let event_ids = edge.props::<i64>(event_id_prop_id_1v).unwrap();
                let edge_ts = edge.timestamps();
                let len = event_ids.len();

                let count: usize = g
                    .edges_par(edge.dst(), Direction::OUT, events_1v)
                    .filter(|(_, a)| probe_map.contains_key(a))
                    .map(|(_, a)| {
                        let entry = probe_map.get(&a).unwrap();
                        let (_, nft_ts) = entry.value();
                        let mut count = 0;

                        for (i, t) in edge
                            .prop_items::<i64>(event_id_prop_id_1v)
                            .unwrap()
                            .enumerate()
                            .filter_map(|(i, (t, v))| v.filter(|v| *v == BOOT).map(|_| (i, t)))
                        {
                            for nft_t in nft_ts.iter() {
                                for (v, program_t) in event_ids
                                    .slice(i + 1..len)
                                    .into_iter()
                                    .zip(edge_ts.slice(i + 1..len).into_iter().flatten())
                                {
                                    if program_t < t || nft_t < &program_t || nft_t - t >= WINDOW {
                                        break;
                                    }
                                    if v == Some(PROGRAM) {
                                        count += 1;
                                    }
                                }

                                for i in (0..i).rev() {
                                    let (v, program_t) =
                                        (event_ids.get(i), edge_ts.get(i).unwrap());
                                    if program_t != t || nft_t < &program_t || nft_t - t >= WINDOW {
                                        break;
                                    }
                                    if v == Some(PROGRAM) {
                                        count += 1;
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
