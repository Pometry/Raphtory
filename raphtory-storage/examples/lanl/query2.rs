use itertools::Itertools;
use raphtory::{arrow::graph::TemporalGraph, core::Direction};
use rayon::prelude::*;

use crate::thread_pool;

// MATCH (a)-[boot:Events1v]->(a)-[program:Events1v]->(a)
// WHERE boot.eventID = 4608
//   AND program.eventID = 4688
//   AND program.epochtime >= boot.epochtime
//   AND program.epochtime - boot.epochtime < $max_time_window
// RETURN count(*)

// Number of answers: 1,291,069
// CPU times: user 24.6 ms, sys: 7.58 ms, total: 32.2 ms
// Wall time: 15.5 s

const BOOT: i64 = 4608;
const PROGRAM: i64 = 4688;

pub(crate) fn run(g: &TemporalGraph) -> Option<usize> {
    // layer
    let events_1v = g.find_layer_id("events_1v")?;

    // properties
    let event_id_prop_id_1v = g.edge_property_id("event_id", events_1v)?;

    let pool = thread_pool(8);
    let count = pool.install(|| {
        g.all_edges_par(events_1v)
            .map(|edge| {
                let boot_t = edge
                    .prop_history::<i64>(event_id_prop_id_1v)
                    .filter(|(_, v)| *v == BOOT)
                    .map(|(t, _)| t)
                    .collect_vec();

                let count: usize = g
                    .edges(edge.dst(), Direction::OUT, events_1v)
                    .map(|(edge, _)| {
                        let program = g.edge(edge, events_1v);

                        // let program_timestamps = program.timestamps().into_iter_chunks();

                        let program_t = program
                            .prop_history::<i64>(event_id_prop_id_1v)
                            .filter(|(_, v)| *v == PROGRAM)
                            .map(|(t, _)| t);

                        let mut count = 0;
                        for program_t in program_t {
                            for boot_t in &boot_t {
                                if program_t >= *boot_t && program_t - boot_t < 4 {
                                    count += 1;
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
