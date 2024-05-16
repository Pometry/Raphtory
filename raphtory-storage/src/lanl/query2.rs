use raphtory::arrow::prelude::*;
use raphtory_arrow::graph::TemporalGraph;
use rayon::prelude::*;

use crate::lanl::{thread_pool, NUM_THREADS};

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
const WINDOW: i64 = 4;

pub fn run(g: &TemporalGraph) -> Option<usize> {
    // layer
    let events_1v = g.find_layer_id("events_1v")?;

    // properties
    let event_id_prop_id_1v = g.edge_property_id("event_id", events_1v)?;

    let pool = thread_pool(NUM_THREADS);
    let count = pool.install(|| {
        g.all_edges_par(events_1v)
            .map(|edge| {
                let mut count = 0;
                let event_ids = edge.prop_values::<i64>(event_id_prop_id_1v).unwrap();
                let edge_ts = edge.timestamp_slice();
                let len = event_ids.len();

                for (i, t) in edge
                    .prop_items::<i64>(event_id_prop_id_1v)
                    .enumerate()
                    .filter_map(|(i, (t, v))| v.filter(|v| *v == BOOT).map(|_| (i, t)))
                {
                    for (v, program_t) in event_ids
                        .slice(i + 1..len)
                        .into_iter()
                        .zip(edge_ts.slice(i + 1..len))
                    {
                        if program_t - t >= WINDOW {
                            break;
                        }
                        if v == Some(PROGRAM) {
                            count += 1;
                        }
                    }

                    for i in (0..i).rev() {
                        let (v, program_t) = (event_ids.get(i), edge_ts.get(i));
                        if program_t != t {
                            break;
                        }
                        if v == Some(PROGRAM) {
                            count += 1;
                        }
                    }
                }

                count
            })
            .sum()
    });

    Some(count)
}
