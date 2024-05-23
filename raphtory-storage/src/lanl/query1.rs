use raphtory_arrow::graph::TemporalGraph;
use rayon::prelude::*;

use crate::lanl::{thread_pool, NUM_THREADS};

// MATCH (a)-[boot:Events1v]->(a)
// WHERE boot.eventID = 4608
// RETURN count(*)
// Number of answers: 6,452
// CPU times: user 10.9 ms, sys: 2.29 ms, total: 13.2 ms
// Wall time: 243 ms

pub fn run(g: &TemporalGraph) -> Option<usize> {
    let g = g.as_ref();
    // layer
    let events_1v = g.find_layer_id("events_1v")?;

    // properties
    let event_id_prop_id_1v = g.edge_property_id("event_id", events_1v)?;

    let pool = thread_pool(NUM_THREADS);
    let count = pool.install(|| {
        g.all_edges_par(events_1v)
            .map(|edge| {
                // let count: usize = edge
                //     .prop_history_par::<i64>(event_id_prop_id_1v)
                //     .filter(|(_, v)| v == &Some(4608))
                //     .count();

                let count: usize = edge
                    .prop_history::<i64>(event_id_prop_id_1v)
                    .filter(|(_, v)| *v == 4608)
                    .count();
                count
            })
            .sum()
    });

    Some(count)
}
