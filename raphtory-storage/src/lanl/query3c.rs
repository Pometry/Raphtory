use itertools::Itertools;
use raphtory_arrow::graph::TemporalGraph;
use rayon::prelude::*;

use crate::lanl::{thread_pool, NUM_THREADS};

// MATCH (a)-[boot:Events1v]->(a)-[program:Events1v]->(a)
//          <-[nf1:Netflow]-(b)
// WHERE a <> b
//   AND nf1.srcPort = 3128
//   AND boot.eventID = 4608
//   AND program.eventID = 4688
// RETURN nf1.epoch_time, boot.epoch_time, program.epoch_time

// Number of answers: 2,744,248,943 paths
// Wall time: OOM on Trovares, 70s on Raphtory

const SRCPORT: i64 = 3128;
const BOOT: i64 = 4608;
const PROGRAM: i64 = 4688;

pub fn run(g: &TemporalGraph) -> Option<usize> {
    // layer
    let nft = g.find_layer_id("netflow")?;
    let events_1v = g.find_layer_id("events_1v")?;

    // properties
    let event_id_prop_id_1v = g.edge_property_id("event_id", events_1v)?;
    let src_port_prop_id = g.edge_property_id("src_port", nft)?;

    let pool = thread_pool(NUM_THREADS);
    let result = pool.install(|| {
        g.all_edges_par(events_1v)
            .flat_map_iter(|edge| {
                let event_ids = edge.prop_history::<i64>(event_id_prop_id_1v);

                let mut boot_vec = Vec::default();
                let mut program_vec = Vec::default();

                for (t, event) in event_ids {
                    if event == PROGRAM {
                        program_vec.push(t)
                    } else if event == BOOT {
                        boot_vec.push(t)
                    }
                }

                let a = edge.dst();

                let nfts: Vec<i64> = g
                    .layer(nft)
                    .in_edges_par(a)
                    .filter(|(_, b)| &a != b)
                    .flat_map_iter(|(eid, _)| {
                        g.edge(eid, nft)
                            .prop_history::<i64>(src_port_prop_id)
                            .filter(|(_, v)| *v == SRCPORT)
                            .map(|(t, _)| t)
                            .collect_vec()
                    })
                    .collect();

                itertools::iproduct!(nfts, boot_vec, program_vec)
            })
            .collect::<Vec<_>>()
    });

    Some(result.len())
}
