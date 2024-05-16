use raphtory::arrow::prelude::ArrayOps;
use raphtory_arrow::graph::TemporalGraph;
use rayon::prelude::*;

use crate::lanl::{thread_pool, NUM_THREADS};

// MATCH (a)-[boot:Events1v]->(a)-[program:Events1v]->(a)
//          <-[nf1:Netflow]-(b)
// WHERE a <> b
//   AND nf1.srcPort = 3128
//   AND boot.eventID = 4608
//   AND program.eventID = 4688
// RETURN count(*)

// Number of answers: 2,744,248,943
// Wall time: 52 s

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
    let count = pool.install(|| {
        g.all_edges_par(events_1v)
            .map(|edge| {
                let event_ids = edge.prop_values::<i64>(event_id_prop_id_1v).unwrap();
                let mut count_boot: usize = 0;
                let mut count_program: usize = 0;

                for event in event_ids.iter().flatten() {
                    if event == PROGRAM {
                        count_program += 1;
                    } else if event == BOOT {
                        count_boot += 1;
                    }
                }

                let a = edge.dst();

                let nfts: usize = g
                    .layer(nft)
                    .in_edges_par(a)
                    .filter(|(_, b)| &a != b)
                    .map(|(eid, _)| {
                        g.edge(eid, nft)
                            .prop_items::<i64>(src_port_prop_id)
                            .filter(|(_, v)| *v == Some(SRCPORT))
                            .count()
                    })
                    .sum();

                count_boot * count_program * nfts
            })
            .sum()
    });

    Some(count)
}
