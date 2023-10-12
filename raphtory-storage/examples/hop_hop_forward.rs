use std::time::Instant;

use ahash::HashSet;
use raphtory::{arrow::{col_graph2::TempColGraphFragment, graph::TemporalGraph}, core::Direction};
use rayon::prelude::*;


fn main() {
    let graph_dir = std::env::args()
        .nth(1)
        .expect("please supply a graph directory");

    let graph = TemporalGraph::new(graph_dir).unwrap();
    let now = Instant::now();

//     MATCH
//     (E)<-[nf1:Netflow]-(B)<-[login1:Events2v]-(A), (B)<-[prog1:Events1v]-(B)
//   WHERE A <> B AND B <> E AND A <> E
//     AND login1.eventID = 4624
//     AND prog1.eventID = 4688
//     AND nf1.dstBytes > 100000000
//     // time constraints within each path
//     AND login1.epochtime < prog1.epochtime
//     AND prog1.epochtime < nf1.epochtime
//     AND nf1.epochtime - login1.epochtime <= 30
//   RETURN count(*)

// Devices (vertices): 159,245
// Netflow (edges): 317,164,045
// Host event 1-vertex (edges): 33,480,483
// Host event 2-vertex (edges): 97,716,529
// Total (edges): 448,361,057

    // layers
    let nft = graph.find_layer_id("netflow").unwrap();
    let wls = graph.find_layer_id("wls").unwrap();

    // properties
    let bytes_prop_id = graph.edge_property_id("_c10", nft).unwrap();
    let event_id_prop_id = graph.edge_property_id("eventID", wls).unwrap();


    for b in graph.all_vertices() {

        let mut dst_bytes_include = false;

        // (E)<-[nft1: Netflow]-(B)
        for (e_id1, e) in graph.edges(b, Direction::OUT, nft) {
            let nf1 = graph.edge(e_id1, nft);
            let iter = nf1.prop_items::<i64>(bytes_prop_id);
            if let Some(nf1_dst_bytes) = iter{
                for (t, _) in nf1_dst_bytes.flatten().filter(|(_, &v)| v > 100_000_000) {
                    // (B)<-[login1:Events2v]-(A)
                    for (e_id2, a) in graph.edges(b, Direction::IN, wls){
                        let login1 = graph.edge(e_id2, wls);
                        // AND nf1.epochtime - login1.epochtime <= 30
                        
                        let login1_ts = login1.timestamps().filter(|ts|{
                            let start = &ts[0];
                            let end = &ts[1];
                            *start > t - 30 && end > t
                        });

                        let iter = login1.props::<i64>(event_id_prop_id);
                        if let Some(login1_event_id) = iter {
                            for _ in login1_event_id.flatten().filter(|&event_id| event_id == &4624){

                            }
                        }
                    }
                }
            }
            
        }
    }


    // let count: usize = graph
    //     .all_edges()
    //     .flat_map(|(e_id, _, _)| {
    //         let edge1 = graph.edge(e_id);
    //         edge1.props::<i64>(bytes_prop_id).map(|bytes_prop| {
    //             bytes_prop
    //                 .par_iter()
    //                 .filter(|&e_bytes| e_bytes > &100_000_000)
    //                 .count()
    //         })
    //     })
    //     .sum();

    // for (e1, n1) in graph.edges(0.into(), Direction::OUT) {
    //     let edge1 = graph.edge(e1);
    //     let e1_ts = edge1.timestamps();
    //     if let Some(edge_bytes) = edge1.props::<u64>("bytes") {

    //         edge_bytes.iter().filter(|&e_bytes| e_bytes > &100_000_000) ;

    //         for (e2, _) in graph.edges(n1, Direction::OUT) {
    //             let edge2 = graph.edge(e2);
    //             let e2_ts = edge2.timestamps();

    //             match (e2_ts.last(), e1_ts.first()) {
    //                 (Some(e2_ts), Some(e1_ts)) => {
    //                     if e2_ts > e1_ts {
    //                         count += 1;
    //                     }
    //                 }
    //                 _ => {}
    //             }
    //         }
    //     }
    // }
 //
    // println!("Time taken: {:?}, counted {count}", now.elapsed());
}
