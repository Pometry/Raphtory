use std::time::Instant;

use raphtory::{
    arrow::{graph::TemporalGraph, loader::ExternalEdgeList},
    core::Direction,
};

fn query1(g: &TemporalGraph) -> Option<usize> {
    // layers
    let nft = g.find_layer_id("netflow")?;
    let wls = g.find_layer_id("wls")?;

    // properties
    let bytes_prop_id = g.edge_property_id("dst_bytes", nft)?;
    let event_id_prop_id = g.edge_property_id("EventID", wls)?;
    let mut count: usize = 0;

    for a in g.all_vertices() {
        // (B)<-[prog1:Events1v]-(B)
        // let b_u64 = Into::<usize>::into(b) as u64;

        // let outbound = g.out_slices(b, nft);

        // let self_loop = outbound.unwrap_or_else(|| &[])
        //     .binary_search(&b_u64);

        // if self_loop.is_err() {
        //     continue;
        // }

        // for (e_id1, e) in g.edges(b, Direction::OUT, nft) {
        //     let nf1 = g.edge(e_id1, nft);

        //     for (_, nf1_t) in nf1
        //         .prop_items::<i64>(bytes_prop_id)?
        //         .filter(|(v, _)| v.as_ref().filter(|&&v| v > &100_000_000).is_some())
        //     {
        //         count += 1;
        //     }
        // }

        for (e_id1, _) in g.edges(a, Direction::OUT, wls) {
            let login1 = g.edge(e_id1, wls);
            for (event_id, _ ) in login1.prop_items::<i64>(event_id_prop_id)? {
                if let Some(event_id) = event_id {
                    if *event_id == 4624{
                        count +=1;
                    }
                }

            }
        }

        // (E)<-[nft1: Netflow]-(B)
        // for (e_id1, e) in g.edges(b, Direction::OUT, nft) {
        //     if e != b {
        //         let nf1 = g.edge(e_id1, nft);
        //         // explode nft2 edge over dstBytes property
        //         for (_, nf1_t) in nf1
        //             .prop_items::<i64>(bytes_prop_id)?
        //             .filter(|(v, _)| v.as_ref().filter(|&&v| v > &100_000_000).is_some())
        //         {
        //             // (B)<-[login1:Events2v]-(A)
        //             for (e_id2, a) in g.edges(b, Direction::IN, wls) {
        //                 if a != e && a != b {
        //                     let login1 = g.edge(e_id2, wls);

        //                     for (login1_event_id, login1_t) in
        //                         login1.prop_items::<i64>(event_id_prop_id)?
        //                     {
        //                         if let Some(login1_event_id) = login1_event_id {
        //                             if *login1_event_id == 4624 && nf1_t - login1_t <= 30 {
        //                                 // AND nf1.epochtime - login1.epochtime <= 30
        //                                 // (B)<-[prog1:Events1v]-(B)
        //                                 for (e_id3, _) in
        //                                     g.edges(b, Direction::OUT, wls).filter(|(_, v)| v == &b)
        //                                 {
        //                                     let prog1 = g.edge(e_id3, wls);
        //                                     for (prog1_event_id, prog1_t) in
        //                                         prog1.prop_items::<i64>(event_id_prop_id)?
        //                                     {
        //                                         if let Some(prog1_event_id) = prog1_event_id {
        //                                             if *prog1_event_id == 4688
        //                                                 && prog1_t < nf1_t
        //                                                 && login1_t < prog1_t
        //                                             {
        //                                                 // AND prog1.epochtime < nf1.epochtime
        //                                                 count += 1;
        //                                             }
        //                                         }
        //                                     }
        //                                 }
        //                             }
        //                         }
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // }
    }

    Some(count)
}

// AND nf1.epochtime - login1.epochtime <= 30

// let login1_ts = login1.timestamps().filter(|ts| {
//     let start = &ts[0];
//     let end = &ts[1];
//     *start > t - 30 && end > t
// });
fn main() {
    let graph_dir = std::env::args()
        .nth(1)
        .expect("please supply a graph directory");

    let netflow_dir = std::env::args()
        .nth(2)
        .expect("please supply a wls directory");

    let wls_dir = std::env::args()
        .nth(3)
        .expect("please supply a netflow directory");

    let now = Instant::now();
    let graph = if std::fs::read_dir(&graph_dir).is_ok() {
        TemporalGraph::new(&graph_dir).expect("failed to load graph")
    } else {
        let layered_edge_list = [
            ExternalEdgeList::new(
                "netflow",
                netflow_dir,
                "src",
                "src_hash",
                "dst",
                "dst_hash",
                "time",
            )
            .expect("failed to load netflow"),
            ExternalEdgeList::new("wls", wls_dir, "src", "src_hash", "dst", "dst_hash", "Time")
                .expect("failed to load wls"),
        ];
        let graph =
            TemporalGraph::from_edge_lists(32768, 262144, 1_000_000, graph_dir, layered_edge_list)
                .expect("failed to load graph");
        graph
    };
    // TemporalGraph::new(graph_dir).unwrap()
    println!("Time taken to load graph: {:?}", now.elapsed());

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

    let count = query1(&graph);

    println!("Time taken: {:?}, count: {:?}", now.elapsed(), count);
}
