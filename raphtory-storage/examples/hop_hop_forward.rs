use std::{time::Instant, collections::BTreeMap};

use raphtory::{
    arrow::{graph::TemporalGraph, loader::ExternalEdgeList},
    core::{Direction, entities::VID},
};

fn query1(g: &TemporalGraph) -> Option<usize> {
    // layer
    let nft = g.find_layer_id("netflow")?;
    let events_1v = g.find_layer_id("events_1v")?;
    let events_2v = g.find_layer_id("events_2v")?;

    let ad = VID(60552);
    let ad_nft = g.edges(ad, Direction::OUT, nft).count();
    let ad_1v = g.edges(ad, Direction::OUT, events_1v).count();
    let ad_2v = g.edges(ad, Direction::OUT, events_2v).count();

    // print all in one line
    println!("OUT ad_nft: {}, ad_1v: {}, ad_2v: {}", ad_nft, ad_1v, ad_2v);

    let ad_nft = g.edges(ad, Direction::IN, nft).count();
    let ad_1v = g.edges(ad, Direction::IN, events_1v).count();
    let ad_2v = g.edges(ad, Direction::IN, events_2v).count();

    // print all in one line
    println!("IN ad_nft: {}, ad_1v: {}, ad_2v: {}", ad_nft, ad_1v, ad_2v);

    // properties
    let bytes_prop_id = g.edge_property_id("dst_bytes", nft)?;
    let prop_id_2v = g.edge_property_id("event_id", events_2v)?;
    let prop_id_1v = g.edge_property_id("event_id", events_1v)?;
    let mut count: usize = 0;
    let mut c: usize = 0;

    let nft_all_edges = g.exploded_edges(nft).count();
    let v1_all_edges = g.exploded_edges(events_1v).count();
    let v2_all_edges = g.exploded_edges(events_2v).count();

    println!("nft_all_edges: {}", nft_all_edges);
    println!("v1_all_edges: {}", v1_all_edges);
    println!("v2_all_edges: {}", v2_all_edges);

    let mut nft_all_edges = 0usize;
    let mut v1_all_edges = 0;
    let mut v2_all_edges = 0;

    for b in g.all_vertices() {
        for (e_id, _) in g.edges(b, Direction::OUT, nft) {
            let nft_e = g.edge(e_id, nft);
            for _ in nft_e.prop_items::<i64>(bytes_prop_id)? {
                nft_all_edges += 1;
            }
        }
        // event1
        for (e_id, _) in g.edges(b, Direction::OUT, events_1v) {
            let v1_e = g.edge(e_id, events_1v);
            for _ in v1_e.prop_items::<i64>(prop_id_1v)? {
                v1_all_edges += 1;
            }
        }
        // event2
        for (e_id, _) in g.edges(b, Direction::OUT, events_2v) {
            let v2_e = g.edge(e_id, events_2v);
            for _ in v2_e.prop_items::<i64>(prop_id_2v)? {
                v2_all_edges += 1;
            }
        }
    }

    // |== Physical Plan ==
    // AdaptiveSparkPlan isFinalPlan=false
    // +- HashAggregate(keys=[], functions=[count(1)])
    //   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=799]
    //      +- HashAggregate(keys=[], functions=[partial_count(1)])
    //         +- Project
    //            +- SortMergeJoin [destination#148], [src_device#82], Inner, ((epoch_time#80L - epoch_time#134L) <= 30)
    //               :- Sort [destination#148 ASC NULLS FIRST], false, 0
    //               :  +- Exchange hashpartitioning(destination#148, 200), ENSURE_REQUIREMENTS, [plan_id=791]
    //               :     +- Project [epoch_time#134L, destination#148]
    //               :        +- Filter (((isnotnull(event_id#135L) AND (event_id#135L = 4624)) AND isnotnull(epoch_time#134L)) AND isnotnull(destination#148))
    //               :           +- FileScan parquet [epoch_time#134L,event_id#135L,destination#148] Batched: true, DataFilters: [isnotnull(event_id#135L), (event_id#135L = 4624), isnotnull(epoch_time#134L), isnotnull(destinat..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/mnt/work/pometry/v2_parquet_day85], PartitionFilters: [], PushedFilters: [IsNotNull(event_id), EqualTo(event_id,4624), IsNotNull(epoch_time), IsNotNull(destination)], ReadSchema: struct<epoch_time:bigint,event_id:bigint,destination:string>
    //               +- Sort [src_device#82 ASC NULLS FIRST], false, 0
    //                  +- Exchange hashpartitioning(src_device#82, 200), ENSURE_REQUIREMENTS, [plan_id=792]
    //                     +- Project [epoch_time#80L, src_device#82]
    //                        +- Filter (((isnotnull(dst_bytes#90L) AND (dst_bytes#90L > 100000000)) AND isnotnull(epoch_time#80L)) AND isnotnull(src_device#82))
    //                           +- FileScan parquet [epoch_time#80L,src_device#82,dst_bytes#90L] Batched: true, DataFilters: [isnotnull(dst_bytes#90L), (dst_bytes#90L > 100000000), isnotnull(epoch_time#80L), isnotnull(src_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/mnt/work/pometry/netflow_parquet_day85], PartitionFilters: [], PushedFilters: [IsNotNull(dst_bytes), GreaterThan(dst_bytes,100000000), IsNotNull(epoch_time), IsNotNull(src_dev..., ReadSchema: struct<epoch_time:bigint,src_device:string,dst_bytes:bigint>

    println!("############################################################");
    println!("nft_all_edges: {}", nft_all_edges);
    println!("v1_all_edges: {}", v1_all_edges);
    println!("v2_all_edges: {}", v2_all_edges);
    let mut sorted_gids = BTreeMap::new();

    for b in g.all_vertices() {
        // // (B)<-[login1:Events2v]-(A)
        // for (e_id, b) in g.edges(a, Direction::OUT, events_2v) {
        //     let login1 = g.edge(e_id, events_2v);

        //     for (event_id, login1_t) in login1
        //         .prop_items::<i64>(prop_id_2v)?
        //         .filter_map(|(v, t)| v.as_ref().map(|&v| (v, t)))
        //     {
        //         if event_id == &4624 {
        //             for (e_id2, _) in g.edges(b, Direction::OUT, nft) {
        //                 let nft_e = g.edge(e_id2, nft);

        //                 for (dst_bytes, nft_t) in nft_e.prop_items::<i64>(bytes_prop_id)? {
        //                     if let Some(dst_bytes) = dst_bytes {
        //                         if dst_bytes > &100_000_000 && *nft_t - *login1_t <= 30 {
        //                             count += 1;
        //                         }
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // }

        let name = g.vertex_gid(b);
        let mut nft_events:usize = 0;
        let mut v2_events: usize = 0;
        // (E)<-[nft1: Netflow]-(B)
        for (e_id1, _) in g.edges(b, Direction::OUT, nft) {
            /*if e != b */
            {
                let nf1 = g.edge(e_id1, nft);
                // explode nft2 edge over dstBytes property
                for (_, nf1_t) in nf1
                    .prop_items::<i64>(bytes_prop_id)?
                    .filter(|(v, _)| v.as_ref().filter(|&&v| v > &100_000_000).is_some())
                {
                    // (B)<-[login1:Events2v]-(A)
                    nft_events+=1;
                    for (e_id2, _) in g.edges(b, Direction::IN, events_2v) {
                        /* if a != e && a != b */
                        {
                            let login1 = g.edge(e_id2, events_2v);

                            for (login1_event_id, login1_t) in login1
                                .prop_items::<i64>(prop_id_2v)?
                                .filter_map(|(v, t)| v.map(|v| (v, t)))
                            {
                                /*if let Some(login_event_id) = login1_event_id*/ {
                                    if *login1_event_id == 4624
                                    && nf1_t - login1_t <= 30
                                    {
                                        v2_events+=1;
                                        count += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        sorted_gids.insert(name.unwrap(), (b, nft_events, v2_events));

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
        //             for (e_id2, a) in g.edges(b, Direction::IN, events_2v) {
        //                 if a != e && a != b {
        //                     let login1 = g.edge(e_id2, events_2v);

        //                     for (login1_event_id, login1_t) in login1
        //                         .prop_items::<i64>(prop_id_2v)?
        //                         .filter_map(|(v, t)| v.map(|v| (v, t)))
        //                     {
        //                         if *login1_event_id == 4624 && nf1_t - login1_t <= 30 {
        //                             // AND nf1.epochtime - login1.epochtime <= 30
        //                             // (B)<-[prog1:Events1v]-(B)

        //                             for (e_id3, _) in g
        //                                 .edges(b, Direction::OUT, events_1v)
        //                                 .filter(|(_, v)| v == &b)
        //                             {
        //                                 let prog1 = g.edge(e_id3, events_1v);
        //                                 for (prog1_event_id, prog1_t) in prog1
        //                                     .prop_items::<i64>(prop_id_1v)?
        //                                     .filter_map(|(v, t)| v.map(|v| (v, t)))
        //                                 {

        //                                     if *prog1_event_id == 4688
        //                                         // && prog1_t < nf1_t // AND prog1.epochtime < nf1.epochtime
        //                                         // && login1_t < prog1_t
        //                                     {
        //                                         count += 1;
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

    for (gid, events) in sorted_gids.into_iter().take(50) {
        println!("{:?}: {:?}", gid, events);
    }
    println!("c: {}", c);
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
        .unwrap_or_else(|| "/mnt/work/pometry/graph_netflow_85".to_string());
        // .unwrap_or_else(|| "/mnt/work/pometry/graph_small".to_string());
    // .expect("please supply a graph directory");

    let netflow_dir = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "/mnt/work/pometry/netflow_parquet_day85".to_string());
    // .expect("please supply a wls directory");

    let v1_dir = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "/mnt/work/pometry/v1_parquet_day85".to_string());
    // .expect("please supply a v1 directory");

    let v2_dir = std::env::args()
        .nth(4)
        .unwrap_or_else(|| "/mnt/work/pometry/v2_parquet_day85".to_string());
    // .expect("please supply a v2 directory");

    let now = Instant::now();
    let graph = if std::fs::read_dir(&graph_dir).is_ok() {
        TemporalGraph::new(&graph_dir).expect("failed to load graph")
    } else {
        let layered_edge_list = [
            ExternalEdgeList::new(
                "netflow",
                netflow_dir,
                "src_device",
                "src_hash",
                "dst_device",
                "dst_hash",
                "epoch_time",
            )
            .expect("failed to load netflow"),
            ExternalEdgeList::new(
                "events_1v",
                v1_dir,
                "src",
                "src_hash",
                "dst",
                "dst_hash",
                "epoch_time",
            )
            .expect("failed to load events_v1"),
            ExternalEdgeList::new(
                "events_2v",
                v2_dir,
                "src",
                "src_hash",
                "destination",
                "dst_hash",
                "epoch_time",
            )
            .expect("failed to load events_v2"),
        ];
        let graph =
            TemporalGraph::from_edge_lists(4096, 1024, 1_000_000, graph_dir, layered_edge_list)
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

    // Launched job 7
    // Number of answers: 2,992,551
    // CPU times: user 25.9 ms, sys: 11.7 ms, total: 37.6 ms
    // Wall time: 21.8 s

    // Devices (vertices): 159,245
    // Netflow (edges): 317,164,045
    // Host event 1-vertex (edges): 33,480,483
    // Host event 2-vertex (edges): 97,716,529
    // Total (edges): 448,361,057



    let count = query1(&graph);

    println!("Time taken: {:?}, count: {:?}", now.elapsed(), count);
}
