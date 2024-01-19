use std::{cmp::Reverse, thread::panicking, time::Instant};

use itertools::Itertools;
use raphtory::{
    algorithms::components::weakly_connected_components, arrow::{graph_impl::Graph2}, prelude::*,
};
fn main() {

    let graph_dir = std::env::args()
        .nth(1)
        .expect("Must supply the graph directory");

    // let graph_dir = "/mnt/work/pometry/graph500/30/raphtory_graph";

    let parquet_dir = std::env::args().nth(2);

    // check if graph dir exists if it does then call open_path
    // else call create_path

    let graph2 = if let Ok(_) = std::fs::metadata(&graph_dir) {
    //     Graph2::open_path(graph_dir).expect("Cannot open graph")
    // } else {
        let parquet_dir = parquet_dir.expect("Must supply the parquet directory");
        let chunk_size = 8_388_608;
        let t_props_chunk_size = 20_970_100;
        let now = Instant::now();
        let graph = Graph2::load_from_dir(
            graph_dir,
            parquet_dir,
            "src",
            "src_hash",
            "dst",
            "dst_hash",
            "time",
            chunk_size,
            chunk_size,
            t_props_chunk_size,
        )
        .expect("Cannot load graph");
        println!("########## Load took {:?} ########## ", now.elapsed());
        graph
    } else {
        panic!("Graph directory does not exist")
    };

    println!("Graph has {} nodes", graph2.count_nodes());
    println!("Graph has {} edges", graph2.count_edges());

//     // println!("{:?}", graph2.layer_names());

//     // let l_graph = graph2.layer("default").unwrap();

//     // println!("Graph has {} nodes", l_graph.count_nodes());
//     // println!("Graph has {} edges", l_graph.count_edges());
//     // println!(
//     //     "Graph earliest {:?} latest {:?}",
//     //     l_graph.earliest_time(),
//     //     l_graph.latest_time()
//     // );

//     // let mid = l_graph.earliest_time().unwrap()
//     //     + ((l_graph.latest_time().unwrap() - l_graph.earliest_time().unwrap()) / 2);

//     // println!("mid: {:?}", mid);
//     // let window = l_graph.window(i64::MIN, mid);

//     // println!("Graph has {} nodes", window.count_nodes());
//     // println!("Graph has {} edges", window.count_edges());
//     // println!(
//     //     "Window Graph earliest {:?} latest {:?}",
//     //     window.earliest_time(),
//     //     window.latest_time()
//     // );

//     // let window2 = l_graph.window(mid, i64::MAX);

//     // println!("Graph has {} nodes", window2.count_nodes());
//     // println!("Graph has {} edges", window2.count_edges());

//     // println!(
//     //     "Window2 Graph earliest {:?} latest {:?}",
//     //     window2.earliest_time(),
//     //     window2.latest_time()
//     // );

//     // let rg = Graph::new();

//     // for v in graph2.all_nodes() {
//     //     let gid = graph2.node_gid(v).unwrap();
//     //     match gid {
//     //         GID::Str(gid) => {
//     //             rg.add_node(0, gid, NO_PROPS).expect("add node failed");
//     //         }
//     //         _ => {
//     //             panic!("unexpected gid type")
//     //         }
//     //     }
//     // }

//     // let layer_id = 2; // netflow
//     // for edge in graph2.all_edges(layer_id) {
//     //     let src_id = edge.src();
//     //     let dst_id = edge.dst();

//     //     let src_gid = graph2.node_gid(src_id).unwrap();
//     //     let dst_gid = graph2.node_gid(dst_id).unwrap();

//     //     match (src_gid, dst_gid) {
//     //         (GID::Str(src), GID::Str(dst)) => {
//     //             rg.add_edge(0, src, dst, NO_PROPS, None)
//     //                 .expect("add edge failed");
//     //         }
//     //         _ => {
//     //             panic!("unexpected gid type")
//     //         }
//     //     }
//     // }

//     // println!("Graph has {} nodes", rg.count_nodes());
//     // println!("Graph has {} edges", rg.count_edges());

//     // let now = Instant::now();
//     // let ccs = weakly_connected_components(&l_graph, 100, None).group_by();
//     // println!("########## Arrow CC took {:?} ########## ", now.elapsed());

//     // let actual = ccs
//     //     .into_iter()
//     //     .map(|(cc, group)| (cc, Reverse(group.len())))
//     //     .sorted_by_key(|(key, count)| (*count, *key))
//     //     .map(|(cc, count)| (cc, count.0))
//     //     .take(2)
//     //     .collect::<Vec<_>>();

//     // println!("Top 10 connected components by size: {}", actual.len());
//     // for (cc, count) in actual {
//     //     println!("{}: {}", cc, count);
//     // }

//     // let now = Instant::now();
//     // let ccs = weakly_connected_components(&window, 100, None).group_by();
//     // println!(
//     //     "########## Window Arrow CC took {:?} ########## ",
//     //     now.elapsed()
//     // );

//     // let actual = ccs
//     //     .into_iter()
//     //     .map(|(cc, group)| (cc, Reverse(group.len())))
//     //     .sorted_by_key(|(key, count)| (*count, *key))
//     //     .map(|(cc, count)| (cc, count.0))
//     //     .take(2)
//     //     .collect::<Vec<_>>();

//     // println!("Top 10 connected components by size: {}", actual.len());
//     // for (cc, count) in actual {
//     //     println!("{}: {}", cc, count);
//     // }

//     // let now = Instant::now();
//     // let ccs = weakly_connected_components(&window2, 100, None).group_by();
//     // println!(
//     //     "########## Window Arrow CC took {:?} ########## ",
//     //     now.elapsed()
//     // );

//     // let actual = ccs
//     //     .into_iter()
//     //     .map(|(cc, group)| (cc, Reverse(group.len())))
//     //     .sorted_by_key(|(key, count)| (*count, *key))
//     //     .map(|(cc, count)| (cc, count.0))
//     //     .take(2)
//     //     .collect::<Vec<_>>();

//     // println!("Top 10 connected components by size: {}", actual.len());
//     // for (cc, count) in actual {
//     //     println!("{}: {}", cc, count);
//     // }

//     // let now = Instant::now();
//     // let ccs: std::collections::HashMap<u64, Vec<String>> =
//     //     weakly_connected_components(&rg, 100, None).group_by();
//     // println!("########## CC took {:?} ########## ", now.elapsed());

//     // let actual = ccs
//     //     .into_iter()
//     //     .map(|(cc, group)| (cc, Reverse(group.len())))
//     //     .sorted_by_key(|(key, count)| (*count, *key))
//     //     .map(|(cc, count)| (cc, count.0))
//     //     .take(2)
//     //     .collect::<Vec<_>>();

//     // println!("Top 10 connected components by size: {}", actual.len());
//     // for (cc, count) in actual {
//     //     println!("{}: {}", cc, count);
//     // }
}
