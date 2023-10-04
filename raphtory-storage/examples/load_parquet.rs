use arrow2::array::{ListArray, PrimitiveArray, StructArray};
use itertools::Itertools;
use raphtory::core::{entities::VID, Direction};
use raphtory_storage::arrow::col_graph2::TempColGraphFragment;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("please supply a file to read");

    let src_col = std::env::args()
        .nth(2)
        .expect("please supply a source column");
    let dst_col = std::env::args()
        .nth(3)
        .expect("please supply a destination column");
    let time_col = std::env::args()
        .nth(4)
        .expect("please supply a time column");

    let graph_dir = std::env::args()
        .nth(5)
        .expect("please supply a graph output directory");

    let chunk_size = 131072;

    let now = std::time::Instant::now();

    let mut graph = if std::fs::read_dir(path.clone()).is_ok() {
        TempColGraphFragment::from_sorted_parquet_dir_edge_list(
            path, &src_col, &dst_col, &time_col, chunk_size, graph_dir,
        )
        .expect("failed to load graph")
    } else {
        TempColGraphFragment::from_sorted_parquet_edge_list(
            path, &src_col, &dst_col, &time_col, chunk_size, graph_dir,
        )
        .expect("failed to load graph")
    };

    println!("loading time: {:?}", now.elapsed());
    let g_num_verts = graph.num_vertices();
    assert!(graph
        .all_edges()
        .all(|(_, VID(src), VID(dst))| src < g_num_verts && dst < g_num_verts));

    for id in 0..graph.num_vertices() {
        let edges: Vec<_> = graph
            .edges(VID(id), Direction::OUT)
            .map(|(_, VID(vid))| vid)
            .collect();
        let sorted = edges.windows(2).all(|w| w[0] <= w[1]);
        if !sorted {
            println!("Not sorted: id={id}, edges={edges:?}");
        }
        assert!(sorted);
    }

    // assert!(graph.outbound()[59][0]
    //     .as_any()
    //     .downcast_ref::<ListArray<i64>>()
    //     .unwrap()
    //     .iter()
    //     .flatten()
    //     .flat_map(|list| list.as_any().downcast_ref::<StructArray>().cloned())
    //     .all(|list| {
    //         let values = list.values()[0]
    //             .as_any()
    //             .downcast_ref::<PrimitiveArray<u64>>()
    //             .unwrap()
    //             .values();
    //         let sorted = values.windows(2).all(|w| w[0] <= w[1]);
    //         if !sorted {
    //             println!("{:?}", values);
    //         }
    //         sorted
    //     }));

    let now = std::time::Instant::now();
    graph.build_inbound_adj_index().unwrap();
    println!("inbound edges index build in {:?}", now.elapsed());

    let now = std::time::Instant::now();
    let first_edges = graph.edges(VID(0), Direction::OUT).collect::<Vec<_>>();
    println!("first edges time: {:?}", now.elapsed());
    println!(
        "first edges: {:?}",
        first_edges.into_iter().take(10).collect::<Vec<_>>()
    );

    let now = std::time::Instant::now();
    let all_edges = graph.all_edges().count();
    println!("all edges time: {:?}", now.elapsed());
    println!("all edges: {}", all_edges);

    let now = std::time::Instant::now();
    let num_vertices = graph.num_vertices();
    println!("num vertices time: {:?}", now.elapsed());
    println!("num vertices: {}", num_vertices);
}
