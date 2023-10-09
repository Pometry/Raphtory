use raphtory::{
    arrow::col_graph2::TempColGraphFragment,
    core::{entities::VID, Direction},
};

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("please supply a file to read");

    let src_hash_col = std::env::args()
        .nth(2)
        .expect("please supply a source column");

    let src_col = std::env::args()
        .nth(3)
        .expect("please supply a source column");

    let dst_hash_col = std::env::args()
        .nth(4)
        .expect("please supply a destination hashcolumn");

    let dst_col = std::env::args()
        .nth(5)
        .expect("please supply a destination hash column");

    let time_col = std::env::args()
        .nth(6)
        .expect("please supply a time column");

    let graph_dir = std::env::args()
        .nth(7)
        .expect("please supply a graph output directory");

    // print all the params in a single line

    println!(
        "path: {}, src_hash_col: {}, src_col: {}, dst_hash_col: {}, dst_col: {}, time_col: {}, graph_dir: {}",
        path, src_hash_col, src_col, dst_hash_col, dst_col, time_col, graph_dir
    );

    let vertex_chunk_size = 1_048_576;
    let edge_chunk_size = 1024;

    let now = std::time::Instant::now();

    let mut graph = if std::fs::read_dir(path.clone()).is_ok() {
        TempColGraphFragment::from_sorted_parquet_dir_edge_list(
            path,
            &src_col,
            &src_hash_col,
            &dst_col,
            &dst_hash_col,
            &time_col,
            Some(vec![
                "time",
                "_c1",
                "source",
                "destination",
                "_c4",
                "source_port",
                "destination_port",
                "_c7",
                "_c8",
                "_c9",
                "_c10",
            ]),
            vertex_chunk_size,
            edge_chunk_size,
            graph_dir,
        )
        .expect("failed to load graph")
    } else {
        TempColGraphFragment::from_sorted_parquet_edge_list(
            path,
            &src_col,
            &dst_col,
            &time_col,
            vertex_chunk_size,
            edge_chunk_size,
            graph_dir,
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
