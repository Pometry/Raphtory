use raphtory::core::{entities::VID, Direction};
use raphtory_storage::arrow::col_graph2::TempColGraphFragment;

fn main() {
    let file = std::env::args()
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

    let chunk_size = 1024;

    let now = std::time::Instant::now();

    let graph = TempColGraphFragment::from_sorted_parquet_edge_list(
        file, &src_col, &dst_col, &time_col, chunk_size, graph_dir,
    )
    .expect("failed to load graph");

    println!("loading time: {:?}", now.elapsed());

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
