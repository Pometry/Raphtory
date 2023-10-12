use clap::Parser;
use raphtory::{
    arrow::col_graph2::TempColGraphFragment,
    core::{entities::VID, Direction},
};

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// the path to the parquet files (if it is a directory, all parquet files in this directory are loaded in order)
    #[arg(short, long)]
    path: String,

    /// the name for the source hash column (used for sorting, optional and defaults to src_col)
    #[arg(long, default_value=None)]
    src_hash_col: Option<String>,

    /// the name for the source column
    #[arg(long, short)]
    src_col: String,

    /// the name for the destination hash column (used for sorting, optional and defaults to dst_col)
    #[arg(long, default_value=None)]
    dst_hash_col: Option<String>,

    /// the name for the destination column in the parquet files
    #[arg(short, long)]
    dst_col: String,

    /// the name for the time column in the parquet files
    #[arg(short, long)]
    time_col: String,

    /// the directory for storing the graph files
    #[arg(short, long)]
    graph_dir: String,

    /// clear the graph directory and reload
    #[arg(short, long, action)]
    force_reload: bool,

    #[arg(long, default_value_t = 1_048_576)]
    vertex_chunk_size: usize,

    #[arg(long, default_value_t = 1_048_576)]
    edge_chunk_size: usize,

    #[arg(long, default_value_t = 1024)]
    edge_max_list_size: usize,
}

fn main() {
    let args = Args::parse();
    let path = args.path;
    let src_hash_col = args.src_hash_col.unwrap_or_else(|| args.src_col.clone());
    let src_col = args.src_col;
    let dst_hash_col = args.dst_hash_col.unwrap_or_else(|| args.dst_col.clone());
    let dst_col = args.dst_col;
    let time_col = args.time_col;
    let graph_dir = args.graph_dir;
    let force_reload = args.force_reload;
    let vertex_chunk_size = args.vertex_chunk_size;
    let edge_chunk_size = args.edge_chunk_size;
    let edge_max_list_size = args.edge_max_list_size;
    // print all the params in a single line

    println!(
        "path: {path}, src_hash_col: {src_hash_col}, src_col: {src_col}, dst_hash_col: {dst_hash_col}, dst_col: {dst_col}, time_col: {time_col}, graph_dir: {graph_dir}, force_reload: {force_reload}"
    );

    if force_reload {
        std::fs::remove_dir_all(&graph_dir).unwrap();
    }

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
            edge_max_list_size,
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
            edge_max_list_size,
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
