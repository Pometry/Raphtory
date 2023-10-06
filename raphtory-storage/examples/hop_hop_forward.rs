use raphtory::arrow::col_graph2::TempColGraphFragment;

fn main() {
    let graph_dir = std::env::args()
        .nth(1)
        .expect("please supply a graph directory");

    TempColGraphFragment::new(graph_dir).unwrap();
}
