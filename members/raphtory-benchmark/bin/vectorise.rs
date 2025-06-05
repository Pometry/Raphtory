use std::time::SystemTime;

use raphtory_benchmark::common::vectors::{
    create_graph_for_vector_bench, vectorise_graph_for_bench,
};

fn print_time(start: SystemTime, message: &str) {
    let duration = SystemTime::now().duration_since(start).unwrap().as_secs();
    println!("{message} - took {duration}s");
}

fn main() {
    for size in [1_000_000] {
        let graph = create_graph_for_vector_bench(size);
        let start = SystemTime::now();
        vectorise_graph_for_bench(graph);
        print_time(start, &format!(">>> vectorise {}k", size / 1000));
    }
}
