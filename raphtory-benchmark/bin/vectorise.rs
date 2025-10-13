use raphtory_benchmark::common::vectors::{
    create_graph_for_vector_bench, vectorise_graph_for_bench_async,
};
use std::time::SystemTime;

const MAX_TIME: u64 = 3600;

// async fn vectorise(graph: Graph) {
//     tokio::task::spawn_blocking(move || vectorise_graph_for_bench(graph));
// }

#[tokio::main]
async fn main() {
    println!("size,duration");
    let mut size = 100.0;
    loop {
        let rounded_size = size as usize;
        let graph = create_graph_for_vector_bench(rounded_size);
        let start = SystemTime::now();
        tokio::select! {
            _ = vectorise_graph_for_bench_async(graph) => {
                let duration = SystemTime::now().duration_since(start).unwrap().as_millis();
                println!("{rounded_size},{duration}");
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(MAX_TIME)) => {
                println!("here!!");
                break;
            }
        };
        size = size * f32::sqrt(2.0);
    }
}
