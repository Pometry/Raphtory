use std::time::Instant;

use raphtory::{arrow::col_graph2::TempColGraphFragment, core::Direction};

fn main() {
    let graph_dir = std::env::args()
        .nth(1)
        .expect("please supply a graph directory");

    let graph = TempColGraphFragment::new(graph_dir).unwrap();
    let now = Instant::now();
    let mut count = 0usize;

    for (e1, n1) in graph.edges(0.into(), Direction::OUT){
        let edge1 = graph.edge(e1);
        let e1_ts = edge1.timestamps();
        // TODO: add filter on edge property of bytes

        for (e2, _) in graph.edges(n1, Direction::OUT) {
            let edge2 = graph.edge(e2);
            let e2_ts = edge2.timestamps();

            match (e2_ts.last(), e1_ts.first()) {
                (Some(e2_ts), Some(e1_ts)) => {
                    if e2_ts > e1_ts {
                        count+=1;
                    }
                }
                _ => {}
            }
        }
    }

    println!("Time taken: {:?}, counted {count}", now.elapsed());
}
