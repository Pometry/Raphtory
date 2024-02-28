use std::fmt::Debug;

#[cfg(test)]
mod tests {
    use crate::lanl::loader::{load_from_dir, load_from_parquet};
    use crate::lanl::{measure, query1, query2, query3, query3b, query3c, query4};

    #[test]
    fn test_query1() {
        let graph_dir = "target";
        let parquet_files = vec![
            "data/netflowsorted/nft_sorted",
            "data/netflowsorted/v1_sorted",
            "data/netflowsorted/v2_sorted",
        ];

        let graph = match load_from_dir(graph_dir) {
            Ok(g) => g,
            Err(e) => {
                println!("Failed to load the graph from the directory. Attempting to load from parquet files: {}", e);
                load_from_parquet(graph_dir, parquet_files).expect("Failed to load the graph from parquet files")
            }
        };

        println!("Node count {}", graph.num_nodes());
        println!("Edge count {}", graph.num_edges(0));
        println!("Earliest time {}", graph.earliest());
        println!("Latest time {}", graph.latest());

        measure("Query 1", || query1::run(&graph).unwrap());
        measure("Query 2", || query2::run(&graph).unwrap());
        measure("Query 3", || query3::run(&graph).unwrap());
        measure("Query 3b", || query3b::run(&graph).unwrap());
        // # measure("Query 3c", || query3c::run(&graph).unwrap());
        measure("Query 4", || query4::run2(&graph).unwrap());
    }
}
