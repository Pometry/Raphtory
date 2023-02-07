use docbrown_core::Prop;
use docbrown_db::{graphdb::GraphDB, loaders::csv::CsvLoader};
use serde::Deserialize;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};
use std::{
    env,
    path::Path,
    time::Instant,
};

#[derive(Deserialize, std::fmt::Debug)]
pub struct Lotr {
    src_id: String,
    dst_id: String,
    time: i64,
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // if not provided the data dir, pick from relative data directory
    // if bincode found, good otherwise build again

    let default_data_dir = String::from("./examples/src/bin/lotr/data");

    let data_dir = Path::new(if args.len() < 2 {
        &default_data_dir
    } else {
        args.get(1).unwrap()
    });

    if !data_dir.exists() {
        panic!("Missing data dir = {}", data_dir.to_str().unwrap())
    }

    let encoded_data_dir = data_dir.join("graphdb.bincode");

    let graph = if encoded_data_dir.exists() {
        let now = Instant::now();
        let g = GraphDB::load_from_file(encoded_data_dir.as_path())
            .expect("Failed to load graph from encoded data files");

        println!(
            "Loaded graph from encoded data files {} with {} vertices, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.len(),
            g.edges_len(),
            now.elapsed().as_secs()
        );

        g
    } else {
        let g = GraphDB::new(2);
        let now = Instant::now();

        let _ = CsvLoader::new(data_dir)
        .load_into_graph(&g, |lotr: Lotr, g: &GraphDB| {
            let src_id = calculate_hash(&lotr.src_id);
            let dst_id = calculate_hash(&lotr.dst_id);
            let time = lotr.time;

            g.add_vertex(
                src_id,
                time,
                &vec![("name".to_string(), Prop::Str("Character".to_string()))],
            );
            g.add_vertex(
                src_id,
                time,
                &vec![("name".to_string(), Prop::Str("Character".to_string()))],
            );
            g.add_edge(
                src_id,
                dst_id,
                time,
                &vec![(
                    "name".to_string(),
                    Prop::Str("Character Co-occurrence".to_string()),
                )],
            );
        }).expect("Failed to load graph from CSV data files");

        println!(
            "Loaded graph from CSV data files {} with {} vertices, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.len(),
            g.edges_len(),
            now.elapsed().as_secs()
        );

        g.save_to_file(encoded_data_dir).expect("Failed to save graph");

        g
    };

}
