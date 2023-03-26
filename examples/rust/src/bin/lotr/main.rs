use docbrown_core::utils;
use docbrown_core::Prop;
use docbrown_db::view_api::*;
use docbrown_db::{csv_loader::csv::CsvLoader, graph::Graph};
use serde::Deserialize;
use std::path::PathBuf;
use std::{env, path::Path, time::Instant};
use docbrown_db::view_api::{GraphViewOps, VertexViewOps};

#[derive(Deserialize, std::fmt::Debug)]
pub struct Lotr {
    src_id: String,
    dst_id: String,
    time: i64,
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let default_data_dir: PathBuf = [env!("CARGO_MANIFEST_DIR"), "src/bin/lotr/data"]
        .iter()
        .collect();

    let data_dir = if args.len() < 2 {
        &default_data_dir
    } else {
        Path::new(args.get(1).unwrap())
    };

    if !data_dir.exists() {
        panic!("Missing data dir = {}", data_dir.to_str().unwrap())
    }

    let encoded_data_dir = data_dir.join("graphdb.bincode");

    let graph = if encoded_data_dir.exists() {
        let now = Instant::now();
        let g = Graph::load_from_file(encoded_data_dir.as_path())
            .expect("Failed to load graph from encoded data files");

        println!(
            "Loaded graph from encoded data files {} with {} vertices, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.num_vertices(),
            g.num_edges(),
            now.elapsed().as_secs()
        );

        g
    } else {
        let g = Graph::new(2);
        let now = Instant::now();

        let _ = CsvLoader::new(data_dir)
            .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
                let src_id = utils::calculate_hash(&lotr.src_id);
                let dst_id = utils::calculate_hash(&lotr.dst_id);
                let time = lotr.time;

                g.add_vertex(
                    time,
                    src_id,
                    &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                );
                g.add_vertex(
                    time,
                    src_id,
                    &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                );
                g.add_edge(
                    time,
                    src_id,
                    dst_id,
                    &vec![(
                        "name".to_string(),
                        Prop::Str("Character Co-occurrence".to_string()),
                    )],
                );
            })
            .expect("Failed to load graph from CSV data files");

        println!(
            "Loaded graph from CSV data files {} with {} vertices, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.num_vertices(),
            g.num_edges(),
            now.elapsed().as_secs()
        );

        g.save_to_file(encoded_data_dir)
            .expect("Failed to save graph");

        g
    };

    assert_eq!(graph.num_vertices(), 139);
    assert_eq!(graph.num_edges(), 701);

    let gandalf = utils::calculate_hash(&"Gandalf");

    assert_eq!(gandalf, 13840129630991083248);
    assert!(graph.has_vertex(gandalf));

    let windowed_graph = graph.window(i64::MIN, i64::MAX);
    let v = windowed_graph.vertex(gandalf).unwrap();

    assert_eq!(v.in_degree(), 24);
    assert_eq!(v.out_degree(), 35);
    assert_eq!(v.degree(), 49);

    let windowed_graph = graph.window(0, i64::MAX);
    let v = windowed_graph.vertex(gandalf).unwrap();

    assert_eq!(v.in_degree(), 24);
    assert_eq!(v.out_degree(), 35);
    assert_eq!(v.degree(), 49);

    let windowed_graph = graph.window(100, 9000);
    let v = windowed_graph.vertex(gandalf).unwrap();

    let actual = v
        .out_edges()
        .map(|e| (e.src().id(), e.dst().id()))
        .collect::<Vec<_>>();

    let expected = vec![
        (13840129630991083248, 6768237561757024290),
        (13840129630991083248, 2582862946330553552),
        (13840129630991083248, 13415634039873497660),
        (13840129630991083248, 357812470600089148),
        (13840129630991083248, 17764752901005380738),
        (13840129630991083248, 6484040860173734298),
        (0, 2914346725110218071),
        (0, 5956895584314169235),
        (0, 12936471037316398897),
        (0, 13050559475682228465),
        (0, 13789593425373656861),
        (0, 14223985880962197705),
    ];

    assert_eq!(actual, expected);

    let windowed_graph = graph.window(i64::MIN, i64::MAX);
    let v = windowed_graph.vertex(gandalf).unwrap();
    let actual = v
        .out_edges()
        .take(10)
        .map(|e| (e.src().id(), e.dst().id()))
        .collect::<Vec<_>>();

    let expected: Vec<(u64, u64)> = vec![
        (13840129630991083248, 12772980705568717046),
        (13840129630991083248, 6768237561757024290),
        (13840129630991083248, 11214194356141027632),
        (13840129630991083248, 2582862946330553552),
        (13840129630991083248, 13415634039873497660),
        (13840129630991083248, 6514938325906662882),
        (13840129630991083248, 13854913496482509346),
        (13840129630991083248, 357812470600089148),
        (13840129630991083248, 17764752901005380738),
        (13840129630991083248, 15044750458947305290),
    ];

    assert_eq!(actual, expected);

    let windowed_graph = graph.window(i64::MIN, i64::MAX);
    let actual = windowed_graph
        .vertices()
        .take(10)
        .map(|tv| tv.id())
        .collect::<Vec<u64>>();

    let expected: Vec<u64> = vec![
        13840129630991083248,
        12772980705568717046,
        8366058037510783370,
        11638942476191275730,
        6768237561757024290,
        13652678879212650868,
        10620258110842154986,
        12687378031997996522,
        11214194356141027632,
        2582862946330553552,
    ];

    assert_eq!(actual, expected);

    let windowed_graph = graph.window(0, 300);
    let actual = windowed_graph
        .vertices()
        .map(|v| v.id())
        .collect::<Vec<u64>>();

    let expected = vec![
        13840129630991083248,
        12772980705568717046,
        8366058037510783370,
        11638942476191275730,
        12936471037316398897,
        5956895584314169235,
        5402476312775412883,
        7320164159843417887,
    ];
    assert_eq!(actual, expected);
}
