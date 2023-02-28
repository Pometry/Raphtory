use docbrown_core::utils;
use docbrown_core::{Direction, Prop};
use docbrown_db::{graph::Graph, csv_loader::csv::CsvLoader};
use serde::Deserialize;
use std::path::PathBuf;
use std::{env, path::Path, time::Instant};

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
            g.len(),
            g.edges_len(),
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
            g.len(),
            g.edges_len(),
            now.elapsed().as_secs()
        );

        g.save_to_file(encoded_data_dir)
            .expect("Failed to save graph");

        g
    };

    assert_eq!(graph.len(), 139);
    assert_eq!(graph.edges_len(), 701);

    let gandalf = utils::calculate_hash(&"Gandalf");

    assert_eq!(gandalf, 13840129630991083248);
    assert!(graph.contains(gandalf));

    assert_eq!(graph.degree(gandalf, Direction::IN), 24);
    assert_eq!(graph.degree(gandalf, Direction::OUT), 35);
    assert_eq!(graph.degree(gandalf, Direction::BOTH), 49);

    assert_eq!(graph.degree_window(gandalf, 0, i64::MAX, Direction::IN), 24);
    assert_eq!(
        graph.degree_window(gandalf, 0, i64::MAX, Direction::OUT),
        35
    );
    assert_eq!(
        graph.degree_window(gandalf, 0, i64::MAX, Direction::BOTH),
        49
    );

    let actual = graph
        .neighbours_window(gandalf, 100, 9000, Direction::OUT)
        .map(|e| (e.src, e.dst, e.t, e.is_remote))
        .collect::<Vec<_>>();

    let expected = vec![
        (13840129630991083248, 6768237561757024290, None, false),
        (13840129630991083248, 2582862946330553552, None, false),
        (13840129630991083248, 13415634039873497660, None, false),
        (13840129630991083248, 357812470600089148, None, false),
        (13840129630991083248, 17764752901005380738, None, false),
        (13840129630991083248, 6484040860173734298, None, false),
        (0, 2914346725110218071, None, true),
        (0, 5956895584314169235, None, true),
        (0, 12936471037316398897, None, true),
        (0, 13050559475682228465, None, true),
        (0, 13789593425373656861, None, true),
        (0, 14223985880962197705, None, true),
    ];

    assert_eq!(actual, expected);

    let actual = graph
        .neighbours(gandalf, Direction::OUT)
        .take(10)
        .map(|e| (e.src, e.dst, e.t, e.is_remote))
        .collect::<Vec<_>>();

    let expected: Vec<(u64, u64, Option<i64>, bool)> = vec![
        (13840129630991083248, 12772980705568717046, None, false),
        (13840129630991083248, 6768237561757024290, None, false),
        (13840129630991083248, 11214194356141027632, None, false),
        (13840129630991083248, 2582862946330553552, None, false),
        (13840129630991083248, 13415634039873497660, None, false),
        (13840129630991083248, 6514938325906662882, None, false),
        (13840129630991083248, 13854913496482509346, None, false),
        (13840129630991083248, 357812470600089148, None, false),
        (13840129630991083248, 17764752901005380738, None, false),
        (13840129630991083248, 15044750458947305290, None, false),
    ];

    assert_eq!(actual, expected);

    let actual = graph
        .neighbours_window_t(gandalf, 3000, 5000, Direction::OUT)
        .map(|e| (e.src, e.dst, e.t, e.is_remote))
        .collect::<Vec<_>>();

    let expected: Vec<(u64, u64, Option<i64>, bool)> = vec![
        (13840129630991083248, 2582862946330553552, Some(3060), false),
        (0, 5956895584314169235, Some(3703), true),
        (0, 5956895584314169235, Some(3914), true),
    ];

    assert_eq!(actual, expected);

    let actual = graph
        .vertices()
        .take(10)
        .map(|tv| tv.g_id)
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

    let actual = graph
        .vertices_window(0, 300)
        .map(|v| v.g_id)
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
