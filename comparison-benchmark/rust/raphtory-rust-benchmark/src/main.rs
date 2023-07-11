use chrono::NaiveDateTime;
use clap::ArgAction;
use clap::Parser;
use csv::StringRecord;
use flate2::read::GzDecoder;
use raphtory::algorithms::connected_components::weakly_connected_components;
use raphtory::algorithms::pagerank::unweighted_page_rank;
use raphtory::graph_loader::fetch_file;
use raphtory::graph_loader::source::csv_loader::CsvLoader;
use raphtory::prelude::{AdditionOps, Graph, GraphViewOps, VertexViewOps, NO_PROPS};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None )]
struct Args {
    /// Set if the file has a header, default is False
    #[arg(long, action=ArgAction::SetTrue)]
    header: bool,

    /// Delimiter of the csv file
    #[arg(long, default_value = "\t")]
    delimiter: String,

    /// Path to a csv file
    #[arg(long, default_value = "")]
    file_path: String,

    /// Position of the from column in the csv
    #[arg(long, default_value = "0")]
    from_column: usize,

    /// Position of the to column in the csv
    #[arg(long, default_value = "1")]
    to_column: usize,

    /// Position of the time column in the csv, Expected time is in unix ms, default will ignore time and set it to 1
    #[arg(long, default_value = "-1")]
    time_column: i32,

    /// Download default files
    #[arg(long, action=ArgAction::SetTrue)]
    download: bool,

    /// Debug to print more info to the screen
    #[arg(long, action=ArgAction::SetTrue)]
    debug: bool,
}

fn main() {
    println!(
        "
██████╗ ███████╗███╗   ██╗ ██████╗██╗  ██╗███╗   ███╗ █████╗ ██████╗ ██╗  ██╗
██╔══██╗██╔════╝████╗  ██║██╔════╝██║  ██║████╗ ████║██╔══██╗██╔══██╗██║ ██╔╝
██████╔╝█████╗  ██╔██╗ ██║██║     ███████║██╔████╔██║███████║██████╔╝█████╔╝
██╔══██╗██╔══╝  ██║╚██╗██║██║     ██╔══██║██║╚██╔╝██║██╔══██║██╔══██╗██╔═██╗
██████╔╝███████╗██║ ╚████║╚██████╗██║  ██║██║ ╚═╝ ██║██║  ██║██║  ██║██║  ██╗
╚═════╝ ╚══════╝╚═╝  ╚═══╝ ╚═════╝╚═╝  ╚═╝╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝
"
    );
    let args = Args::parse();
    // Set default values
    let debug = args.debug;
    if debug {
        println!(
            "
  .___   .____  ____   .     .   ___        __   __   ___   .___   .____
 /   `  /      /   \\  /     / .'   \\       |    |  .'   `. /   `  /
 |    | |__.   |,_-<  |     | |            |\\  /|  |     | |    | |__.
 |    | |      |    ` |     | |    _       | \\/ |  |     | |    | |
 /---/  /----/ `----'  `._.'   `.___|      /    /   `.__.' /---/  /----/
            "
        );
        println!("Debug mode enabled.\nArguments: {:?}", args);
    }
    let header = args.header;
    let delimiter = args.delimiter;
    let file_path = args.file_path;
    let from_column = args.from_column;
    let to_column = args.to_column;
    let time_column = args.time_column;
    let download = args.download;

    if download {
        let url = "https://osf.io/download/nbq6h/";
        println!("Downloading default file from url {}...", url);
        // make err msg from url and custom string
        let err_msg = format!("Failed to download file from {}", url);
        let path = fetch_file("simple-relationships.csv.gz", true, url, 1200).expect(&err_msg);
        println!("Downloaded file to {}", path.to_str().unwrap());
        println!("Unpacking file...");
        // extract a file from a gz archive
        // Open the input .csv.gz file
        let input_file = File::open(&path).expect("Failed to open downloaded file");
        let gz_decoder = GzDecoder::new(input_file);
        let path_str = path.to_str().unwrap();
        let dst_file = if path_str.len() >= 3 {
            let new_length = path_str.len() - 3;
            path_str[..new_length].to_owned()
        } else {
            path_str.to_owned()
        };
        let mut output_file = File::create(&dst_file)
            .expect("Failed to create new file to decompress downloaded data");
        // Decompress and write the content to the output file
        let mut buffer = vec![0; 4096];
        let mut gz_reader = io::BufReader::new(gz_decoder);
        loop {
            let bytes_read = gz_reader.read(&mut buffer).unwrap_or(0);
            if bytes_read == 0 {
                break;
            }
            output_file.write_all(&buffer[..bytes_read]).unwrap();
        }

        // exit program
        println!("Downloaded+Unpacked file, please run again without --download flag and with --file-path={}", dst_file);
        return;
    }

    if file_path.is_empty() {
        println!("You did not set a file path");
        return;
    }
    if !Path::new(&file_path).exists() {
        println!("File path does not exist or is not a file {}", &file_path);
        return;
    }

    if debug {
        println!("Reading file {}", &file_path);
    }

    println!("Running setup...");
    let mut now = Instant::now();
    // Iterate over the CSV records
    let g = {
        let g = Graph::new();
        CsvLoader::new(file_path)
            .set_header(header)
            .set_delimiter(&delimiter)
            .load_rec_into_graph(&g, |generic_loader: StringRecord, g: &Graph| {
                let src_id = generic_loader
                    .get(from_column)
                    .map(|s| s.to_owned())
                    .unwrap();
                let dst_id = generic_loader.get(to_column).map(|s| s.to_owned()).unwrap();
                let mut edge_time = NaiveDateTime::from_timestamp_opt(1, 0).unwrap();
                if time_column != -1 {
                    edge_time = NaiveDateTime::from_timestamp_millis(
                        generic_loader
                            .get(time_column as usize)
                            .unwrap()
                            .parse()
                            .unwrap(),
                    )
                    .unwrap();
                }
                if debug {
                    println!("Adding edge {} -> {} at time {}", src_id, dst_id, edge_time);
                }
                g.add_edge(edge_time, src_id, dst_id, NO_PROPS, None)
                    .expect("Failed to add edge");
            })
            .expect("Failed to load graph from CSV data files");
        g
    };
    println!("Setup took {} seconds", now.elapsed().as_secs_f64());

    if debug {
        println!(
            "Graph has {} vertices and {} edges",
            g.num_vertices(),
            g.num_edges()
        )
    }

    // Degree of all nodes
    now = Instant::now();
    let _degree = g.vertices().iter().map(|v| v.degree()).collect::<Vec<_>>();
    println!("Degree: {} seconds", now.elapsed().as_secs_f64());

    // Out neighbours of all nodes with time
    now = Instant::now();
    let _out_neighbours = g
        .vertices()
        .iter()
        .map(|v| v.out_neighbours())
        .collect::<Vec<_>>();
    println!("Out neighbours: {} seconds", now.elapsed().as_secs_f64());

    // page rank with time
    now = Instant::now();
    let _page_rank: HashMap<String, f64> = unweighted_page_rank(&g, 1000, None, None, true)
        .into_iter()
        .collect();
    println!("Page rank: {} seconds", now.elapsed().as_secs_f64());

    // connected components with time
    now = Instant::now();
    let _cc: HashMap<String, u64> = weakly_connected_components(&g, usize::MAX, None);
    println!(
        "Connected components: {} seconds",
        now.elapsed().as_secs_f64()
    );
}
