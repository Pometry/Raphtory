use clap::{ArgAction, Parser};
use csv::StringRecord;
use flate2::read::GzDecoder;
use raphtory::{
    algorithms::{
        centrality::pagerank::unweighted_page_rank, components::weakly_connected_components,
    },
    graph_loader::fetch_file,
    io::csv_loader::CsvLoader,
    prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS},
};
use raphtory_api::core::utils::logging::global_debug_logger;
use std::{
    fs::File,
    io::{self, Read, Write},
    path::Path,
    time::Instant,
};
use tracing::{debug, info};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Set if the file has a header, default is False
    #[arg(long, action = ArgAction::SetTrue)]
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
    #[arg(long, action = ArgAction::SetTrue)]
    download: bool,

    /// Debug to print more info to the screen
    #[arg(long, action = ArgAction::SetTrue)]
    debug: bool,

    /// Set the number of locks for the node and edge storage
    #[arg(long)]
    num_shards: Option<usize>,
}

fn main() {
    global_debug_logger();
    info!(
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
        info!(
            "
  .___   .____  ____   .     .   ___        __   __   ___   .___   .____
 /   `  /      /   \\  /     / .'   \\       |    |  .'   `. /   `  /
 |    | |__.   |,_-<  |     | |            |\\  /|  |     | |    | |__.
 |    | |      |    ` |     | |    _       | \\/ |  |     | |    | |
 /---/  /----/ `----'  `._.'   `.___|      /    /   `.__.' /---/  /----/
            "
        );
        info!("Debug mode enabled.\nArguments: {:?}", args);
    }
    let header = args.header;
    let delimiter = args.delimiter;
    let file_path = args.file_path;
    let from_column = args.from_column;
    let to_column = args.to_column;
    let time_column = args.time_column;
    let download = args.download;
    let num_shards = args.num_shards;

    if download {
        let url = "https://osf.io/download/nbq6h/";
        info!("Downloading default file from url {}...", url);
        // make err msg from url and custom string
        let err_msg = format!("Failed to download file from {}", url);
        let path = fetch_file("simple-relationships.csv.gz", true, url, 1200).expect(&err_msg);
        info!("Downloaded file to {}", path.to_str().unwrap());
        info!("Unpacking file...");
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
        info!("Downloaded+Unpacked file, please run again without --download flag and with --file-path={}", dst_file);
        return;
    }

    if file_path.is_empty() {
        info!("You did not set a file path");
        return;
    }
    if !Path::new(&file_path).exists() {
        info!("File path does not exist or is not a file {}", &file_path);
        return;
    }

    if debug {
        debug!("Reading file {}", &file_path);
    }

    info!("Running setup...");
    let mut now = Instant::now();
    // Iterate over the CSV records
    let g = match num_shards {
        Some(num_shards) => {
            info!("Constructing graph with {num_shards} shards.");
            Graph::new_with_shards(num_shards)
        }
        None => Graph::new(),
    };
    CsvLoader::new(file_path)
        .set_header(header)
        .set_delimiter(&delimiter)
        .load_rec_into_graph(&g, |generic_loader: StringRecord, g: &Graph| {
            let src_id = generic_loader.get(from_column).unwrap();
            let dst_id = generic_loader.get(to_column).unwrap();
            let edge_time = if time_column != -1 {
                generic_loader
                    .get(time_column as usize)
                    .unwrap()
                    .parse()
                    .unwrap()
            } else {
                1i64
            };
            if debug {
                debug!("Adding edge {} -> {} at time {}", src_id, dst_id, edge_time);
            }
            g.add_edge(edge_time, src_id, dst_id, NO_PROPS, None)
                .expect("Failed to add edge");
        })
        .expect("Failed to load graph from CSV data files");
    info!("Setup took {} seconds", now.elapsed().as_secs_f64());

    if debug {
        //leaving this here for now, need to remove the flag and handle properly
        debug!(
            "Graph has {} nodes and {} edges",
            g.count_nodes(),
            g.count_edges()
        )
    }

    // Degree of all nodes
    now = Instant::now();
    let _degree = g.nodes().iter().map(|v| v.degree()).collect::<Vec<_>>();
    info!("Degree: {} seconds", now.elapsed().as_secs_f64());

    // Out neighbours of all nodes with time
    now = Instant::now();
    let nodes = &g.nodes();
    let _out_neighbours = nodes.iter().map(|v| v.out_neighbours()).collect::<Vec<_>>();
    info!("Out neighbours: {} seconds", now.elapsed().as_secs_f64());

    // page rank with time
    now = Instant::now();
    let _page_rank = unweighted_page_rank(&g, Some(1000), None, None, true, None);
    info!("Page rank: {} seconds", now.elapsed().as_secs_f64());

    // connected community_detection with time
    now = Instant::now();
    let _cc = weakly_connected_components(&g, usize::MAX, None);
    info!(
        "Connected community_detection: {} seconds",
        now.elapsed().as_secs_f64()
    );
}
