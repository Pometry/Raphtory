use std::path::PathBuf;
use crate::{graph_loader::{fetch_file}, graph::Graph};
use docbrown_core::{Prop};
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use chrono::*;
use itertools::Itertools;

//https://snap.stanford.edu/data/soc-RedditHyperlinks.html
pub fn reddit_file(timeout:u64) -> Result<PathBuf, Box<dyn std::error::Error>> {
    fetch_file(
        "reddit.tsv",
        "https://snap.stanford.edu/data/soc-redditHyperlinks-body.tsv",
        timeout
    )
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
    where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

pub fn reddit_graph(shards: usize,timeout:u64) -> Graph {

    let graph = {
        let g = Graph::new(shards);

        if let Ok(path) = reddit_file(timeout){
            if let Ok(lines) = read_lines(path.as_path()) {
                // Consumes the iterator, returns an (Optional) String
                for line in lines.dropping(1) {
                    if let Ok(reddit) = line {
                        let reddit: Vec<&str> = reddit.split("	").collect();
                        let src_id = &reddit[0];
                        let dst_id = &reddit[1];
                        let post_id = reddit[2].to_string();

                        match NaiveDateTime::parse_from_str(reddit[3], "%Y-%m-%d %H:%M:%S"){
                            Ok(time) => {
                                let time = time.timestamp();
                                let post_label:i32 = reddit[4].parse::<i32>().unwrap();
                                let post_properties: Vec<f64> =
                                    reddit[5].split(",")
                                        .map(|s| s.parse::<f64>().unwrap())
                                        .collect();
                                let edge_properties = &vec![
                                    ("post_label".to_string(),Prop::I32(post_label)),
                                    ("post_id".to_string(),Prop::Str(post_id)),
                                     ("word_count".to_string(),Prop::F64(post_properties[7])),
                                     ("long_words".to_string(),Prop::F64(post_properties[9])),
                                      ("sentences".to_string(),Prop::F64(post_properties[13])),
                                      ("readability".to_string(),Prop::F64(post_properties[17])),
                                      ("positive_sentiment".to_string(),Prop::F64(post_properties[17])),
                                      ("negative_sentiment".to_string(),Prop::F64(post_properties[17])),
                                      ("compound_sentiment".to_string(),Prop::F64(post_properties[17]))
                                ];
                                g.add_vertex(
                                    time,
                                    src_id.clone(),
                                    &vec![],
                                ).map_err(|err| println!("{:?}", err)).ok();
                                g.add_vertex(
                                    time,
                                    dst_id.clone(),
                                    &vec![],
                                ).map_err(|err| println!("{:?}", err)).ok();
                                g.add_edge(
                                    time,
                                    src_id.clone(),
                                    dst_id.clone(),
                                    edge_properties,
                                );
                            }
                            Err(e) => {println!("{}",e)}
                        }

                    }
                }
            }
        };

        g
    };
    graph
}
// #[cfg(test)]
// mod reddit_test {
//     use crate::graph_loader::reddit_hyperlinks::reddit_graph;
//     use crate::view_api::GraphViewOps;
//
//     #[test]
//     fn check_data() {
//         if let Ok(g) = reddit_graph(1, 600) {
//             println!("{} {}",g.num_vertices(),g.num_edges());
//         };
//     }
// }
//

