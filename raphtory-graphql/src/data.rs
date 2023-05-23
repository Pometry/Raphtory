use std::collections::HashMap;
use std::fs;
// use std::sync::Arc;

use raphtory::db::graph::Graph;
// use raphtory_io::graph_loader::source::csv_loader::CsvLoader;
// use serde::Deserialize;

// pub(crate) type TGraph = Graph;
//
// pub(crate) struct Metadata<G: GraphViewOps> {
//     parent_graph: Arc<G>,
// }
// #[derive(Deserialize, std::fmt::Debug)]
// pub struct Lotr {
//     src_id: String,
//     dst_id: String,
//     time: i64,
// }

pub(crate) struct Data {
    pub(crate) graphs: HashMap<String, Graph>,
}

impl Data {
    pub(crate) fn load(directory_path: &str) -> Self {
        let paths = fs::read_dir(directory_path)
            .unwrap_or_else(|_| panic!("error reading directory {directory_path}"));

        let graphs = paths
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                let graph = Graph::load_from_file(&path).ok()?;
                let filename = path.file_name()?.to_str()?.to_string();

                Some((filename, graph))
            })
            .collect();

        Self { graphs }
    }
}

// impl Metadata<TGraph> {
//     pub(crate) fn graph(&self) -> &TGraph {
//         &self.parent_graph
//     }
//
//     pub(crate) fn lotr() -> Self {
//         let graph = TGraph::new(4);
//
//         CsvLoader::new("/tmp/lotr.csv")
//             .load_into_graph(&graph, |lotr: Lotr, g: &Graph| {
//                 g.add_vertex(
//                     lotr.time,
//                     lotr.src_id.clone(),
//                     &vec![("type".to_string(), Prop::Str("Character".to_string()))],
//                 )
//                 .expect("Failed to add vertex");
//
//                 g.add_vertex(
//                     lotr.time,
//                     lotr.dst_id.clone(),
//                     &vec![("type".to_string(), Prop::Str("Character".to_string()))],
//                 )
//                 .expect("Failed to add vertex");
//
//                 g.add_edge(
//                     lotr.time,
//                     lotr.src_id.clone(),
//                     lotr.dst_id.clone(),
//                     &vec![(
//                         "type".to_string(),
//                         Prop::Str("Character Co-occurrence".to_string()),
//                     )],
//                     None,
//                 )
//                 .expect("Failed to add edge");
//             })
//             .expect("Failed to load csv into graph");
//
//         Self {
//             parent_graph: Arc::new(graph),
//         }
//     }
// }
//
// impl Default for Metadata<TGraph> {
//     fn default() -> Self {
//         Self {
//             parent_graph: Arc::new(TGraph::new(4)),
//         }
//     }
// }
