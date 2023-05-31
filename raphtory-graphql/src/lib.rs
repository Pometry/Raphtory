// pub use crate::data::Data;
pub use crate::model::algorithm::Algorithm;
pub use crate::server::RaphtoryServer;
// pub use crate::model::QueryRoot;

mod model;
mod observability;
mod routes;
mod server;

mod data;

// #[cfg(test)]
// mod graphql_test {
//     use super::*;
//     use dynamic_graphql::dynamic::DynamicRequestExt;
//     use dynamic_graphql::{App, FieldValue};
//     use std::collections::HashMap;
//     use std::env;
//
//     #[tokio::test]
//     async fn execute_dummy_algo() {
//         let graph = tokio::task::spawn_blocking(|| {
//             raphtory_io::graph_loader::example::lotr_graph::lotr_graph(1)
//         })
//         .await
//         .unwrap();
//         let graphs = HashMap::from([("lotr".to_string(), graph)]);
//         let data = Data { graphs };
//
//         // env::set_var("PLUGIN_DIRECTORY", "/tmp/plugins");
//
//         #[derive(App)]
//         struct App(QueryRoot);
//         let schema = App::create_schema().data(data).finish().unwrap();
//
//         let query = r#"
//         {
//           graph(name: "lotr") {
//             algorithms {
//               dummyalgo(mandatoryArg: 1) {
//                 message
//               }
//             }
//           }
//         }
//         "#;
//
//         // let query = r#"
//         // {
//         //   graph(name: "lotr") {
//         //     algorithms {
//         //       pagerank(iterCount: 20) {
//         //         rank
//         //       }
//         //     }
//         //   }
//         // }
//         // "#;
//
//         let root = QueryRoot;
//         let req = dynamic_graphql::Request::new(query).root_value(FieldValue::owned_any(root));
//
//         let res = schema.execute(req).await;
//         let data = res.data.into_json().unwrap();
//     }
// }
