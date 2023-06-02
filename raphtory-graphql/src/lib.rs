pub use crate::model::algorithm::Algorithm;
pub use crate::server::RaphtoryServer;

mod model;
mod observability;
mod routes;
mod server;

mod data;

#[cfg(test)]
mod graphql_test {
    use super::*;
    use dynamic_graphql::dynamic::DynamicRequestExt;
    use dynamic_graphql::{App, FieldValue};
    use raphtory::db::graph::Graph;
    use std::collections::HashMap;
    use std::env;

    #[tokio::test]
    async fn basic_query() {
        let graph = Graph::new(1);
        graph.add_vertex(0, 11, &vec![]);
        let graphs = HashMap::from([("lotr".to_string(), graph)]);
        let data = data::Data { graphs };

        #[derive(App)]
        struct App(model::QueryRoot);
        let schema = App::create_schema().data(data).finish().unwrap();

        let query = r#"
        {
          graph(name: "lotr") {
            nodes {
              id
            }
          }
        }
        "#;

        let root = model::QueryRoot;
        let req = dynamic_graphql::Request::new(query).root_value(FieldValue::owned_any(root));

        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();

        assert_eq!(
            data,
            serde_json::json!({
                "graph": {
                    "nodes": [
                        {
                            "id": 11
                        }
                    ]
                }
            }),
        );
    }
}
