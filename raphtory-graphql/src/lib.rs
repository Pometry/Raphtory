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
    use raphtory::core::Prop;
    use raphtory::db::graph::Graph;
    use raphtory::db::mutation_api::AdditionOps;
    use std::collections::HashMap;
    use std::env;

    #[tokio::test]
    async fn basic_query() {
        let graph = Graph::new(1);
        if let Err(err) = graph.add_vertex(0, 11, []) {
            panic!("Could not add vertex! {:?}", err);
        }
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

    #[tokio::test]
    async fn query_nodefilter() {
        let graph = Graph::new(1);
        if let Err(err) = graph.add_vertex(0, "gandalf", []) {
            panic!("Could not add vertex! {:?}", err);
        }
        if let Err(err) = graph.add_vertex(0, "bilbo", []) {
            panic!("Could not add vertex! {:?}", err);
        }
        if let Err(err) = graph.add_vertex(0, "frodo", []) {
            panic!("Could not add vertex! {:?}", err);
        }

        let graphs = HashMap::from([("lotr".to_string(), graph)]);
        let data = data::Data { graphs };

        #[derive(App)]
        struct App(model::QueryRoot);
        let schema = App::create_schema().data(data).finish().unwrap();

        let gandalf_query = r#"
        {
          graph(name: "lotr") {
            nodes(filter: { name: { eq: "gandalf" } }) {
              name
            }
          }
        }
        "#;

        let root = model::QueryRoot;
        let req =
            dynamic_graphql::Request::new(gandalf_query).root_value(FieldValue::owned_any(root));

        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();

        assert_eq!(
            data,
            serde_json::json!({
                "graph": {
                    "nodes": [
                        {
                            "name": "gandalf"
                        }
                    ]
                }
            }),
        );

        let not_gandalf_query = r#"
        {
          graph(name: "lotr") {
            nodes(filter: { name: { ne: "gandalf" } }) {
              name
            }
          }
        }
        "#;

        let root = model::QueryRoot;
        let req = dynamic_graphql::Request::new(not_gandalf_query)
            .root_value(FieldValue::owned_any(root));

        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();

        assert_eq!(
            data,
            serde_json::json!({
                "graph": {
                    "nodes": [
                        { "name": "bilbo" },
                        { "name": "frodo" }
                    ]
                }
            }),
        );
    }

    #[tokio::test]
    async fn query_properties() {
        let graph = Graph::new(1);
        if let Err(err) = graph.add_vertex(0, "gandalf", []) {
            panic!("Could not add vertex! {:?}", err);
        }
        if let Err(err) = graph.add_vertex(
            0,
            "bilbo",
            [("food".to_string(), Prop::Str("lots".to_string()))],
        ) {
            panic!("Could not add vertex! {:?}", err);
        }
        if let Err(err) = graph.add_vertex(
            0,
            "frodo",
            [("food".to_string(), Prop::Str("some".to_string()))],
        ) {
            panic!("Could not add vertex! {:?}", err);
        }

        let graphs = HashMap::from([("lotr".to_string(), graph)]);
        let data = data::Data { graphs };

        #[derive(App)]
        struct App(model::QueryRoot);
        let schema = App::create_schema().data(data).finish().unwrap();

        let prop_has_key_filter = r#"
        {
          graph(name: "lotr") {
            nodes(filter: { propertyHas: {
                            key: "food"
                          }}) {
              name
            }
          }
        }
        "#;

        let root = model::QueryRoot;
        let req = dynamic_graphql::Request::new(prop_has_key_filter)
            .root_value(FieldValue::owned_any(root));

        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();

        assert_eq!(
            data,
            serde_json::json!({
                "graph": {
                    "nodes": [
                        { "name": "bilbo" },
                        { "name": "frodo" },
                    ]
                }
            }),
        );

        let prop_has_value_filter = r#"
        {
          graph(name: "lotr") {
            nodes(filter: { propertyHas: {
                            valueStr: "lots"
                          }}) {
              name
            }
          }
        }
        "#;

        let root = model::QueryRoot;
        let req = dynamic_graphql::Request::new(prop_has_value_filter)
            .root_value(FieldValue::owned_any(root));

        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();

        assert_eq!(
            data,
            serde_json::json!({
                "graph": {
                    "nodes": [
                        { "name": "bilbo" },
                    ]
                }
            }),
        );
    }
}
