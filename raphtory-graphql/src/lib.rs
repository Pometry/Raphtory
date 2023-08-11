pub use crate::{model::algorithm::Algorithm, server::RaphtoryServer};

mod model;
mod observability;
mod routes;
pub mod server;

mod data;

#[cfg(test)]
mod graphql_test {
    use super::*;
    use crate::{data::Data, model::App};
    use dynamic_graphql::Request;
    use raphtory::{
        db::api::view::internal::{IntoDynamic, MaterializedGraph},
        prelude::*,
    };
    use serde_json::json;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[tokio::test]
    async fn search_for_gandalf_query() {
        let graph = Graph::new();
        graph
            .add_vertex(0, "Gandalf", [("kind".to_string(), Prop::str("wizard"))])
            .expect("Could not add vertex!");
        graph
            .add_vertex(0, "Frodo", [("kind".to_string(), Prop::str("Hobbit"))])
            .expect("Could not add vertex!");

        let graphs = HashMap::from([("lotr".to_string(), graph.into_dynamic())]);
        let data = data::Data::from_map(graphs);
        let schema = App::create_schema().data(data).finish().unwrap();

        let query = r#"
        {
          graph(name: "lotr") {
            search(query: "kind:wizard", limit: 10, offset: 0) {
              name
            }
          }
        }
        "#;
        let req = Request::new(query);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();

        assert_eq!(
            data,
            json!({
                "graph": {
                    "search": [
                        {
                            "name": "Gandalf"
                        }
                    ]
                }
            }),
        );
    }

    #[tokio::test]
    async fn basic_query() {
        let graph = Graph::new();
        graph
            .add_vertex(0, 11, NO_PROPS)
            .expect("Could not add vertex!");

        let graphs = HashMap::from([("lotr".to_string(), graph.into_dynamic())]);
        let data = data::Data::from_map(graphs);

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
        let req = Request::new(query);
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
        let graph = Graph::new();
        if let Err(err) = graph.add_vertex(0, "gandalf", NO_PROPS) {
            panic!("Could not add vertex! {:?}", err);
        }
        if let Err(err) = graph.add_vertex(0, "bilbo", NO_PROPS) {
            panic!("Could not add vertex! {:?}", err);
        }
        if let Err(err) = graph.add_vertex(0, "frodo", NO_PROPS) {
            panic!("Could not add vertex! {:?}", err);
        }

        let graphs = HashMap::from([("lotr".to_string(), graph.into_dynamic())]);
        let data = Data::from_map(graphs);

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

        let req = Request::new(gandalf_query);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();

        assert_eq!(
            data,
            json!({
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

        let req = Request::new(not_gandalf_query);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();

        assert_eq!(
            data,
            json!({
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
        let graph = Graph::new();
        if let Err(err) = graph.add_vertex(0, "gandalf", NO_PROPS) {
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

        let graphs = HashMap::from([("lotr".to_string(), graph.into_dynamic())]);
        let data = data::Data::from_map(graphs);

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

        let req = Request::new(prop_has_key_filter);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();

        assert_eq!(
            data,
            json!({
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

        let req = Request::new(prop_has_value_filter);
        let res = schema.execute(req).await;
        let data = res.data.into_json().unwrap();

        assert_eq!(
            data,
            json!({
                "graph": {
                    "nodes": [
                        { "name": "bilbo" },
                    ]
                }
            }),
        );
    }

    #[tokio::test]
    async fn test_mutation() {
        let test_dir = tempdir().unwrap();
        let g0 = Graph::new();
        let test_dir_path = test_dir.path().to_str().unwrap().replace(r#"\"#, r#"\\"#);
        let f0 = &test_dir.path().join("g0");
        let f1 = &test_dir.path().join("g1");
        g0.save_to_file(f0).unwrap();

        let g1 = Graph::new();
        g1.add_vertex(0, 1, NO_PROPS).unwrap();

        let g2 = Graph::new();
        g2.add_vertex(0, 2, NO_PROPS).unwrap();

        let data = Data::default();
        let schema = App::create_schema().data(data).finish().unwrap();

        let list_graphs = r#"
        {
          graphs {
            name
          }
        }"#;

        let list_nodes = |name: &str| {
            format!(
                r#"{{
                  graph(name: "{}") {{
                    nodes {{
                      id
                    }}
                  }}
                }}"#,
                name
            )
        };

        let load_all = &format!(
            r#"mutation {{
              loadGraphsFromPath(path: "{}")
            }}"#,
            test_dir_path
        );

        let load_new = &format!(
            r#"mutation {{
              loadNewGraphsFromPath(path: "{}")
            }}"#,
            test_dir_path
        );

        // only g0 which is empty
        let req = Request::new(load_all);
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"loadGraphsFromPath": ["g0"]}));

        let req = Request::new(list_graphs);
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"graphs": [{"name": "g0"}]}));

        let req = Request::new(list_nodes("g0"));
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"graph": {"nodes": []}}));

        // add g1 to folder and replace g0 with g2 and load new graphs
        g1.save_to_file(f1).unwrap();
        g2.save_to_file(f0).unwrap();
        let req = Request::new(load_new);
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"loadNewGraphsFromPath": ["g1"]}));

        // g0 is still empty
        let req = Request::new(list_nodes("g0"));
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"graph": {"nodes": []}}));

        // g1 has node 1
        let req = Request::new(list_nodes("g1"));
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"graph": {"nodes": [{"id": 1}]}}));

        // reload all graphs from folder
        let req = Request::new(load_all);
        schema.execute(req).await;

        // g0 now has node 2
        let req = Request::new(list_nodes("g0"));
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"graph": {"nodes": [{"id": 2}]}}));

        // g1 still has node 1
        let req = Request::new(list_nodes("g1"));
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        assert_eq!(res_json, json!({"graph": {"nodes": [{"id": 1}]}}));
    }

    #[tokio::test]
    async fn test_graph_injection() {
        let g: MaterializedGraph = Graph::new().into();
        let gb = serde_json::to_string(&g).unwrap();

        let data = Data::default();
        let schema = App::create_schema().data(data).finish().unwrap();

        let req = Request::new(format!(
            r#"
        mutation {{
          newGraphFromJson(
            name: "test",
            graph: "{}"
          )
        }}"#,
            gb
        ));
        let res = schema.execute(req).await;
        let res_json = res.data.into_json().unwrap();
        println!("{:?}", res_json);

        let req = Request::new(
            r#"{
          graph(name: "test") {
            nodes {
              id
            }
          }
        "#,
        );
        let res = schema.execute(req).await;
        println!("{:?}", res.errors);
        let res_json = res.data.into_json().unwrap();
        println!("{:?}", res_json)
    }
}
