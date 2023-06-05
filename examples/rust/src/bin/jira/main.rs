use raphtory::{core::Prop, db::{graph::Graph, view_api::internal::GraphViewInternalOps}};
use raphtory_io::graph_loader::source::json_loader::JsonLinesLoader;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Person{
    email_address: Option<String>,
    account_id: String,
    display_name: String
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Fields{
    created: chrono::DateTime<chrono::Utc>,
    creator: Person,
    reporter: Person,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct JiraIssue {
    id: String,
    key: String,
    fields: Fields,
}

fn main() {
    let file = std::env::args().nth(1).unwrap();
    let g = Graph::new(4);
    let loader = JsonLinesLoader::<JiraIssue>::new(file.into(), None);

    loader
        .load_into_graph(&g, |rec, g| {
            println!("{:?}", rec);
            g.add_vertex(
                rec.fields.created,
                rec.id,
                &vec![("name".to_string(), Prop::Str(rec.key))],
            )
        })
        .expect("Failed to load JSON file into graph");

    let num_vertices = g.vertices_len();
    println!("Number of vertices: {}", num_vertices);
}
