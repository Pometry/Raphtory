use crate::core::Prop;
use crate::db::graph as rap;
use crate::db::mutation_api::{AdditionOps, PropertyAdditionOps};
use crate::graph_loader::source::neo4j_loader::Neo4JConnection;
use neo4rs::*;

fn load_movies(row: Row, graph: &rap::Graph) {
    let film: Node = row.get("film").unwrap();
    let film_title: String = film.get("title").unwrap();
    let film_tagline: String = film.get("tagline").unwrap_or("No tagline :(".to_string());
    let film_release: i64 = film.get("released").unwrap();

    let actor: Node = row.get("actor").unwrap();
    let actor_name: String = actor.get("name").unwrap();
    let actor_born: i64 = actor.get("born").unwrap_or(film_release);

    let relation: Relation = row.get("relation").unwrap();
    let relation_type = relation.typ();

    graph
        .add_vertex(actor_born, actor_name.clone(), [])
        .unwrap();
    graph
        .add_vertex_properties(
            actor_name.clone(),
            [("type".into(), Prop::Str("actor".into()))],
        )
        .unwrap();
    graph
        .add_vertex(film_release, film_title.clone(), [])
        .unwrap();
    graph
        .add_vertex_properties(
            film_title.clone(),
            [
                ("type".into(), Prop::Str("film".into())),
                ("tagline".into(), Prop::Str(film_tagline)),
            ],
        )
        .unwrap();
    graph
        .add_edge(
            film_release,
            actor_name,
            film_title,
            [],
            Some(relation_type.as_str()),
        )
        .unwrap();
}

pub async fn neo4j_movie_graph(
    uri: String,
    username: String,
    password: String,
    database: String,
    shards: usize,
) -> rap::Graph {
    let g = rap::Graph::new(shards);
    let neo = Neo4JConnection::new(uri, username, password, database)
        .await
        .unwrap();

    neo.load_query_into_graph(
        &g,
        query("MATCH (actor:Person) -[relation]-> (film:Movie) RETURN actor,relation,film"),
        load_movies,
    )
    .await
    .unwrap();
    g
}
