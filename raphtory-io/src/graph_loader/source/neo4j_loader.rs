use neo4rs::*;
use raphtory::db::graph as rap;
/// A struct that defines the Neo4J loader with configurable options.
pub struct Neo4JConnection {
    // The created graph object given the arguments
    pub neo_graph: neo4rs::Graph,
}

impl Neo4JConnection {
    pub async fn new(
        uri: String,
        username: String,
        password: String,
        database: String,
    ) -> Result<Self> {
        let config = config()
            .uri(uri.as_str())
            .user(username.as_str())
            .password(password.as_str())
            .db(database.as_str())
            .build()?;
        let graph: neo4rs::Graph = Graph::connect(config).await?;
        Ok(Self { neo_graph: graph })
    }

    pub async fn run(&self, query: Query) -> Result<()> {
        Ok(self.neo_graph.run(query).await?)
    }

    pub async fn execute(&self, query: Query) -> Result<RowStream> {
        Ok(self.neo_graph.execute(query).await?)
    }

    pub async fn load_query_into_graph(
        &self,
        g: &rap::Graph,
        query: Query,
        loader: fn(Row, &rap::Graph),
    ) -> Result<()> {
        let mut result = self.neo_graph.execute(query).await.unwrap();

        while let Ok(Some(row)) = result.next().await {
            loader(row, g);
        }
        Ok(())
    }
}

#[cfg(test)]
mod neo_loader_test {
    use crate::graph_loader::source::neo4j_loader::Neo4JConnection;
    use neo4rs::*;
    use raphtory::core::Prop;
    use raphtory::db::graph as rap;
    use raphtory::db::view_api::GraphViewOps;

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
            .add_vertex(actor_born, actor_name.clone(), &vec![])
            .unwrap();
        graph
            .add_vertex_properties(
                actor_name.clone(),
                &vec![("type".into(), Prop::Str("actor".into()))],
            )
            .unwrap();
        graph
            .add_vertex(film_release, film_title.clone(), &vec![])
            .unwrap();
        graph
            .add_vertex_properties(
                film_title.clone(),
                &vec![
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
                &vec![],
                Some(relation_type.as_str()),
            )
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "Need to work out how to dummy neo for this"]
    async fn test_movie_db() {
        let neo = Neo4JConnection::new(
            "127.0.0.1:7687".to_string(),
            "neo4j".to_string(),
            "password".to_string(),
            "neo4j".to_string(),
        )
        .await
        .unwrap();
        let doc_graph = rap::Graph::new(1);

        neo.load_query_into_graph(
            &doc_graph,
            query("MATCH (actor:Person) -[relation]-> (film:Movie) RETURN actor,relation,film"),
            load_movies,
        )
        .await
        .unwrap();
        assert_eq!(2012, doc_graph.latest_time().unwrap());
        assert_eq!(1929, doc_graph.earliest_time().unwrap());
    }

    #[tokio::test]
    #[ignore = "Need to work out how to dummy neo for this"]
    async fn test_neo_connection() {
        let neo = Neo4JConnection::new(
            "127.0.0.1:7687".to_string(),
            "neo4j".to_string(),
            "password".to_string(),
            "neo4j".to_string(),
        )
        .await
        .unwrap();

        neo.run(query("CREATE (p:Person {id: $id})").param("id", 3))
            .await
            .unwrap();

        let mut result = neo
            .execute(query("MATCH (p:Person {id: $id}) RETURN p").param("id", 3))
            .await
            .unwrap();

        while let Ok(Some(row)) = result.next().await {
            dbg!(&row);
            let node: Node = row.get("p").unwrap();
            let id: i64 = node.get("id").unwrap();
            let label = node.labels().pop().unwrap();
            assert_eq!(id, 3);
            assert_eq!(label, "Person");
        }
    }
}
