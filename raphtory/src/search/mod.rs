// search goes here

use std::{collections::HashSet, ops::Deref, sync::Arc};

use tantivy::{
    collector::TopDocs,
    schema::{Field, Schema, FAST, INDEXED, STORED, TEXT},
    DocAddress, Document, Index, IndexSortByField,
};

use crate::{
    core::{errors::GraphError, time::TryIntoTime, vertex_ref::VertexRef},
    db::{mutation_api::internal::InternalAdditionOps, vertex::VertexView},
    prelude::*,
};

use self::fields::{VERTEX_ID, VERTEX_ID_REV};

pub struct IndexedGraph<G> {
    graph: G,
    index: Arc<Index>,
    reader: tantivy::IndexReader,
}

impl<G> Deref for IndexedGraph<G> {
    type Target = G;

    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

pub(in crate::search) mod fields {
    pub const VERTEX_GID: &str = "vertex_gid";
    pub const TIME: &str = "time";
    pub const VERTEX_ID: &str = "vertex_id";
    pub const VERTEX_ID_REV: &str = "vertex_id_rev";
}

impl<G: GraphViewOps> IndexedGraph<G> {
    pub fn from_graph<S: AsRef<str>, I: IntoIterator<Item = S>>(
        vertex_index_props: I,
        g: &G,
    ) -> tantivy::Result<Self> {
        let mut vec_props = vec![];
        let mut set_props = HashSet::new();
        for prop in vertex_index_props {
            if set_props.contains(prop.as_ref()) {
                continue;
            }
            set_props.insert(prop.as_ref().to_string());
            vec_props.push(prop.as_ref().to_string());
        }

        let mut index_graph = Self::from_empty_graph(g.clone(), vec_props);

        let time_field = index_graph.index.schema().get_field(fields::TIME)?;
        let vertex_id_field = index_graph.index.schema().get_field(fields::VERTEX_ID)?;
        let vertex_id_rev_field = index_graph
            .index
            .schema()
            .get_field(fields::VERTEX_ID_REV)?;
        let vertex_gid_field = index_graph.index.schema().get_field(fields::VERTEX_GID)?;

        let schema = index_graph.index.schema();
        let mut writer = index_graph.index.writer(100_000_000)?;

        for vertex in g.vertices() {
            let vertex_gid = vertex.id();
            let vertex_id: u64 = Into::<usize>::into(vertex.vertex) as u64;
            let temp_prop_names = vertex.property_names(false);

            for temp_prop_name in temp_prop_names {
                let prop_field = schema.get_field(&temp_prop_name)?;
                if set_props.contains(&temp_prop_name) {
                    for (time, prop_value) in vertex.property_history(temp_prop_name) {
                        if let Prop::Str(prop_text) = prop_value {
                            // what now?
                            let mut document = Document::new();
                            // add time to the document
                            document.add_i64(time_field, time);
                            // add the property to the document
                            document.add_text(prop_field, prop_text);
                            // add the vertex_id
                            document.add_u64(vertex_id_field, vertex_id);
                            document.add_u64(vertex_id_rev_field, u64::MAX - vertex_id);
                            // add the vertex_gid
                            document.add_text(vertex_gid_field, vertex_gid);

                            writer.add_document(document)?;
                        }
                    }
                }
            }
            writer.commit()?;
        }

        index_graph.graph = g.clone();

        Ok(index_graph)
    }

    pub fn from_empty_graph<S: AsRef<str>, I>(graph: G, vertex_index_props: I) -> Self
    where
        I: IntoIterator<Item = S>,
    {
        let mut schema = Schema::builder();

        for prop in vertex_index_props {
            schema.add_text_field(prop.as_ref(), TEXT);
        }

        // ensure time is part of the index
        schema.add_i64_field(fields::TIME, INDEXED | STORED);
        // ensure we add vertex_id as stored to get back the vertex id after the search
        schema.add_u64_field(fields::VERTEX_ID, FAST | STORED);
        // reverse to sort by it
        schema.add_u64_field(fields::VERTEX_ID_REV, FAST | STORED);
        // global id of the vertex
        schema.add_text_field(fields::VERTEX_GID, STORED);

        let index_settings = tantivy::IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: VERTEX_ID.to_string(),
                order: tantivy::Order::Asc,
            }),
            ..tantivy::IndexSettings::default()
        };

        let index = Index::builder()
            .settings(index_settings)
            .schema(schema.build())
            .create_in_ram()
            .expect("failed to create index");

        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::OnCommit)
            .try_into()
            .unwrap();

        IndexedGraph {
            graph,
            index: Arc::new(index),
            reader,
        }
    }

    pub fn reload(&self) -> Result<(), GraphError> {
        self.reader.reload()?;
        Ok(())
    }

    fn resolve_vertex_from_search_result(
        &self,
        vertex_id: Field,
        doc: Document,
    ) -> Option<VertexView<G>> {
        let vertex_id: usize = doc
            .get_first(vertex_id)
            .and_then(|value| value.as_u64())?
            .try_into()
            .ok()?;
        let vertex_id = VertexRef::Local(vertex_id.into());
        self.graph.vertex(vertex_id)
    }

    pub fn search(
        &self,
        q: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<VertexView<G>>, GraphError> {
        let searcher = self.reader.searcher();
        let query_parser = tantivy::query::QueryParser::for_index(&self.index, vec![]);
        let query = query_parser.parse_query(q)?;

        let ranking = TopDocs::with_limit(limit)
            .and_offset(offset)
            .order_by_u64_field(VERTEX_ID_REV.to_string());

        let top_docs: Vec<(u64, DocAddress)> = searcher.search(&query, &ranking)?;
        // let top_docs = searcher.search(&query, &ranking)?;

        let vertex_id = self.index.schema().get_field("vertex_id")?;

        let results = top_docs
            .into_iter()
            .map(|(_, doc_address)| searcher.doc(doc_address))
            .filter_map(Result::ok)
            .filter_map(|doc| self.resolve_vertex_from_search_result(vertex_id, doc))
            .collect::<Vec<_>>();

        Ok(results)
    }
}

impl<G: GraphViewOps + InternalAdditionOps> InternalAdditionOps for IndexedGraph<G> {
    fn internal_add_vertex(
        &self,
        t: i64,
        v: u64,
        name: Option<&str>,
        props: Vec<(String, Prop)>,
    ) -> Result<VertexRef, GraphError> {
        let t: i64 = t.try_into_time()?;
        let mut document = Document::new();
        // add time to the document
        let time = self.index.schema().get_field("time")?;
        document.add_i64(time, t);
        // add name to the document

        if let Some(vertex_name) = name {
            let name = self.index.schema().get_field("name")?;
            document.add_text(name, vertex_name);
        }

        // index all props that are declared in the schema
        for (prop_name, prop) in props.iter() {
            if let Ok(field) = self.index.schema().get_field(prop_name) {
                match prop {
                    Prop::Str(s) => document.add_text(field, s),
                    _ => {}
                }
            }
        }
        // add the vertex id to the document
        let v_ref = self.graph.internal_add_vertex(t, v, name, props)?;
        let v_id = self.graph.local_vertex_ref(v_ref).unwrap();
        // get the field from the index
        let vertex_id = self.index.schema().get_field(VERTEX_ID)?;
        let vertex_id_rev = self.index.schema().get_field(VERTEX_ID_REV)?;
        let index_v_id: u64 = Into::<usize>::into(v_id) as u64;

        document.add_u64(vertex_id, index_v_id);
        document.add_u64(vertex_id_rev, u64::MAX - index_v_id);

        let mut writer = self.index.writer(50_000_000)?;

        writer.add_document(document)?;

        writer.commit()?;

        Ok(v_ref)
    }

    fn internal_add_edge(
        &self,
        t: i64,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use tantivy::{DocAddress, doc};

    use super::*;

    #[test]
    fn create_indexed_graph_from_existing_graph() {
        let graph = Graph::new();

        graph
            .add_vertex(1, "Gandalf", [("kind".to_string(), Prop::str("Wizard"))])
            .expect("add vertex failed");

        graph
            .add_vertex(
                2,
                "Frodo",
                [
                    ("kind".to_string(), Prop::str("Hobbit")),
                    ("has_ring".to_string(), Prop::str("yes")),
                ],
            )
            .expect("add vertex failed");

        graph
            .add_vertex(2, "Merry", [("kind".to_string(), Prop::str("Hobbit"))])
            .expect("add vertex failed");

        graph
            .add_vertex(4, "Gollum", [("kind".to_string(), Prop::str("Creature"))])
            .expect("add vertex failed");

        graph
            .add_vertex(9, "Gollum", [("has_ring".to_string(), Prop::str("yes"))])
            .expect("add vertex failed");

        graph
            .add_vertex(9, "Frodo", [("has_ring".to_string(), Prop::str("no"))])
            .expect("add vertex failed");

        graph
            .add_vertex(10, "Frodo", [("has_ring".to_string(), Prop::str("yes"))])
            .expect("add vertex failed");

        graph
            .add_vertex(10, "Gollum", [("has_ring".to_string(), Prop::str("no"))])
            .expect("add vertex failed");

        let indexed_graph: IndexedGraph<Graph> =
            IndexedGraph::from_graph(["kind", "has_ring"], &graph)
                .expect("failed to generate index from graph");
        indexed_graph.reload().expect("failed to reload index");

        let results = indexed_graph
            .search("kind:hobbit", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Frodo", "Merry"];
        assert_eq!(actual, expected);

        let results = indexed_graph
            .search("kind:wizard", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];
        assert_eq!(actual, expected);

        let results = indexed_graph
            .search("kind:creature", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gollum"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn add_vertex_search_by_name() {

        let graph = IndexedGraph::from_empty_graph(Graph::new(), vec!["name"]);

        graph
            .add_vertex(1, "Gandalf", [])
            .expect("add vertex failed");

        graph.reload().expect("reload failed");

        let vertices = graph
            .search(r#"name:gandalf"#, 10, 0)
            .expect("search failed");

        let actual = vertices.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];

        assert_eq!(actual, expected);
    }

    #[test]
    fn add_vertex_search_by_description() {
        let graph = IndexedGraph::from_empty_graph(Graph::new(), vec!["name", "description"]);

        graph
            .add_vertex(
                1,
                "Bilbo",
                [("description".to_string(), Prop::str("A hobbit"))],
            )
            .expect("add vertex failed");

        graph
            .add_vertex(
                2,
                "Gandalf",
                [("description".to_string(), Prop::str("A wizard"))],
            )
            .expect("add vertex failed");

        graph.reload().expect("reload failed");
        // Find the Wizard
        let vertices = graph
            .search(r#"description:wizard"#, 10, 0)
            .expect("search failed");
        let actual = vertices.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];
        assert_eq!(actual, expected);
        // Find the Hobbit
        let vertices = graph
            .search(r#"description:'hobbit'"#, 10, 0)
            .expect("search failed");
        let actual = vertices.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Bilbo"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn add_vertex_search_by_description_and_time() {
        let graph = IndexedGraph::from_empty_graph(Graph::new(), ["name", "description"]);

        graph
            .add_vertex(
                1,
                "Gandalf",
                [("description".to_string(), Prop::str("The wizard"))],
            )
            .expect("add vertex failed");

        graph
            .add_vertex(
                2,
                "Saruman",
                [("description".to_string(), Prop::str("Another wizard"))],
            )
            .expect("add vertex failed");

        graph.reload().expect("reload failed");
        // Find Saruman
        let vertices = graph
            .search(r#"description:wizard AND time:[2 TO 5]"#, 10, 0)
            .expect("search failed");
        let actual = vertices.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Saruman"];
        assert_eq!(actual, expected);
        // Find Gandalf
        let vertices = graph
            .search(r#"description:'wizard' AND time:[1 TO 2}"#, 10, 0)
            .expect("search failed");
        let actual = vertices.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf"];
        assert_eq!(actual, expected);
        // Find both wizards
        let vertices = graph
            .search(r#"description:'wizard' AND time:[1 TO 100]"#, 10, 0)
            .expect("search failed");
        let actual = vertices.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gandalf", "Saruman"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn tantivy_101() {
        let vertex_index_props = vec!["name"];

        let mut schema = Schema::builder();

        for prop in vertex_index_props {
            schema.add_text_field(prop.as_ref(), TEXT);
        }

        // ensure time is part of the index
        schema.add_u64_field("time", INDEXED | STORED);
        // ensure we add vertex_id as stored to get back the vertex id after the search
        schema.add_text_field("vertex_id", FAST | STORED);

        let index = Index::create_in_ram(schema.build());

        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::OnCommit)
            .try_into()
            .unwrap();

        {
            let mut writer = index.writer(50_000_000).unwrap();

            let name = index.schema().get_field("name").unwrap();
            let time = index.schema().get_field("time").unwrap();
            let vertex_id = index.schema().get_field("vertex_id").unwrap();

            writer
                .add_document(doc!(name => "Gandalf", time => 1u64, vertex_id => 0u64))
                .expect("add document failed");

            writer.commit().expect("commit failed");
        }

        reader.reload().unwrap();

        let searcher = reader.searcher();

        let query_parser = tantivy::query::QueryParser::for_index(&index, vec![]);
        let query = query_parser.parse_query(r#"name:"gandalf""#).unwrap();

        let ranking = TopDocs::with_limit(10).order_by_u64_field(VERTEX_ID.to_string());
        let top_docs: Vec<(u64, DocAddress)> = searcher.search(&query, &ranking).unwrap();

        assert!(top_docs.len() > 0);
    }
}
