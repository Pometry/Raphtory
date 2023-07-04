// search goes here

use std::{collections::HashSet, ops::Deref, sync::Arc};

use tantivy::{
    collector::TopDocs,
    schema::{Field, Schema, SchemaBuilder, FAST, INDEXED, STORED, TEXT},
    DateOptions, DocAddress, Document, Index, IndexReader, IndexSortByField,
};

use crate::{
    core::{
        entities::vertices::vertex_ref::VertexRef,
        utils::{errors::GraphError, time::TryIntoTime},
    },
    db::{api::mutation::internal::InternalAdditionOps, graph::vertex::VertexView},
    prelude::*,
};

use self::fields::{NAME, TIME, VERTEX_ID, VERTEX_ID_REV};

#[derive(Clone)]
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
    pub const TIME: &str = "time";
    pub const VERTEX_ID: &str = "vertex_id";
    pub const VERTEX_ID_REV: &str = "vertex_id_rev";
    pub const NAME: &str = "name";
}

const EMPTY: [(&str, Prop); 0] = [];

impl<G: GraphViewOps> From<G> for IndexedGraph<G> {
    fn from(graph: G) -> Self {
        Self::from_graph(&graph).expect("failed to generate index from graph")
    }
}

impl<G: GraphViewOps> IndexedGraph<G> {
    fn new_schema_builder() -> SchemaBuilder {
        let mut schema = Schema::builder();
        // we first add GID time, ID and ID_REV
        // ensure time is part of the index
        schema.add_i64_field(fields::TIME, INDEXED | STORED);
        // ensure we add vertex_id as stored to get back the vertex id after the search
        schema.add_u64_field(fields::VERTEX_ID, FAST | STORED);
        // reverse to sort by it
        schema.add_u64_field(fields::VERTEX_ID_REV, FAST | STORED);
        // add name
        schema.add_text_field(fields::NAME, TEXT);
        schema
    }

    fn schema_from_props<S: AsRef<str>, I: IntoIterator<Item = (S, Prop)>>(props: I) -> Schema {
        let mut schema = Self::new_schema_builder();

        for (prop_name, prop) in props.into_iter() {
            match prop {
                Prop::Str(_) => {
                    schema.add_text_field(prop_name.as_ref(), TEXT);
                }
                _ => todo!(),
            }
        }

        schema.build()
    }

    // we need to check every vertex for the properties and add them
    // to the schem depending on the type of the property
    //
    fn schema(g: &G) -> Schema {
        let mut schema = Self::new_schema_builder();

        // TODO: load all these from the graph at some point in the future
        let mut prop_names_set = g
            .all_vertex_prop_names(false)
            .into_iter()
            .chain(g.all_vertex_prop_names(true).into_iter())
            .collect::<HashSet<_>>();

        for vertex in g.vertices() {
            if prop_names_set.is_empty() {
                break;
            }
            let mut found_props = HashSet::from(["name".to_string()]);

            for prop in prop_names_set.iter() {
                // load temporal props
                for (_, prop_value) in vertex.property_history(prop.to_string()) {
                    if found_props.contains(prop) {
                        continue;
                    }
                    match prop_value {
                        Prop::Str(_) => {
                            schema.add_text_field(prop, TEXT);
                        }
                        Prop::DTime(_) => {
                            schema.add_date_field(prop, INDEXED);
                        }
                        x => todo!("prop value {:?} not supported yet", x),
                    }

                    found_props.insert(prop.to_string());
                }
                // load static props
                if let Some(prop_value) = vertex.static_property(prop.to_string()) {
                    match prop_value {
                        Prop::Str(_) => {
                            let name = if prop == "_id" { NAME } else { prop };
                            if !found_props.contains(name) {
                                println!("found_props {:?}", found_props);
                                println!("adding text field {:?}", name);
                                schema.add_text_field(name, TEXT);
                                found_props.insert(prop.to_string());
                            }
                        }
                        _ => todo!(),
                    }
                }
            }

            for found_prop in found_props {
                prop_names_set.remove(&found_prop);
            }
        }

        schema.build()
    }

    pub fn from_graph(g: &G) -> tantivy::Result<Self> {
        let schema = Self::schema(g);
        let (index, reader) = Self::new_index(schema.clone());

        let time_field = schema.get_field(fields::TIME)?;
        let vertex_id_field = schema.get_field(fields::VERTEX_ID)?;
        let vertex_id_rev_field = schema.get_field(fields::VERTEX_ID_REV)?;

        let mut writer = index.writer(100_000_000)?;

        for vertex in g.vertices() {
            let vertex_id: u64 = Into::<usize>::into(vertex.vertex) as u64;
            let temp_prop_names = vertex.property_names(false);

            for temp_prop_name in temp_prop_names {
                let prop_field = schema.get_field(&temp_prop_name)?;
                for (time, prop_value) in vertex.property_history(temp_prop_name) {
                    if let Prop::Str(prop_text) = prop_value {
                        let mut document = Document::new();
                        // add time to the document
                        document.add_i64(time_field, time);
                        // add the property to the document
                        document.add_text(prop_field, prop_text);
                        // add the vertex_id
                        document.add_u64(vertex_id_field, vertex_id);
                        document.add_u64(vertex_id_rev_field, u64::MAX - vertex_id);

                        writer.add_document(document)?;
                    }
                }
            }

            let prop_names = vertex.property_names(true);
            for prop_name in prop_names {
                let field_name = if prop_name == "_id" {
                    "name"
                } else {
                    &prop_name
                };

                let prop_field = schema.get_field(field_name)?;
                if let Some(prop_value) = vertex.static_property(prop_name.to_string()) {
                    if let Prop::Str(prop_text) = prop_value {
                        // what now?
                        let mut document = Document::new();
                        // add the property to the document
                        document.add_text(prop_field, prop_text);
                        // add the vertex_id
                        document.add_u64(vertex_id_field, vertex_id);
                        document.add_u64(vertex_id_rev_field, u64::MAX - vertex_id);

                        document.add_i64(time_field, i64::MAX);
                        writer.add_document(document)?;
                    }
                }
            }
            writer.commit()?;
        }

        reader.reload()?;
        Ok(IndexedGraph {
            graph: g.clone(),
            index: Arc::new(index),
            reader,
        })
    }

    fn new_index(schema: Schema) -> (Index, IndexReader) {
        let index_settings = tantivy::IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: VERTEX_ID.to_string(),
                order: tantivy::Order::Asc,
            }),
            ..tantivy::IndexSettings::default()
        };

        let index = Index::builder()
            .settings(index_settings)
            .schema(schema)
            .create_in_ram()
            .expect("failed to create index");

        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::OnCommit)
            .try_into()
            .unwrap();
        (index, reader)
    }

    pub fn new<S: AsRef<str>, I: IntoIterator<Item = (S, Prop)>>(graph: G, props: I) -> Self {
        let schema = Self::schema_from_props(props);

        let (index, reader) = Self::new_index(schema);

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
        let time = self.index.schema().get_field(TIME)?;
        document.add_i64(time, t);
        // add name to the document

        if let Some(vertex_name) = name {
            let name = self.index.schema().get_field(NAME)?;
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
        _t: i64,
        _src: u64,
        _dst: u64,
        _props: Vec<(String, Prop)>,
        _layer: Option<&str>,
    ) -> Result<(), GraphError> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use tantivy::{doc, DocAddress};

    use super::*;

    #[test]
    #[ignore = "this test is slow"]
    fn load_jira_graph() -> Result<(), GraphError> {
        let graph = Graph::load_from_file("/tmp/graphs/jira").expect("failed to load graph");
        assert!(graph.num_vertices() > 0);

        let index_graph: IndexedGraph<Graph> = graph.into();

        let issues = index_graph.search("name:'DEV-1690'", 5, 0)?;

        assert!(issues.len() >= 1);

        println!("issues: {:?}", issues);

        Ok(())
    }

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
            IndexedGraph::from_graph(&graph).expect("failed to generate index from graph");
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

        // search by name
        let results = indexed_graph
            .search("name:gollum", 10, 0)
            .expect("search failed");
        let actual = results.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        let expected = vec!["Gollum"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn add_vertex_search_by_name() {
        let graph = IndexedGraph::new(Graph::new(), EMPTY);

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
        let graph = IndexedGraph::new(Graph::new(), [("description", Prop::str(""))]);

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
        let graph = IndexedGraph::new(Graph::new(), [("description", Prop::str(""))]);

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
