use crate::{
    core::{utils::errors::GraphError, Prop},
    search::{fields, new_index, property_index::PropertyIndex, register_default_tokenizers},
};
use parking_lot::RwLock;
use raphtory_api::core::{
    entities::properties::props::Meta, storage::timeindex::TimeIndexEntry, PropType,
};
use std::{borrow::Borrow, path::PathBuf, sync::Arc};
use tantivy::{
    schema::{Schema, SchemaBuilder, FAST, INDEXED, STORED},
    Index, IndexReader, IndexWriter, Term,
};

#[derive(Clone)]
pub struct EntityIndex {
    pub(crate) index: Arc<Index>,
    pub(crate) reader: IndexReader,
    pub(crate) const_property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    pub(crate) temporal_property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
}

impl EntityIndex {
    pub(crate) fn new(schema: Schema, path: &Option<PathBuf>) -> Result<Self, GraphError> {
        let path = path.as_ref().map(|p| p.join("fields"));
        let (index, reader) = new_index(schema, &path)?;
        Ok(Self {
            index: Arc::new(index),
            reader,
            const_property_indexes: Arc::new(RwLock::new(Vec::new())),
            temporal_property_indexes: Arc::new(RwLock::new(Vec::new())),
        })
    }

    fn load_from_path(path: &PathBuf, is_edge: bool) -> Result<Self, GraphError> {
        let index = Index::open_in_dir(path.join("fields"))?;

        register_default_tokenizers(&index);

        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()?;

        let const_property_indexes =
            PropertyIndex::load_all(&path.join("const_properties"), is_edge)?;
        let temporal_property_indexes =
            PropertyIndex::load_all(&path.join("temporal_properties"), is_edge)?;

        Ok(Self {
            index: Arc::new(index),
            reader,
            const_property_indexes: Arc::new(RwLock::new(const_property_indexes)),
            temporal_property_indexes: Arc::new(RwLock::new(temporal_property_indexes)),
        })
    }

    pub(crate) fn load_nodes_index_from_path(path: &PathBuf) -> Result<Self, GraphError> {
        EntityIndex::load_from_path(path, false)
    }

    pub(crate) fn load_edges_index_from_path(path: &PathBuf) -> Result<Self, GraphError> {
        EntityIndex::load_from_path(path, true)
    }

    fn get_property_writers(
        &self,
        prop_ids: impl Iterator<Item = usize>,
        property_indexes: &RwLock<Vec<Option<PropertyIndex>>>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        let indexes = property_indexes.read();

        let mut writers = Vec::new();
        writers.resize_with(indexes.len(), || None);
        for id in prop_ids {
            let writer = indexes[id]
                .as_ref()
                .map(|index| index.index.writer(50_000_000))
                .transpose()?;
            writers[id] = writer;
        }

        Ok(writers)
    }

    pub(crate) fn get_const_property_writers(
        &self,
        prop_ids: impl Iterator<Item = usize>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        self.get_property_writers(prop_ids, &self.const_property_indexes)
    }

    pub(crate) fn get_temporal_property_writers(
        &self,
        prop_ids: impl Iterator<Item = usize>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        self.get_property_writers(prop_ids, &self.temporal_property_indexes)
    }

    // We initialize the property indexes per property as and when we discover a new property while processing each node and edge update.
    // While when creating indexes for a graph already built, all nodes/edges properties are already known in advance,
    // which is why create all the property indexes upfront.
    fn initialize_property_indexes(
        &self,
        property_indexes: &RwLock<Vec<Option<PropertyIndex>>>,
        add_schema_fields: fn(&mut SchemaBuilder),
        new_property: fn(Schema, &Option<PathBuf>) -> Result<PropertyIndex, GraphError>,
        path: &Option<PathBuf>,
        props: &Vec<(String, usize, PropType)>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        let mut indexes = property_indexes.write();
        let mut writers: Vec<Option<IndexWriter>> = Vec::new();

        for (prop_name, prop_id, prop_type) in props {
            // Resize the vector if needed
            if prop_id >= &indexes.len() {
                indexes.resize(prop_id + 1, None);
            }
            // Resize the writers if needed
            if *prop_id >= writers.len() {
                writers.resize_with(*prop_id + 1, || None);
            }

            // Create a new PropertyIndex if it doesn't exist
            if indexes[*prop_id].is_none() {
                let mut schema_builder =
                    PropertyIndex::schema_builder(&*prop_name, prop_type.clone());
                add_schema_fields(&mut schema_builder);
                let schema = schema_builder.build();
                let prop_index_path = path.as_deref().map(|p| p.join(prop_id.to_string()));
                let property_index = new_property(schema, &prop_index_path)?;
                let writer = property_index.index.writer(50_000_000)?;

                writers[*prop_id] = Some(writer);
                indexes[*prop_id] = Some(property_index);
            }
        }

        Ok(writers)
    }

    pub(crate) fn initialize_node_const_property_indexes(
        &self,
        path: &Option<PathBuf>,
        node_const_props: &Vec<(String, usize, PropType)>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        self.initialize_property_indexes(
            &self.const_property_indexes,
            |schema| {
                schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
            },
            PropertyIndex::new_node_property,
            path,
            node_const_props,
        )
    }

    pub(crate) fn initialize_node_temporal_property_indexes(
        &self,
        path: &Option<PathBuf>,
        node_temp_props: &Vec<(String, usize, PropType)>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        self.initialize_property_indexes(
            &self.temporal_property_indexes,
            |schema| {
                schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::SECONDARY_TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
            },
            PropertyIndex::new_node_property,
            path,
            node_temp_props,
        )
    }

    pub(crate) fn initialize_edge_const_property_indexes(
        &self,
        path: &Option<PathBuf>,
        edge_const_props: &Vec<(String, usize, PropType)>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        self.initialize_property_indexes(
            &self.const_property_indexes,
            |schema| {
                schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
            },
            PropertyIndex::new_edge_property,
            path,
            edge_const_props,
        )
    }

    pub(crate) fn initialize_edge_temporal_property_indexes(
        &self,
        path: &Option<PathBuf>,
        edge_temp_props: &Vec<(String, usize, PropType)>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        self.initialize_property_indexes(
            &self.temporal_property_indexes,
            |schema| {
                schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::SECONDARY_TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
            },
            PropertyIndex::new_edge_property,
            path,
            edge_temp_props,
        )
    }

    pub(crate) fn delete_const_properties_index_docs(
        &self,
        entity_id: u64,
        writers: &mut [Option<IndexWriter>],
        props: impl Iterator<Item = usize>,
    ) -> Result<(), GraphError> {
        let indexes = self.const_property_indexes.read();
        for prop_id in props {
            if let Some(Some(writer)) = writers.get(prop_id) {
                if let Some(index) = &indexes[prop_id] {
                    let term = Term::from_field_u64(index.entity_id_field, entity_id);
                    writer.delete_term(term);
                }
            }
        }

        self.commit_writers(writers)?;
        Ok(())
    }

    pub(crate) fn index_node_const_properties(
        &self,
        node_id: u64,
        writers: &[Option<IndexWriter>],
        props: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Result<(), GraphError> {
        let indexes = self.const_property_indexes.read();
        for (prop_id, prop_value) in props {
            if let Some(Some(writer)) = writers.get(prop_id) {
                if let Some(index) = &indexes[prop_id] {
                    let prop_doc =
                        index.create_node_const_property_document(node_id, prop_value.borrow())?;
                    writer.add_document(prop_doc)?;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn index_node_temporal_properties(
        &self,
        time: TimeIndexEntry,
        node_id: u64,
        writers: &[Option<IndexWriter>],
        props: impl IntoIterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Result<(), GraphError> {
        let indexes = self.temporal_property_indexes.read();
        for (prop_id, prop) in props {
            if let Some(Some(writer)) = writers.get(prop_id) {
                if let Some(index) = &indexes[prop_id] {
                    let prop_doc = index.create_node_temporal_property_document(
                        time,
                        node_id,
                        prop.borrow(),
                    )?;
                    writer.add_document(prop_doc)?;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn index_edge_const_properties(
        &self,
        edge_id: u64,
        layer_id: usize,
        writers: &[Option<IndexWriter>],
        props: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Result<(), GraphError> {
        let indexes = self.const_property_indexes.read();
        for (prop_id, prop_value) in props {
            if let Some(Some(writer)) = writers.get(prop_id) {
                if let Some(index) = &indexes[prop_id] {
                    let prop_doc = index.create_edge_const_property_document(
                        edge_id,
                        layer_id,
                        prop_value.borrow(),
                    )?;
                    writer.add_document(prop_doc)?;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn index_edge_temporal_properties(
        &self,
        time: TimeIndexEntry,
        edge_id: u64,
        layer_id: usize,
        writers: &[Option<IndexWriter>],
        props: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Result<(), GraphError> {
        let indexes = self.temporal_property_indexes.read();
        for (prop_id, prop) in props {
            if let Some(Some(writer)) = writers.get(prop_id) {
                if let Some(index) = &indexes[prop_id] {
                    let prop_doc = index.create_edge_temporal_property_document(
                        time,
                        edge_id,
                        layer_id,
                        prop.borrow(),
                    )?;
                    writer.add_document(prop_doc)?;
                }
            }
        }

        Ok(())
    }

    fn fetch_property_index(
        &self,
        indexes: &Arc<RwLock<Vec<Option<PropertyIndex>>>>,
        prop_id: Option<usize>,
    ) -> Option<(Arc<PropertyIndex>, usize)> {
        prop_id.and_then(|id| {
            indexes
                .read()
                .get(id)
                .and_then(|opt| opt.as_ref())
                .cloned()
                .map(Arc::from)
                .map(|index| (index, id))
        })
    }

    pub(crate) fn get_const_property_index(
        &self,
        meta: &Meta,
        prop_name: &str,
    ) -> Result<Option<(Arc<PropertyIndex>, usize)>, GraphError> {
        Ok(self.fetch_property_index(
            &self.const_property_indexes,
            meta.const_prop_meta().get_id(prop_name),
        ))
    }

    pub(crate) fn get_temporal_property_index(
        &self,
        meta: &Meta,
        prop_name: &str,
    ) -> Result<Option<(Arc<PropertyIndex>, usize)>, GraphError> {
        Ok(self.fetch_property_index(
            &self.temporal_property_indexes,
            meta.temporal_prop_meta().get_id(prop_name),
        ))
    }

    pub(crate) fn commit_writers(
        &self,
        writers: &mut [Option<IndexWriter>],
    ) -> Result<(), GraphError> {
        for writer in writers {
            if let Some(writer) = writer {
                writer.commit()?;
            }
        }
        Ok(())
    }

    pub(crate) fn reload_const_property_indexes(&self) -> Result<(), GraphError> {
        let indexes = self.const_property_indexes.read();
        for index in indexes.iter().flatten() {
            index.reader.reload()?;
        }
        Ok(())
    }

    pub(crate) fn reload_temporal_property_indexes(&self) -> Result<(), GraphError> {
        let indexes = self.temporal_property_indexes.read();
        for index in indexes.iter().flatten() {
            index.reader.reload()?;
        }
        Ok(())
    }
}
