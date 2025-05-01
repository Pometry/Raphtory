use crate::{
    core::{utils::errors::GraphError, Prop},
    db::api::storage::graph::storage_ops::GraphStorage,
    search::{fields, new_index, property_index::PropertyIndex, register_default_tokenizers},
};
use itertools::Itertools;
use parking_lot::RwLock;
use raphtory_api::core::{
    entities::properties::props::{Meta, PropMapper},
    storage::{arc_str::ArcStr, dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
    PropType,
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

    pub(crate) fn create_property_index(
        &self,
        prop_id: MaybeNew<usize>,
        prop_name: &str,
        prop_type: &PropType,
        is_static: bool,
        add_const_schema_fields: fn(&mut SchemaBuilder),
        add_temporal_schema_fields: fn(&mut SchemaBuilder),
        new_property: fn(Schema, path: &Option<PathBuf>) -> Result<PropertyIndex, GraphError>,
        path: &Option<PathBuf>,
    ) -> Result<(), GraphError> {
        prop_id
            .if_new(|prop_id| {
                let mut prop_index_guard = if is_static {
                    self.const_property_indexes.write()
                } else {
                    self.temporal_property_indexes.write()
                };

                // Resize the vector if needed
                if prop_id >= prop_index_guard.len() {
                    prop_index_guard.resize(prop_id + 1, None);
                }

                let mut schema_builder =
                    PropertyIndex::schema_builder(&*prop_name, prop_type.clone());

                let path = if is_static {
                    add_const_schema_fields(&mut schema_builder);
                    path.as_deref().map(|p| p.join("const_properties"))
                } else {
                    add_temporal_schema_fields(&mut schema_builder);
                    path.as_deref().map(|p| p.join("temporal_properties"))
                };

                let schema = schema_builder.build();
                let prop_index_path = path.map(|p| p.join(prop_id.to_string()));
                let property_index = new_property(schema, &prop_index_path)?;
                prop_index_guard[prop_id] = Some(property_index);

                Ok::<_, GraphError>(())
            })
            .transpose()?;
        Ok(())
    }

    fn get_property_writers(
        &self,
        prop_ids: impl Iterator<Item = usize>,
        property_indexes: &RwLock<Vec<Option<PropertyIndex>>>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        let prop_index_guard = property_indexes.read();

        let mut writers = Vec::new();
        writers.resize_with(prop_index_guard.len(), || None);
        for id in prop_ids {
            let writer = prop_index_guard[id]
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
        graph: &GraphStorage,
        property_indexes: &RwLock<Vec<Option<PropertyIndex>>>,
        prop_keys: impl Iterator<Item = ArcStr>,
        get_property_meta: fn(&GraphStorage) -> &PropMapper,
        add_schema_fields: fn(&mut SchemaBuilder),
        new_property: fn(Schema, &Option<PathBuf>) -> Result<PropertyIndex, GraphError>,
        path: &Option<PathBuf>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        let prop_meta = get_property_meta(graph);
        let properties = prop_keys
            .filter_map(|k| {
                prop_meta.get_id(&*k).and_then(|prop_id| {
                    prop_meta
                        .get_dtype(prop_id)
                        .map(|prop_type| (k.to_string(), prop_id, prop_type))
                })
            })
            .collect_vec();

        let mut prop_index_guard = property_indexes.write();
        let mut writers: Vec<Option<IndexWriter>> = Vec::new();

        for (prop_name, prop_id, prop_type) in properties {
            // Resize the vector if needed
            if prop_id >= prop_index_guard.len() {
                prop_index_guard.resize(prop_id + 1, None);
            }

            // Create a new PropertyIndex if it doesn't exist
            if prop_index_guard[prop_id].is_none() {
                let mut schema_builder = PropertyIndex::schema_builder(&*prop_name, prop_type);
                add_schema_fields(&mut schema_builder);
                let schema = schema_builder.build();
                let prop_index_path = path.as_deref().map(|p| p.join(prop_id.to_string()));
                let property_index = new_property(schema, &prop_index_path)?;
                let writer = property_index.index.writer(50_000_000)?;

                writers.push(Some(writer));
                prop_index_guard[prop_id] = Some(property_index);
            }
        }

        Ok(writers)
    }

    pub(crate) fn initialize_node_const_property_indexes(
        &self,
        graph: &GraphStorage,
        prop_keys: impl Iterator<Item = ArcStr>,
        path: &Option<PathBuf>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        self.initialize_property_indexes(
            graph,
            &self.const_property_indexes,
            prop_keys,
            |g| g.node_meta().const_prop_meta(),
            |schema| {
                schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
            },
            PropertyIndex::new_node_property,
            path,
        )
    }

    pub(crate) fn initialize_node_temporal_property_indexes(
        &self,
        graph: &GraphStorage,
        prop_keys: impl Iterator<Item = ArcStr>,
        path: &Option<PathBuf>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        self.initialize_property_indexes(
            graph,
            &self.temporal_property_indexes,
            prop_keys,
            |g| g.node_meta().temporal_prop_meta(),
            |schema| {
                schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::SECONDARY_TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
            },
            PropertyIndex::new_node_property,
            path,
        )
    }

    pub(crate) fn initialize_edge_const_property_indexes(
        &self,
        graph: &GraphStorage,
        prop_keys: impl Iterator<Item = ArcStr>,
        path: &Option<PathBuf>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        self.initialize_property_indexes(
            graph,
            &self.const_property_indexes,
            prop_keys,
            |g| g.edge_meta().const_prop_meta(),
            |schema| {
                schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
            },
            PropertyIndex::new_edge_property,
            path,
        )
    }

    pub(crate) fn initialize_edge_temporal_property_indexes(
        &self,
        graph: &GraphStorage,
        prop_keys: impl Iterator<Item = ArcStr>,
        path: &Option<PathBuf>,
    ) -> Result<Vec<Option<IndexWriter>>, GraphError> {
        self.initialize_property_indexes(
            graph,
            &self.temporal_property_indexes,
            prop_keys,
            |g| g.edge_meta().temporal_prop_meta(),
            |schema| {
                schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::SECONDARY_TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
            },
            PropertyIndex::new_edge_property,
            path,
        )
    }

    pub(crate) fn delete_const_properties_index_docs(
        &self,
        entity_id: u64,
        writers: &mut [Option<IndexWriter>],
        props: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Result<(), GraphError> {
        let property_indexes = self.const_property_indexes.read();
        for (prop_id, _prop_value) in props {
            if let Some(Some(prop_writer)) = writers.get(prop_id) {
                if let Some(property_index) = &property_indexes[prop_id] {
                    let term = Term::from_field_u64(property_index.entity_id_field, entity_id);
                    prop_writer.delete_term(term);
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
        let property_indexes = self.const_property_indexes.read();
        for (prop_id, prop_value) in props {
            if let Some(Some(prop_writer)) = writers.get(prop_id) {
                if let Some(property_index) = &property_indexes[prop_id] {
                    let prop_doc = property_index
                        .create_node_const_property_document(node_id, prop_value.borrow())?;
                    prop_writer.add_document(prop_doc)?;
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
        let property_indexes = self.temporal_property_indexes.read();
        for (prop_id, prop) in props {
            if let Some(Some(prop_writer)) = writers.get(prop_id) {
                if let Some(property_index) = &property_indexes[prop_id] {
                    let prop_doc = property_index.create_node_temporal_property_document(
                        time,
                        node_id,
                        prop.borrow(),
                    )?;
                    prop_writer.add_document(prop_doc)?;
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
        let property_indexes = self.const_property_indexes.read();
        for (prop_id, prop_value) in props {
            if let Some(Some(prop_writer)) = writers.get(prop_id) {
                if let Some(property_index) = &property_indexes[prop_id] {
                    let prop_doc = property_index.create_edge_const_property_document(
                        edge_id,
                        layer_id,
                        prop_value.borrow(),
                    )?;
                    prop_writer.add_document(prop_doc)?;
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
        let property_indexes = self.temporal_property_indexes.read();
        for (prop_id, prop) in props {
            if let Some(Some(prop_writer)) = writers.get(prop_id) {
                if let Some(property_index) = &property_indexes[prop_id] {
                    let prop_doc = property_index.create_edge_temporal_property_document(
                        time,
                        edge_id,
                        layer_id,
                        prop.borrow(),
                    )?;
                    prop_writer.add_document(prop_doc)?;
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
        for writer_option in writers {
            if let Some(const_writer) = writer_option {
                const_writer.commit()?;
            }
        }
        Ok(())
    }

    pub(crate) fn reload_const_property_indexes(&self) -> Result<(), GraphError> {
        let const_indexes = self.const_property_indexes.read();
        for property_index_option in const_indexes.iter().flatten() {
            property_index_option.reader.reload()?;
        }
        Ok(())
    }

    pub(crate) fn reload_temporal_property_indexes(&self) -> Result<(), GraphError> {
        let temporal_indexes = self.temporal_property_indexes.read();
        for property_index_option in temporal_indexes.iter().flatten() {
            property_index_option.reader.reload()?;
        }
        Ok(())
    }
}
