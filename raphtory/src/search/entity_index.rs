use crate::{
    db::{
        api::{
            properties::internal::{ConstantPropertiesOps, TemporalPropertyViewOps},
            view::IndexSpec,
        },
        graph::node::NodeView,
    },
    errors::GraphError,
    prelude::GraphViewOps,
    search::{
        fields, get_props, new_index, property_index::PropertyIndex, register_default_tokenizers,
    },
};
use parking_lot::RwLock;
use raphtory_api::core::entities::{
    properties::{meta::Meta, prop::PropType, tprop::TPropOps},
    LayerIds, EID, VID,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage},
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
    borrow::Borrow,
    path::{Path, PathBuf},
    sync::Arc,
};
use tantivy::{
    schema::{Schema, SchemaBuilder, FAST, INDEXED, STORED},
    Index,
};

#[derive(Clone)]
pub struct EntityIndex {
    pub(crate) index: Arc<Index>,
    pub(crate) const_property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
    pub(crate) temporal_property_indexes: Arc<RwLock<Vec<Option<PropertyIndex>>>>,
}

impl EntityIndex {
    pub(crate) fn new(schema: Schema, path: &Option<PathBuf>) -> Result<Self, GraphError> {
        let path = path.as_ref().map(|p| p.join("fields"));
        let index = new_index(schema, &path)?;
        Ok(Self {
            index: Arc::new(index),
            const_property_indexes: Arc::new(RwLock::new(Vec::new())),
            temporal_property_indexes: Arc::new(RwLock::new(Vec::new())),
        })
    }

    fn load_from_path(path: &Path, is_edge: bool) -> Result<Self, GraphError> {
        let index = Index::open_in_dir(path.join("fields"))?;

        register_default_tokenizers(&index);

        let const_property_indexes =
            PropertyIndex::load_all(&path.join("const_properties"), is_edge)?;
        let temporal_property_indexes =
            PropertyIndex::load_all(&path.join("temporal_properties"), is_edge)?;

        Ok(Self {
            index: Arc::new(index),
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

    fn init_prop_indexes(
        &self,
        indexes: &Arc<RwLock<Vec<Option<PropertyIndex>>>>,
        prop_id: usize,
        prop_name: String,
        prop_type: PropType,
        index_path: Option<PathBuf>,
        add_schema_fields: fn(&mut SchemaBuilder),
        new_property: fn(Schema, &Option<PathBuf>) -> Result<PropertyIndex, GraphError>,
    ) -> Result<(), GraphError> {
        let mut indexes = indexes.write();
        // Resize the vector if needed
        if prop_id >= indexes.len() {
            indexes.resize(prop_id + 1, None);
        }
        // Create a new PropertyIndex if it doesn't exist
        if indexes[prop_id].is_none() {
            let mut schema_builder = PropertyIndex::schema_builder(&*prop_name, prop_type.clone());
            add_schema_fields(&mut schema_builder);
            let schema = schema_builder.build();
            let prop_index_path = index_path.as_deref().map(|p| p.join(prop_id.to_string()));
            let property_index = new_property(schema, &prop_index_path)?;
            indexes[prop_id] = Some(property_index);
        }
        Ok(())
    }

    pub(crate) fn index_node_const_props(
        &self,
        graph: &GraphStorage,
        index_spec: &IndexSpec,
        path: &Option<PathBuf>,
    ) -> Result<(), GraphError> {
        let props = &index_spec.node_const_props;
        let meta = graph.node_meta().const_prop_meta();
        for (prop_name, prop_id, prop_type) in get_props(props, meta) {
            self.init_prop_indexes(
                &self.const_property_indexes,
                prop_id,
                prop_name,
                prop_type,
                path.as_deref().map(|p| p.join("const_properties")),
                |schema| {
                    schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
                },
                PropertyIndex::new_node_property,
            )?;

            let indexes = self.const_property_indexes.read();
            if let Some(prop_index) = &indexes[prop_id] {
                let mut writer = prop_index.index.writer(50_000_000)?;
                (0..graph.count_nodes())
                    .into_par_iter()
                    .try_for_each(|v_id| {
                        let node = NodeView::new_internal(graph, VID(v_id));
                        if let Some(prop_value) = node.get_const_prop(prop_id) {
                            let prop_doc = prop_index
                                .create_node_const_property_document(v_id as u64, &prop_value)?;
                            writer.add_document(prop_doc)?;
                        }
                        Ok::<(), GraphError>(())
                    })?;

                writer.commit()?;
            }
        }
        Ok(())
    }

    pub(crate) fn index_node_temporal_props(
        &self,
        graph: &GraphStorage,
        index_spec: &IndexSpec,
        path: &Option<PathBuf>,
    ) -> Result<(), GraphError> {
        let props = &index_spec.node_temp_props;
        let meta = graph.node_meta().temporal_prop_meta();
        for (prop_name, prop_id, prop_type) in get_props(props, meta) {
            self.init_prop_indexes(
                &self.temporal_property_indexes,
                prop_id,
                prop_name,
                prop_type,
                path.as_deref().map(|p| p.join("temporal_properties")),
                |schema| {
                    schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
                    schema.add_u64_field(fields::SECONDARY_TIME, INDEXED | FAST | STORED);
                    schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
                },
                PropertyIndex::new_node_property,
            )?;

            let indexes = self.temporal_property_indexes.read();
            if let Some(prop_index) = &indexes[prop_id] {
                let mut writer = prop_index.index.writer(50_000_000)?;
                (0..graph.count_nodes())
                    .into_par_iter()
                    .try_for_each(|v_id| {
                        let node = NodeView::new_internal(graph, VID(v_id));
                        let node_id = usize::from(node.node) as u64;
                        for (t, prop_value) in node.temporal_iter(prop_id) {
                            let prop_doc = prop_index.create_node_temporal_property_document(
                                t.into(),
                                node_id,
                                &prop_value,
                            )?;
                            writer.add_document(prop_doc)?;
                        }

                        Ok::<(), GraphError>(())
                    })?;

                writer.commit()?;
            }
        }
        Ok(())
    }

    pub(crate) fn index_edge_const_props(
        &self,
        graph: &GraphStorage,
        index_spec: &IndexSpec,
        path: &Option<PathBuf>,
    ) -> Result<(), GraphError> {
        let props = &index_spec.edge_const_props;
        let meta = graph.edge_meta().const_prop_meta();
        for (prop_name, prop_id, prop_type) in get_props(props, meta) {
            self.init_prop_indexes(
                &self.const_property_indexes,
                prop_id,
                prop_name,
                prop_type,
                path.as_deref().map(|p| p.join("const_properties")),
                |schema| {
                    schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
                    schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
                },
                PropertyIndex::new_edge_property,
            )?;

            let indexes = self.const_property_indexes.read();
            if let Some(prop_index) = &indexes[prop_id] {
                let mut writer = prop_index.index.writer(50_000_000)?;
                (0..graph.count_edges())
                    .into_par_iter()
                    .try_for_each(|e_id| {
                        let edge = graph.core_edge(EID(e_id));
                        for (layer_id, prop_value) in
                            edge.constant_prop_iter(&LayerIds::All, prop_id)
                        {
                            let prop_doc = prop_index.create_edge_const_property_document(
                                e_id as u64,
                                layer_id,
                                prop_value.borrow(),
                            )?;
                            writer.add_document(prop_doc)?;
                        }
                        Ok::<(), GraphError>(())
                    })?;
                writer.commit()?;
            }
        }
        Ok(())
    }

    pub(crate) fn index_edge_temporal_props(
        &self,
        graph: &GraphStorage,
        index_spec: &IndexSpec,
        path: &Option<PathBuf>,
    ) -> Result<(), GraphError> {
        let props = &index_spec.edge_temp_props;
        let meta = graph.edge_meta().temporal_prop_meta();
        for (prop_name, prop_id, prop_type) in get_props(props, meta) {
            self.init_prop_indexes(
                &self.temporal_property_indexes,
                prop_id,
                prop_name,
                prop_type,
                path.as_deref().map(|p| p.join("temporal_properties")),
                |schema| {
                    schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
                    schema.add_u64_field(fields::SECONDARY_TIME, INDEXED | FAST | STORED);
                    schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
                    schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
                },
                PropertyIndex::new_edge_property,
            )?;

            let indexes = self.temporal_property_indexes.read();
            if let Some(prop_index) = &indexes[prop_id] {
                let mut writer = prop_index.index.writer(50_000_000)?;
                (0..graph.count_edges())
                    .into_par_iter()
                    .try_for_each(|e_id| {
                        let edge = graph.core_edge(EID(e_id));
                        for (layer_id, prop_value) in
                            edge.temporal_prop_iter(&LayerIds::All, prop_id)
                        {
                            for (t, prop_value) in prop_value.iter() {
                                let prop_doc = prop_index.create_edge_temporal_property_document(
                                    t,
                                    e_id as u64,
                                    layer_id,
                                    &prop_value,
                                )?;
                                writer.add_document(prop_doc)?;
                            }
                        }
                        Ok::<(), GraphError>(())
                    })?;
                writer.commit()?;
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
}
