use crate::{
    errors::GraphError,
    prelude::*,
    search::{fields, new_index, TOKENIZER},
};
use raphtory_api::core::{
    entities::properties::prop::PropType, storage::timeindex::TimeIndexEntry,
};
use std::{fs, path::PathBuf, sync::Arc};
use tantivy::{
    collector::TopDocs,
    query::AllQuery,
    schema::{
        Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, Type,
        FAST, INDEXED, STRING, TEXT,
    },
    Document, Index, IndexReader, TantivyDocument,
};

#[derive(Clone)]
pub struct PropertyIndex {
    pub(crate) index: Arc<Index>,
    pub(crate) time_field: Option<Field>,
    pub(crate) secondary_time_field: Option<Field>,
    pub(crate) layer_field: Option<Field>,
    pub(crate) entity_id_field: Field,
}

impl PropertyIndex {
    fn fetch_fields(
        schema: &Schema,
        is_edge: bool,
    ) -> Result<(Option<Field>, Option<Field>, Option<Field>, Field), GraphError> {
        let time_field = schema
            .get_field(fields::TIME)
            .map_err(|_| GraphError::IndexErrorMsg("Missing required field: TIME".into()))
            .ok();

        let secondary_time_field = schema
            .get_field(fields::SECONDARY_TIME)
            .map_err(|_| GraphError::IndexErrorMsg("Missing required field: SECONDARY_TIME".into()))
            .ok();

        let layer_field = if is_edge {
            Some(schema.get_field(fields::LAYER_ID).map_err(|_| {
                GraphError::IndexErrorMsg("Missing required field: LAYER_ID".into())
            })?)
        } else {
            None
        };

        let entity_id_field = schema
            .get_field(if is_edge {
                fields::EDGE_ID
            } else {
                fields::NODE_ID
            })
            .map_err(|_| {
                GraphError::IndexErrorMsg(format!(
                    "Missing required field: {}",
                    if is_edge {
                        fields::EDGE_ID
                    } else {
                        fields::NODE_ID
                    }
                ))
            })?;

        Ok((
            time_field,
            secondary_time_field,
            layer_field,
            entity_id_field,
        ))
    }

    fn new_property(
        schema: Schema,
        is_edge: bool,
        path: &Option<PathBuf>,
    ) -> Result<Self, GraphError> {
        let (time_field, secondary_time_field, layer_field, entity_id_field) =
            Self::fetch_fields(&schema, is_edge)?;

        let index = new_index(schema, path)?;

        Ok(Self {
            index: Arc::new(index),
            time_field,
            secondary_time_field,
            layer_field,
            entity_id_field,
        })
    }

    pub(crate) fn new_node_property(
        schema: Schema,
        path: &Option<PathBuf>,
    ) -> Result<Self, GraphError> {
        Self::new_property(schema, false, path)
    }

    pub(crate) fn new_edge_property(
        schema: Schema,
        path: &Option<PathBuf>,
    ) -> Result<Self, GraphError> {
        Self::new_property(schema, true, path)
    }

    fn load_from_path(path: &PathBuf, is_edge: bool) -> Result<Self, GraphError> {
        let index = Index::open_in_dir(path)?;
        let schema = index.schema();
        let (time_field, secondary_time_field, layer_field, entity_id_field) =
            Self::fetch_fields(&schema, is_edge)?;

        Ok(Self {
            index: Arc::new(index),
            time_field,
            secondary_time_field,
            layer_field,
            entity_id_field,
        })
    }

    pub(crate) fn load_all(path: &PathBuf, is_edge: bool) -> Result<Vec<Option<Self>>, GraphError> {
        if !path.exists() {
            return Ok(vec![]);
        }

        let mut result = vec![];
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                    if let Ok(prop_id) = file_name.parse::<usize>() {
                        let prop_index = Self::load_from_path(&path, is_edge)?;

                        if result.len() <= prop_id {
                            result.resize(prop_id + 1, None);
                        }

                        result[prop_id] = Some(prop_index);
                    }
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn print(&self) -> Result<(), GraphError> {
        let searcher = self.get_reader()?.searcher();
        let top_docs = searcher.search(&AllQuery, &TopDocs::with_limit(100))?;
        println!("Total property doc count: {}", top_docs.len());
        for (_score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("Property doc: {:?}", doc.to_json(searcher.schema()));
        }

        Ok(())
    }

    pub(crate) fn schema_builder(prop_name: &str, prop_type: PropType) -> SchemaBuilder {
        let mut schema_builder = Schema::builder();

        match prop_type {
            PropType::Str => {
                schema_builder.add_text_field(prop_name, STRING);
                schema_builder.add_text_field(
                    format!("{prop_name}_tokenized").as_ref(),
                    TextOptions::default()
                        .set_indexing_options(
                            TextFieldIndexing::default()
                                .set_tokenizer(TOKENIZER)
                                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                        )
                        .set_stored(),
                );
            }
            PropType::DTime => {
                schema_builder.add_date_field(prop_name, INDEXED | FAST);
            }
            PropType::U8 => {
                schema_builder.add_u64_field(prop_name, INDEXED | FAST);
            }
            PropType::U16 => {
                schema_builder.add_u64_field(prop_name, INDEXED | FAST);
            }
            PropType::U64 => {
                schema_builder.add_u64_field(prop_name, INDEXED | FAST);
            }
            PropType::I64 => {
                schema_builder.add_i64_field(prop_name, INDEXED | FAST);
            }
            PropType::I32 => {
                schema_builder.add_i64_field(prop_name, INDEXED | FAST);
            }
            PropType::F64 => {
                schema_builder.add_f64_field(prop_name, INDEXED | FAST);
            }
            PropType::F32 => {
                schema_builder.add_f64_field(prop_name, INDEXED | FAST);
            }
            PropType::Bool => {
                schema_builder.add_bool_field(prop_name, INDEXED | FAST);
            }
            _ => {
                schema_builder.add_text_field(prop_name, TEXT | FAST);
            }
        }

        schema_builder
    }

    pub fn get_prop_field(&self, prop_name: &str) -> tantivy::Result<Field> {
        self.index.schema().get_field(prop_name)
    }

    pub fn get_tokenized_prop_field(&self, prop_name: &str) -> tantivy::Result<Field> {
        self.index
            .schema()
            .get_field(format!("{prop_name}_tokenized").as_ref())
    }

    pub fn get_prop_field_type(&self, prop_name: &str) -> tantivy::Result<Type> {
        Ok(self
            .index
            .schema()
            .get_field_entry(self.index.schema().get_field(prop_name)?)
            .field_type()
            .value_type())
    }

    fn add_property_value_to_doc(document: &mut TantivyDocument, field: Field, prop_value: &Prop) {
        match prop_value.clone() {
            Prop::Str(v) => {
                document.add_text(field, v.clone());
                document.add_text(Field::from_field_id(1), v);
            }
            Prop::NDTime(v) => {
                if let Some(time) = v.and_utc().timestamp_nanos_opt() {
                    document.add_date(field, tantivy::DateTime::from_timestamp_nanos(time));
                }
            }
            Prop::U8(v) => document.add_u64(field, u64::from(v)),
            Prop::U16(v) => document.add_u64(field, u64::from(v)),
            Prop::U64(v) => document.add_u64(field, v),
            Prop::I64(v) => document.add_i64(field, v),
            Prop::I32(v) => document.add_i64(field, v as i64),
            Prop::F64(v) => document.add_f64(field, v),
            Prop::F32(v) => document.add_f64(field, v as f64),
            Prop::Bool(v) => document.add_bool(field, v),
            prop => document.add_text(field, prop.to_string()),
        }
    }

    fn create_property_document(
        &self,
        field_entity_id: Field,
        entity_id: u64,
        time: Option<TimeIndexEntry>,
        layer_id: Option<usize>,
        prop_value: &Prop,
    ) -> tantivy::Result<TantivyDocument> {
        let field_property = Field::from_field_id(0);

        let mut document = TantivyDocument::new();
        document.add_u64(field_entity_id, entity_id);

        if let (Some(time), Some(field_time), Some(secondary_time_field)) =
            (time, self.time_field, self.secondary_time_field)
        {
            document.add_i64(field_time, time.0);
            document.add_u64(secondary_time_field, time.1 as u64);
        }

        if let (Some(layer_id), Some(field_layer_id)) = (layer_id, self.layer_field) {
            document.add_u64(field_layer_id, layer_id as u64);
        }

        Self::add_property_value_to_doc(&mut document, field_property, prop_value);

        Ok(document)
    }

    pub(crate) fn create_node_const_property_document(
        &self,
        node_id: u64,
        prop_value: &Prop,
    ) -> tantivy::Result<TantivyDocument> {
        let field_node_id = self.entity_id_field;
        self.create_property_document(field_node_id, node_id, None, None, prop_value)
    }

    pub(crate) fn create_node_temporal_property_document(
        &self,
        time: TimeIndexEntry,
        node_id: u64,
        prop_value: &Prop,
    ) -> tantivy::Result<TantivyDocument> {
        let field_node_id = self.entity_id_field;
        self.create_property_document(field_node_id, node_id, Some(time), None, prop_value)
    }

    pub(crate) fn create_edge_const_property_document(
        &self,
        edge_id: u64,
        layer_id: usize,
        prop_value: &Prop,
    ) -> tantivy::Result<TantivyDocument> {
        let field_edge_id = self.entity_id_field;
        self.create_property_document(field_edge_id, edge_id, None, Some(layer_id), prop_value)
    }

    pub(crate) fn create_edge_temporal_property_document(
        &self,
        time: TimeIndexEntry,
        edge_id: u64,
        layer_id: usize,
        prop_value: &Prop,
    ) -> tantivy::Result<TantivyDocument> {
        let field_edge_id = self.entity_id_field;
        self.create_property_document(
            field_edge_id,
            edge_id,
            Some(time),
            Some(layer_id),
            prop_value,
        )
    }

    pub(crate) fn get_reader(&self) -> Result<IndexReader, GraphError> {
        let reader = self
            .index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()?;
        Ok(reader)
    }
}
