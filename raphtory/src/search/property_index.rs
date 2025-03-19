use crate::{
    core::{storage::timeindex::AsTime, utils::errors::GraphError},
    prelude::*,
    search::{fields, new_index, TOKENIZER},
};
use raphtory_api::core::{
    storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
    PropType,
};
use std::{fmt::Debug, sync::Arc};
use tantivy::{
    collector::TopDocs,
    query::AllQuery,
    schema::{
        Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, Type,
        FAST, INDEXED, TEXT,
    },
    Document, Index, IndexReader, TantivyDocument,
};

#[derive(Clone)]
pub struct PropertyIndex {
    pub(crate) prop_name: ArcStr,
    pub(crate) index: Arc<Index>,
    pub(crate) reader: IndexReader,
    pub(crate) time_field: Option<Field>,
    pub(crate) secondary_time_field: Option<Field>,
    pub(crate) layer_field: Option<Field>,
    pub(crate) entity_id_field: Field,
}

impl PropertyIndex {
    fn new_property(prop_name: ArcStr, schema: Schema, is_edge: bool) -> Self {
        let time_field = schema.get_field(fields::TIME).ok();
        let secondary_time_field = schema.get_field(fields::SECONDARY_TIME).ok();
        let entity_id_field = schema
            .get_field(if is_edge {
                fields::EDGE_ID
            } else {
                fields::NODE_ID
            })
            .ok()
            .expect(if is_edge {
                "Need edge id"
            } else {
                "Need node id"
            });

        let layer_field = if is_edge {
            schema.get_field(fields::LAYER_ID).ok()
        } else {
            None
        };

        let (index, reader) = new_index(schema);

        Self {
            prop_name,
            index: Arc::new(index),
            reader,
            time_field,
            secondary_time_field,
            layer_field,
            entity_id_field,
        }
    }

    pub(crate) fn new_node_property(prop_name: ArcStr, schema: Schema) -> Self {
        Self::new_property(prop_name, schema, false)
    }

    pub(crate) fn new_edge_property(prop_name: ArcStr, schema: Schema) -> Self {
        Self::new_property(prop_name, schema, true)
    }

    pub(crate) fn print(&self) -> Result<(), GraphError> {
        let searcher = self.reader.searcher();
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
                schema_builder.add_text_field(
                    prop_name,
                    TextOptions::default().set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer(TOKENIZER)
                            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                    ),
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

    pub fn get_prop_field_type(&self, prop_name: &str) -> tantivy::Result<Type> {
        Ok(self
            .index
            .schema()
            .get_field_entry(self.index.schema().get_field(prop_name)?)
            .field_type()
            .value_type())
    }

    fn add_property_value(document: &mut TantivyDocument, field: Field, prop_value: &Prop) {
        match prop_value.clone() {
            Prop::Str(v) => document.add_text(field, v),
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

        Self::add_property_value(&mut document, field_property, prop_value);

        // println!("Added prop doc: {}", &document.to_json(&schema));

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
}
