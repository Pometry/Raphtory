use crate::{
    core::{storage::timeindex::AsTime, utils::errors::GraphError},
    prelude::*,
    search::{fields, new_index},
};
use raphtory_api::core::{storage::arc_str::ArcStr, PropType};
use std::{fmt::Debug, sync::Arc};
use tantivy::{
    collector::TopDocs,
    query::AllQuery,
    schema::{Field, Schema, SchemaBuilder, Type, FAST, INDEXED, STORED, TEXT},
    Document, Index, IndexReader, IndexSettings, TantivyDocument,
};
use tantivy::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};
use crate::search::TOKENIZER;

#[derive(Clone)]
pub struct PropertyIndex {
    pub(crate) prop_name: ArcStr,
    pub(crate) index: Arc<Index>,
    pub(crate) reader: IndexReader,
}

impl PropertyIndex {
    pub(crate) fn new(prop_name: ArcStr, prop_type: PropType) -> Self {
        let schema = Self::schema_builder(&*prop_name, prop_type).build();
        let (index, reader) = new_index(schema, IndexSettings::default());
        Self {
            prop_name,
            index: Arc::new(index),
            reader,
        }
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

    fn schema_builder(prop_name: &str, prop_type: PropType) -> SchemaBuilder {
        let mut schema = Schema::builder();
        schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
        schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
        schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);

        match prop_type {
            PropType::Str => {
                schema.add_text_field(
                    prop_name,
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
                schema.add_date_field(prop_name, INDEXED | FAST | STORED);
            }
            PropType::U8 => {
                schema.add_u64_field(prop_name, INDEXED | FAST | STORED);
            }
            PropType::U16 => {
                schema.add_u64_field(prop_name, INDEXED | FAST | STORED);
            }
            PropType::U64 => {
                schema.add_u64_field(prop_name, INDEXED | FAST | STORED);
            }
            PropType::I64 => {
                schema.add_i64_field(prop_name, INDEXED | FAST | STORED);
            }
            PropType::I32 => {
                schema.add_i64_field(prop_name, INDEXED | FAST | STORED);
            }
            PropType::F64 => {
                schema.add_f64_field(prop_name, INDEXED | FAST | STORED);
            }
            PropType::F32 => {
                schema.add_f64_field(prop_name, INDEXED | FAST | STORED);
            }
            PropType::Bool => {
                schema.add_bool_field(prop_name, INDEXED | FAST | STORED);
            }
            _ => {
                schema.add_text_field(prop_name, TEXT | FAST | STORED);
            }
        }

        schema
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

    pub(crate) fn create_document<'a>(
        &self,
        time: i64,
        field_id: &str,
        id: u64,
        prop_name: String,
        prop_value: Prop,
    ) -> tantivy::Result<TantivyDocument> {
        let schema = self.index.schema();
        let field_time = schema.get_field(fields::TIME)?;
        let field_id = schema.get_field(field_id)?;
        let field_property = schema.get_field(&prop_name)?;

        let mut document = TantivyDocument::new();
        document.add_i64(field_time, time);
        document.add_u64(field_id, id);

        match prop_value {
            Prop::Str(v) => {
                document.add_text(field_property, v);
            }
            Prop::NDTime(v) => {
                let time = tantivy::DateTime::from_timestamp_nanos(
                    v.and_utc().timestamp_nanos_opt().unwrap(),
                );
                document.add_date(field_property, time);
            }
            Prop::U8(v) => {
                document.add_u64(field_property, u64::from(v));
            }
            Prop::U16(v) => {
                document.add_u64(field_property, u64::from(v));
            }
            Prop::U64(v) => {
                document.add_u64(field_property, v);
            }
            Prop::I64(v) => {
                document.add_i64(field_property, v);
            }
            Prop::I32(v) => {
                document.add_i64(field_property, i64::from(v));
            }
            Prop::F64(v) => {
                document.add_f64(field_property, v);
            }
            Prop::F32(v) => {
                document.add_f64(field_property, f64::from(v));
            }
            Prop::Bool(v) => {
                document.add_bool(field_property, v);
            }
            prop => document.add_text(field_property, prop.to_string()),
        }

        // println!("Added prop doc: {}", &document.to_json(&schema));

        Ok(document)
    }
}
