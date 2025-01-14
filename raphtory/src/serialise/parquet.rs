use std::{fs::File, path::Path, sync::Arc};

use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use itertools::Itertools;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use raphtory_api::core::entities::{properties::props::PropMapper, GidType};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Serialize,
};

use crate::{
    core::{arrow_dtype_from_prop_type, utils::errors::GraphError},
    db::{
        api::{mutation::internal::InternalAdditionOps, view::StaticGraphViewOps},
        graph::edge::EdgeView,
    },
    io::parquet_loaders::load_edges_from_parquet,
    prelude::*,
};

pub trait ParquetEncoder {
    fn encode_parquet(&self, path: impl AsRef<Path>) -> Result<(), GraphError>;
}

pub trait ParquetDecoder {
    fn decode_parquet(&self, path: impl AsRef<Path>) -> Result<Self, GraphError>
    where
        Self: Sized;
}

const TIME_COL: &str = "rap_time";
const SRC_COL: &str = "rap_src";
const DST_COL: &str = "rap_dst";
const LAYER_COL: &str = "rap_layer";

const T_EDGE_FILE: &str = "t_edges.parquet";

#[derive(Debug)]
struct ParquetEdge<G: StaticGraphViewOps>(EdgeView<G>);

struct ParquetProp<'a>(&'a Prop);

impl<'a> Serialize for ParquetProp<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            Prop::I32(i) => serializer.serialize_i32(*i),
            Prop::I64(i) => serializer.serialize_i64(*i),
            Prop::F32(f) => serializer.serialize_f32(*f),
            Prop::F64(f) => serializer.serialize_f64(*f),
            Prop::U8(u) => serializer.serialize_u8(*u),
            Prop::U16(u) => serializer.serialize_u16(*u),
            Prop::U32(u) => serializer.serialize_u32(*u),
            Prop::U64(u) => serializer.serialize_u64(*u),
            Prop::Str(s) => serializer.serialize_str(s),
            Prop::Bool(b) => serializer.serialize_bool(*b),
            Prop::List(l) => {
                let mut state = serializer.serialize_seq(Some(l.len()))?;
                for prop in l.iter() {
                    state.serialize_element(&ParquetProp(prop))?;
                }
                state.end()
            }
            Prop::Map(m) => {
                let mut state = serializer.serialize_map(Some(m.len()))?;
                for (k, v) in m.iter() {
                    state.serialize_entry(k, &ParquetProp(v))?;
                }
                state.end()
            }
            _ => todo!(),
        }
    }
}

#[derive(Debug)]
struct ParquetGID(GID);

impl Serialize for ParquetGID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.0 {
            GID::U64(id) => serializer.serialize_u64(*id),
            GID::Str(id) => serializer.serialize_str(id),
        }
    }
}

impl<G: StaticGraphViewOps> Serialize for ParquetEdge<G> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let edge = &self.0;
        let mut state = serializer.serialize_map(None)?;
        let t = edge.edge.time().expect("Exploded edges need to have time");
        let layer = edge
            .layer_name()
            .expect("Exploded edges need to have layer");

        state.serialize_entry(TIME_COL, &t.0)?;
        state.serialize_entry(SRC_COL, &ParquetGID(edge.src().id()))?;
        state.serialize_entry(DST_COL, &ParquetGID(edge.dst().id()))?;
        state.serialize_entry(LAYER_COL, &layer)?;

        for (name, prop) in edge.properties().temporal().iter_latest() {
            state.serialize_entry(&name, &ParquetProp(&prop))?;
        }

        state.end()
    }
}

impl<G: StaticGraphViewOps + InternalAdditionOps + std::fmt::Debug> ParquetEncoder for G {
    fn encode_parquet(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let fields = arrow_fields(self.edge_meta().temporal_prop_meta())?;

        let id_type = match self.id_type() {
            Some(GidType::Str) => DataType::Utf8,
            Some(GidType::U64) => DataType::UInt64,
            None => todo!(), // The graph is empty what now?
        };

        let default_fields = vec![
            Field::new(TIME_COL, DataType::Int64, false),
            Field::new(SRC_COL, id_type.clone(), false),
            Field::new(DST_COL, id_type, false),
            Field::new(LAYER_COL, DataType::Utf8, false),
        ];

        let schema: Arc<Schema> = Schema::new(
            default_fields
                .into_iter()
                .chain(fields.into_iter())
                .collect::<Vec<_>>(),
        )
        .into();

        println!("{:?}", schema);

        let edge_file = File::create(path.as_ref().join(T_EDGE_FILE))?;

        let mut writer = ArrowWriter::try_new(edge_file, schema.clone(), Some(props))?;

        let mut decoder = ReaderBuilder::new(schema).build_decoder()?;

        for edge_rows in self
            .edges()
            .explode()
            .into_iter()
            .chunks(100_000)
            .into_iter()
            .map(|chunk| {
                chunk
                    .map(|edge| ParquetEdge(edge))
                    .inspect(|e| println!("{:?}", serde_json::to_value(e)))
                    .collect_vec()
            })
        {
            decoder.serialize(&edge_rows)?;
            if let Some(rb) = decoder.flush()? {
                writer.write(&rb)?;
                writer.flush()?;
            }
        }

        writer.close()?;

        Ok(())
    }
}

fn arrow_fields(meta: &PropMapper) -> Result<Vec<Field>, GraphError> {
    meta.get_keys()
        .into_iter()
        .filter_map(|name| {
            let prop_id = meta.get_id(&name)?;
            meta.get_dtype(prop_id)
                .map(move |prop_type| (name, prop_type))
        })
        .inspect(|pair| println!("{:?}", pair))
        .map(|(name, prop_type)| {
            arrow_dtype_from_prop_type(&prop_type).map(|d_type| Field::new(name, d_type, true))
        })
        .collect()
}

impl ParquetDecoder for Graph {
    fn decode_parquet(&self, path: impl AsRef<Path>) -> Result<Self, GraphError>
    where
        Self: Sized,
    {
        let g = Graph::new();
        load_edges_from_parquet(
            &g,
            path.as_ref(),
            TIME_COL,
            SRC_COL,
            DST_COL,
            Some(&["str_prop", "int_prop"]),
            None,
            None,
            None,
            Some(LAYER_COL),
        )?;
        Ok(g)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{db::graph::graph::assert_graph_equal, test_utils::build_graph_from_edge_list};

    #[test]
    fn write_edges_to_parquet() {
        let temp_dir = tempfile::tempdir().unwrap();
        let g = build_graph_from_edge_list(&[
            (0u64, 1, 12, "one".to_string(), 123i64),
            (1, 2, 13, "two".to_string(), 124),
        ]);
        g.encode_parquet(&temp_dir).unwrap();
        let g2 = g.decode_parquet(&temp_dir).unwrap();
        assert_graph_equal(&g, &g2);
    }
}
