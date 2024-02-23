use crate::core::{entities::properties::props::PropMapper, PropType};
use arrow2::datatypes::{DataType, Field, Schema};

fn schema_from_prop_meta(prop_map: PropMapper) -> Schema {
    let src_field = Field::new("srcs", DataType::UInt64, false);
    let dst_field = Field::new("dsts", DataType::UInt64, false);
    let time_field = Field::new("time", DataType::Int64, false);
    let mut schema = vec![src_field, dst_field, time_field];

    for (id, key) in prop_map.get_keys().iter().enumerate() {
        match prop_map.get_dtype(id).unwrap() {
            PropType::Empty => panic!(),
            PropType::Str => {
                schema.push(Field::new(key, DataType::LargeUtf8, true));
            }
            PropType::U8 => {
                schema.push(Field::new(key, DataType::UInt8, true));
            }
            PropType::U16 => {
                schema.push(Field::new(key, DataType::UInt16, true));
            }
            PropType::I32 => {
                schema.push(Field::new(key, DataType::Int32, true));
            }
            PropType::I64 => {
                schema.push(Field::new(key, DataType::Int64, true));
            }
            PropType::U32 => {
                schema.push(Field::new(key, DataType::UInt32, true));
            }
            PropType::U64 => {
                schema.push(Field::new(key, DataType::UInt64, true));
            }
            PropType::F32 => {
                schema.push(Field::new(key, DataType::Float32, true));
            }
            PropType::F64 => {
                schema.push(Field::new(key, DataType::Float64, true));
            }
            PropType::Bool => {
                schema.push(Field::new(key, DataType::Boolean, true));
            }
            PropType::List => {
                panic!("List not supported as property")
            }
            PropType::Map => {
                panic!("Map not supported as property")
            }
            PropType::DTime => {
                panic!("datetime not supported as property")
            }
            PropType::Graph => {
                panic!("Graph not supported as property")
            }
            PropType::Document => {
                panic!("Document not supported as property")
            }
        }
    }

    let schema = Schema::from(schema);
    schema
}
