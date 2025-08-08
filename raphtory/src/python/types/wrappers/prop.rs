use crate::python::types::repr::Repr;
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::TimeIndexEntry};
impl Repr for Prop {
    fn repr(&self) -> String {
        match &self {
            Prop::Str(v) => v.repr(),
            Prop::Bool(v) => v.repr(),
            Prop::I64(v) => v.repr(),
            Prop::U8(v) => v.repr(),
            Prop::U16(v) => v.repr(),
            Prop::U64(v) => v.repr(),
            Prop::F64(v) => v.repr(),
            Prop::DTime(v) => v.repr(),
            Prop::NDTime(v) => v.repr(),
            Prop::Array(v) => format!("{:?}", v),
            Prop::I32(v) => v.repr(),
            Prop::U32(v) => v.repr(),
            Prop::F32(v) => v.repr(),
            Prop::List(v) => v.repr(),
            Prop::Map(v) => v.repr(),
            Prop::Decimal(v) => v.repr(),
        }
    }
}

pub type PropValue = Option<Prop>;
pub type PropHistItems = Vec<(TimeIndexEntry, Prop)>;
