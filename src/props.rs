use crate::tvec::DefaultTVec;

#[derive(Debug, Default, PartialEq)]
pub enum TProp {
    #[default]
    Empty,
    Str(DefaultTVec<String>),
    U32(DefaultTVec<u32>),
    U64(DefaultTVec<u64>),
    F32(DefaultTVec<f32>),
    F64(DefaultTVec<f64>),
}
