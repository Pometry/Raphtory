use crate::core::PropType;

pub mod props;

#[derive(thiserror::Error, Debug)]
pub enum PropError {
    #[error("Wrong type for property {name}: expected {expected:?} but actual type is {actual:?}")]
    PropertyTypeError {
        name: String,
        expected: PropType,
        actual: PropType,
    },
}
