use crate::db::api::properties::internal::{InheritPropertiesOps, PropertiesOps};
use std::sync::Arc;

mod constant_props;
mod props;
mod temporal_props;

pub type DynProps = Arc<dyn PropertiesOps + Send + Sync>;
impl InheritPropertiesOps for DynProps {}

pub use constant_props::*;
pub use props::*;
pub use temporal_props::*;
