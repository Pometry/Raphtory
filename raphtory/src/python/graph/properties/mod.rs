use crate::db::api::properties::internal::{InheritPropertiesOps, PropertiesOps};
use std::sync::Arc;

mod props;
mod static_props;
mod temporal_props;

pub type DynProps = Arc<dyn PropertiesOps + Send + Sync>;
impl InheritPropertiesOps for DynProps {}

pub use props::*;
pub use static_props::*;
pub use temporal_props::*;
