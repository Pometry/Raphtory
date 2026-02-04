use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use crate::{db::api::view::StaticGraphViewOps, errors::GraphError};

pub(crate) fn to_prop<G: StaticGraphViewOps>(g: &G, weight: Option<&str>, val: f64) -> Result<Prop, GraphError> {
    let mut weight_type = PropType::U8;
    if let Some(weight) = weight {
        if let Some((_, dtype)) = g.edge_meta().get_prop_id_and_type(weight, false) {
            weight_type = dtype;
        } else {
            return Err(GraphError::PropertyMissingError(weight.to_string()));
        }
    }
    let prop_val = match weight_type {
        PropType::F32 => Prop::F32(val as f32),
        PropType::F64 => Prop::F64(val as f64),
        PropType::U8 => Prop::U8(val as u8),
        PropType::U16 => Prop::U16(val as u16),
        PropType::U32 => Prop::U32(val as u32),
        PropType::U64 => Prop::U64(val as u64),
        PropType::I32 => Prop::I32(val as i32),
        PropType::I64 => Prop::I64(val as i64),
        p_type => {
            return Err(GraphError::InvalidProperty {
                reason: format!("Weight type: {:?}, not supported", p_type),
            })
        }
    };
    Ok(prop_val)
}

pub mod bellman_ford;
pub mod dijkstra;
pub mod johnson;
pub mod single_source_shortest_path;
pub mod temporal_reachability;

