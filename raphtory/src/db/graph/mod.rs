use crate::core::entities::properties::props::{DictMapper};
use std::sync::Arc;

pub mod edge;
pub mod edges;
pub mod graph;
pub mod node;
pub mod nodes;
pub mod path;
pub mod views;

fn create_node_type_filter(
    dict_mapper: &DictMapper,
    node_types: &[impl AsRef<str>],
) -> Arc<[bool]> {
    let len = dict_mapper.len();
    let mut bool_arr = vec![false; len];

    for nt in node_types {
        if let Some(id) = dict_mapper.get_id(nt.as_ref()) {
            bool_arr[id] = true;
        }
    }

    bool_arr.into()
}
