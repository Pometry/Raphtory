use raphtory_api::core::storage::dict_mapper::DictMapper;
use std::sync::Arc;

pub mod assertions;
pub mod edge;
pub mod edges;
pub mod graph;
pub mod node;
pub mod nodes;
pub mod path;
pub mod views;

pub(crate) fn create_node_type_filter<I: IntoIterator<Item = V>, V: AsRef<str>>(
    dict_mapper: &DictMapper,
    node_types: I,
) -> Arc<[bool]> {
    let len = dict_mapper.len();
    let mut bool_arr = vec![false; len];

    for nt in node_types {
        let nt = nt.as_ref();
        if nt.is_empty() {
            bool_arr[0] = true;
        } else if let Some(id) = dict_mapper.get_id(nt) {
            bool_arr[id] = true;
        }
    }
    bool_arr.into()
}
