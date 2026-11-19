use itertools::Itertools;
use raphtory_api::core::{
    entities::{properties::prop::Prop, LayerId},
    storage::{dict_mapper::DictMapper, timeindex::EventTime},
};
use std::sync::Arc;

pub mod assertions;
pub mod edge;
pub(crate) mod edge_reads;
pub mod edges;
pub mod graph;
pub mod node;
pub mod nodes;
pub mod path;
pub mod views;

/// Groups the latest values of an entity's temporal properties, merging rows that occur at the
/// same time and layer: one `(time, layer, (prop id, value) pairs)` row per update, in time order,
/// with just the properties whose latest value it set.
pub(crate) fn merge_latest_prop_rows(
    mut latest: Vec<((EventTime, LayerId), (usize, Prop))>,
) -> impl Iterator<Item = (EventTime, LayerId, Vec<(usize, Prop)>)> + Send + Sync {
    // we need to sort because we're going by property, and each can have their latest update at any time
    latest.sort_unstable_by_key(|((t, layer_id), (prop_id, _))| (*t, *layer_id, *prop_id));
    let mut latest = latest.into_iter().peekable();
    // if multiple properties' latest values are set in the same update, merge them together
    std::iter::from_fn(move || {
        let (row, first) = latest.next()?;
        let props = std::iter::once(first)
            .chain(
                latest
                    .peeking_take_while(|(next, _)| *next == row)
                    .map(|(_, prop)| prop),
            )
            .collect();
        Some((row.0, row.1, props))
    })
}

pub(crate) fn create_node_type_filter<I: IntoIterator<Item = V>, V: AsRef<str>>(
    dict_mapper: &DictMapper,
    node_types: I,
) -> Arc<[bool]> {
    let mut bool_arr = vec![false; dict_mapper.num_all_fields()];

    for nt in node_types {
        let nt = nt.as_ref();
        if nt.is_empty() {
            bool_arr[0] = true; // FIXME: "" treated as default?
        } else if let Some(id) = dict_mapper.get_id(nt) {
            bool_arr[id] = true;
        }
    }
    bool_arr.into()
}
