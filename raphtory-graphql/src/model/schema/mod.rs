use crate::model::schema::property_schema::PropertySchema;
use raphtory::prelude::PropertiesOps;
use raphtory_api::core::entities::{properties::meta::PropMapper, LayerId};

pub(crate) mod graph_schema;
pub(crate) mod layer_schema;
pub(crate) mod node_schema;
pub(crate) mod property_schema;

/// Maximum number of distinct values collected per property key. More than that and we don't report any.
const MAX_VARIANTS: usize = 20;

/// Above this many entities (nodes graph-wide for `NodeSchema`, edges in the
/// layer for `LayerSchema`), schema resolvers skip collecting property values
/// (variants) and only return keys and types
const MAX_DETAILED_SCHEMA_ENTITIES: usize = 1000;

const DEFAULT_NODE_TYPE: &str = "None";

pub(crate) fn visible_ids_any_layer(
    ids: impl Iterator<Item = usize>,
    mapper: &PropMapper,
) -> Vec<usize> {
    let layer_presence = mapper.read_lock_layer_presence();
    ids.filter(|id| layer_presence.any_layer_has(*id)).collect()
}

pub(crate) fn visible_ids_layer(
    ids: impl Iterator<Item = usize>,
    layer: LayerId,
    mapper: &PropMapper,
) -> Vec<usize> {
    let layer_presence = mapper.read_lock_layer_presence();
    ids.filter(|id| layer_presence.has(layer, *id)).collect()
}

/// Collect the distinct values seen against each property id. Once a property
/// exceeds `max_variants` values we drop its set and report no variants for it.
pub(crate) fn collect_variants<P: PropertiesOps>(
    props_per_entity: impl Iterator<Item = P>,
    mapper: &PropMapper,
    max_variants: usize,
    visible_properties: &[usize],
) -> Vec<PropertySchema> {
    // Vec is indexed by prop id; variants[0] returns the HashSet of variants for prop id 0
    // `None` = never seen, `Some(empty)` = seen but past `max_variants`

    let cap = visible_properties.last().copied().map_or(0, |id| id + 1);
    let mut variants: Vec<Option<ahash::HashSet<String>>> = Vec::with_capacity(cap);
    for props in props_per_entity {
        for &id in visible_properties {
            let Some(value) = props.get_by_id(id) else {
                continue;
            };
            if variants.len() <= id {
                variants.resize_with(id + 1, || None);
            }
            match &mut variants[id] {
                slot @ None => *slot = Some(ahash::HashSet::from_iter([value.to_string()])),
                Some(seen) if !seen.is_empty() => {
                    seen.insert(value.to_string());
                    if seen.len() > max_variants {
                        seen.clear();
                    }
                }
                // already past the boundary, nothing left to record
                Some(_) => {}
            }
        }
    }

    // one read lock
    let locked = mapper.locked();
    visible_properties
        .iter()
        .filter_map(|&id| {
            let (name, dtype) = locked.get_name_and_dtype(id)?;
            let seen = variants.get_mut(id)?.take()?;
            let mut seen = Vec::from_iter(seen);
            seen.sort_unstable();
            Some(PropertySchema::new(name.to_string(), dtype.clone(), seen))
        })
        .collect()
}

/// Keys and types only, no value scan - for graphs too big to sample.
fn collect_no_variants(mapper: &PropMapper, visible_properties: &[usize]) -> Vec<PropertySchema> {
    let mapper = mapper.locked();
    visible_properties
        .iter()
        .filter_map(|id| mapper.get_name_and_dtype(*id))
        .map(|(name, dtype)| PropertySchema::new(name.to_string(), dtype.clone(), vec![]))
        .collect()
}
