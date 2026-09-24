use crate::model::schema::property_schema::PropertySchema;
use raphtory::prelude::PropertiesOps;
use raphtory_api::core::entities::properties::meta::PropMapper;

pub(crate) mod cache;
pub(crate) mod graph_schema;
pub(crate) mod layer_schema;
pub(crate) mod node_schema;
pub(crate) mod property_schema;

/// Maximum number of distinct values collected per edge property key. More than that and we don't report any.
const MAX_EDGE_VARIANTS: usize = 20;

/// Above this many entities (nodes graph-wide for `NodeSchema`, edges in the
/// layer for `LayerSchema`), schema resolvers skip collecting property values
/// (variants) and only return keys and types
const MAX_DETAILED_SCHEMA_ENTITIES: usize = 1000;

/// Maximum number of distinct values collected per node property key.
const MAX_NODE_VARIANTS: usize = 100;

const DEFAULT_NODE_TYPE: &'static str = "None";

/// Collect the distinct values seen against each property id. Once a property
/// exceeds `max_variants` values we drop its set and report no variants for it.
///
/// `keep` decides which prop ids may be reported at all, letting callers apply
/// visibility/presence rules without the accumulator needing to know them.
pub(crate) fn collect_variants<P: PropertiesOps>(
    props_per_entity: impl Iterator<Item = P>,
    mapper: &PropMapper,
    max_variants: usize,
    keep: impl Fn(usize) -> bool,
) -> Vec<PropertySchema> {
    // Vec is indexed by prop id; variants[0] returns the HashSet of variants for prop id 0
    // `None` = never seen, `Some(empty)` = seen but past `max_variants`
    let mut variants: Vec<Option<ahash::HashSet<String>>> = Vec::new();
    for props in props_per_entity {
        for id in props.ids() {
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

    // one read lock for every name and dtype, and prop id order for the output
    let locked = mapper.locked();
    locked
        .iter_ids_and_types()
        .filter(|(id, _, _)| keep(*id))
        .filter_map(|(id, name, dtype)| {
            let seen = variants.get_mut(id).and_then(Option::take)?;
            let mut seen = Vec::from_iter(seen);
            seen.sort_unstable();
            Some(PropertySchema::new(name.to_string(), dtype.clone(), seen))
        })
        .collect()
}
