use crate::{
    db::api::view::{internal::GraphType, StaticGraphViewOps},
    prelude::TimeOps,
    search::fields,
};
use itertools::Itertools;
use raphtory_api::core::entities::VID;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::{HashMap, HashSet};
use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    Score, SegmentReader,
};

pub struct WindowFilterCollector<G> {
    prop_id: usize,
    field: String,
    graph: G,
}

impl<G> WindowFilterCollector<G>
where
    G: StaticGraphViewOps,
{
    pub fn new(field: String, prop_id: usize, graph: G) -> Self {
        Self {
            field,
            prop_id,
            graph,
        }
    }
}

impl<G> Collector for WindowFilterCollector<G>
where
    G: StaticGraphViewOps,
{
    type Fruit = HashSet<u64>;
    type Child = WindowFilterSegmentCollector<G>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let column_opt_time = segment_reader.fast_fields().column_opt(fields::TIME)?;
        let column_opt_entity_id = segment_reader.fast_fields().column_opt(&self.field)?;

        Ok(WindowFilterSegmentCollector {
            column_opt_time,
            column_opt_entity_id,
            segment_ord: segment_local_id,
            unique_entity_ids: HashMap::new(),
            graph: self.graph.clone(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<HashMap<u64, Option<i64>>>,
    ) -> tantivy::Result<HashSet<u64>> {
        let mut global_unique_entity_ids: HashMap<u64, Option<i64>> = HashMap::new();

        for (entity_id, time) in segment_fruits.into_iter().flatten() {
            global_unique_entity_ids
                .entry(entity_id)
                .and_modify(|existing_time| {
                    // If "time" against entity_id in global list is None,
                    // ignore it because we only care of uniqueness for entries within window
                    if let (Some(existing_time), Some(new_time)) = (existing_time, time) {
                        if new_time > *existing_time {
                            *existing_time = new_time;
                        }
                    }
                })
                // If entity_id from any segment is not already present in the global list, add it
                .or_insert(time);
        }

        let unique_entity_ids: HashSet<u64> = global_unique_entity_ids.keys().cloned().collect();

        let result = match (self.graph.start(), self.graph.end()) {
            (Some(start), Some(end))
                if matches!(self.graph.graph_type(), GraphType::PersistentGraph) =>
            {
                unique_entity_ids
                    .into_par_iter()
                    // Skip entity_ids which don't qualify last_before check for given timestamp and prop_id
                    .filter_map(|id| {
                        let vid = VID(id as usize);
                        let t = global_unique_entity_ids.get(&id).copied()?;

                        let available = t
                            .map(|t| {
                                self.graph.is_prop_update_available_window(
                                    self.prop_id,
                                    vid,
                                    t.into(),
                                    start..end,
                                )
                            })
                            // "t" is none for entity_ids that are already within window.
                            // Therefore, they must always be included.
                            .unwrap_or(true);

                        available.then_some((id, t))
                    })
                    .collect()
            }
            // If the graph is non-windowed or an event graph,
            // all entries in the global list are valid
            _ => global_unique_entity_ids,
        };

        Ok(result.keys().cloned().collect())
    }
}

pub struct WindowFilterSegmentCollector<G> {
    column_opt_time: Option<Column<i64>>,
    column_opt_entity_id: Option<Column<u64>>,
    segment_ord: u32,
    unique_entity_ids: HashMap<u64, Option<i64>>,
    graph: G,
}

impl<G> SegmentCollector for WindowFilterSegmentCollector<G>
where
    G: StaticGraphViewOps,
{
    type Fruit = HashMap<u64, Option<i64>>;

    fn collect(&mut self, doc_id: u32, _score: Score) {
        let opt_time = self
            .column_opt_time
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());
        let opt_entity_id = self
            .column_opt_entity_id
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());

        if let (Some(time), Some(entity_id)) = (opt_time, opt_entity_id) {
            match (self.graph.start(), self.graph.end()) {
                (Some(start), Some(end)) => {
                    match self.graph.graph_type() {
                        GraphType::EventGraph => {
                            if time >= start && time < end {
                                self.unique_entity_ids.entry(entity_id).or_default();
                            }
                        }
                        GraphType::PersistentGraph => {
                            if time >= start && time < end {
                                // If doc with "time" within window is seen later than doc with "time" before start
                                // it must take precedence
                                self.unique_entity_ids.insert(entity_id, None);
                            } else if time < start {
                                self.unique_entity_ids
                                    .entry(entity_id)
                                    .and_modify(|last_time| {
                                        if let Some(last) = last_time {
                                            if time > *last {
                                                *last = time;
                                            }
                                        }
                                    })
                                    .or_insert(Some(time));
                            }
                        }
                    }
                }
                _ => {
                    // Non-windowed graph docs are collected without any conditions
                    self.unique_entity_ids.entry(entity_id).or_default();
                }
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.unique_entity_ids
    }
}
