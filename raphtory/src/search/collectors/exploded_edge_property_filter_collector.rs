use crate::search::fields;
use raphtory_api::core::{entities::EID, storage::timeindex::EventTime};
use std::collections::HashSet;
use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    Score, SegmentReader,
};

pub struct ExplodedEdgePropertyFilterCollector {
    field: String,
}

impl ExplodedEdgePropertyFilterCollector {
    pub fn new(field: String) -> Self {
        Self { field }
    }
}

impl Collector for ExplodedEdgePropertyFilterCollector {
    type Fruit = HashSet<(EventTime, EID, usize)>;
    type Child = ExplodedEdgePropertyFilterSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let column_opt_time = segment_reader.fast_fields().column_opt(fields::TIME)?;
        let column_opt_entity_id = segment_reader.fast_fields().column_opt(&self.field)?;
        let column_opt_layer_id = segment_reader.fast_fields().column_opt(fields::LAYER_ID)?;
        let column_opt_event_id: Option<Column<u64>> =
            segment_reader.fast_fields().column_opt(fields::EVENT_ID)?;

        Ok(ExplodedEdgePropertyFilterSegmentCollector {
            column_opt_time,
            column_opt_entity_id,
            column_opt_layer_id,
            column_opt_event_id,
            unique_entity_ids: HashSet::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<HashSet<(EventTime, EID, usize)>>,
    ) -> tantivy::Result<HashSet<(EventTime, EID, usize)>> {
        let mut global_unique_entity_ids: HashSet<(EventTime, EID, usize)> = HashSet::new();

        for entity_id in segment_fruits.into_iter().flatten() {
            global_unique_entity_ids.insert(entity_id);
        }

        Ok(global_unique_entity_ids)
    }
}

pub struct ExplodedEdgePropertyFilterSegmentCollector {
    column_opt_time: Option<Column<i64>>,
    column_opt_entity_id: Option<Column<u64>>,
    column_opt_layer_id: Option<Column<u64>>,
    column_opt_event_id: Option<Column<u64>>,
    unique_entity_ids: HashSet<(EventTime, EID, usize)>,
}

impl SegmentCollector for ExplodedEdgePropertyFilterSegmentCollector {
    type Fruit = HashSet<(EventTime, EID, usize)>;

    fn collect(&mut self, doc_id: u32, _score: Score) {
        let opt_time = self
            .column_opt_time
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());
        let opt_entity_id = self
            .column_opt_entity_id
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());
        let opt_layer_id = self
            .column_opt_layer_id
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());
        let opt_event_id = self
            .column_opt_event_id
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());

        if let (Some(time), Some(entity_id), Some(layer_id), Some(event_id)) =
            (opt_time, opt_entity_id, opt_layer_id, opt_event_id)
        {
            let tie = EventTime::new(time, event_id as usize);
            let eid = EID(entity_id as usize);
            let layer_id = layer_id as usize;

            self.unique_entity_ids.insert((tie, eid, layer_id));
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.unique_entity_ids
    }
}
