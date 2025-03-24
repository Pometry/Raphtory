use std::collections::HashSet;
use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    Score, SegmentReader,
};

pub struct UniqueEntityFilterCollector {
    field: String,
}

impl UniqueEntityFilterCollector {
    pub fn new(field: String) -> Self {
        Self { field }
    }
}

impl Collector for UniqueEntityFilterCollector {
    type Fruit = HashSet<u64>;
    type Child = UniqueFilterSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let column_opt_entity_id = segment_reader.fast_fields().column_opt(&self.field)?;

        Ok(UniqueFilterSegmentCollector {
            column_opt_entity_id,
            seen_entities: HashSet::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<HashSet<u64>>) -> tantivy::Result<HashSet<u64>> {
        let mut global_unique_entity_ids: HashSet<u64> = HashSet::new();

        for entity_id in segment_fruits.into_iter().flatten() {
            global_unique_entity_ids.insert(entity_id);
        }

        Ok(global_unique_entity_ids)
    }
}

pub struct UniqueFilterSegmentCollector {
    column_opt_entity_id: Option<Column<u64>>,
    seen_entities: HashSet<u64>,
}

impl SegmentCollector for UniqueFilterSegmentCollector {
    type Fruit = HashSet<u64>;

    fn collect(&mut self, doc_id: u32, _score: Score) {
        let opt_entity_id = self
            .column_opt_entity_id
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());

        if let Some(entity_id) = opt_entity_id {
            self.seen_entities.insert(entity_id);
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.seen_entities
    }
}
