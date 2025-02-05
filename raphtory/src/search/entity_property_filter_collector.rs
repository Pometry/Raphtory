use crate::{db::api::view::StaticGraphViewOps, search::fields};
use raphtory_api::core::entities::VID;
use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    DocId, Score, SegmentReader,
};

pub struct EntityPropertyFilterCollector<TCollector, G>
where
    G: StaticGraphViewOps,
{
    prop_id: usize,
    field: String,
    collector: TCollector,
    graph: G,
}

impl<TCollector, G> EntityPropertyFilterCollector<TCollector, G>
where
    TCollector: Collector + Send + Sync,
    G: StaticGraphViewOps,
{
    pub fn new(prop_id: usize, field: String, collector: TCollector, graph: G) -> Self {
        Self {
            prop_id,
            field,
            collector,
            graph,
        }
    }
}

impl<TCollector, G> Collector for EntityPropertyFilterCollector<TCollector, G>
where
    TCollector: Collector + Send + Sync,
    G: StaticGraphViewOps,
{
    type Fruit = TCollector::Fruit;
    type Child = EntityPropertyFilterSegmentCollector<TCollector::Child, G>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let column_opt_time = segment_reader.fast_fields().column_opt(fields::TIME)?;
        let column_opt_entity_id = segment_reader.fast_fields().column_opt(&self.field)?;

        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;

        Ok(EntityPropertyFilterSegmentCollector {
            prop_id: self.prop_id,
            column_opt_time,
            column_opt_entity_id,
            segment_collector,
            graph: self.graph.clone(),
        })
    }

    fn requires_scoring(&self) -> bool {
        self.collector.requires_scoring()
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<TCollector::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<TCollector::Fruit> {
        self.collector.merge_fruits(segment_fruits)
    }
}

pub struct EntityPropertyFilterSegmentCollector<TSegmentCollector, G> {
    prop_id: usize,
    column_opt_time: Option<Column<i64>>,
    column_opt_entity_id: Option<Column<u64>>,
    segment_collector: TSegmentCollector,
    graph: G,
}

impl<TSegmentCollector, G> EntityPropertyFilterSegmentCollector<TSegmentCollector, G>
where
    G: StaticGraphViewOps,
{
    #[inline]
    fn accept_document(&mut self, doc_id: DocId) -> bool {
        let opt_time = self
            .column_opt_time
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());

        let opt_node_id = self
            .column_opt_entity_id
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());

        match (opt_time, opt_node_id) {
            (Some(time), Some(node_id)) => {
                let node_id = VID(node_id as usize);
                self.graph
                    .is_prop_update_available(self.prop_id, node_id, time.into())
            }
            _ => false,
        }
    }
}

impl<TSegmentCollector, G> SegmentCollector
    for EntityPropertyFilterSegmentCollector<TSegmentCollector, G>
where
    TSegmentCollector: SegmentCollector,
    G: StaticGraphViewOps,
{
    type Fruit = TSegmentCollector::Fruit;

    fn collect(&mut self, doc: u32, score: Score) {
        println!("collecting docs = {}", doc);
        if self.accept_document(doc) {
            self.segment_collector.collect(doc, score);
        }
    }

    fn harvest(self) -> TSegmentCollector::Fruit {
        self.segment_collector.harvest()
    }
}
