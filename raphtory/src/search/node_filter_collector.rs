use crate::db::api::view::StaticGraphViewOps;
use raphtory_api::core::entities::VID;
use std::fmt::Debug;
use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    DocId, Score, SegmentReader,
};

pub struct NodeFilterCollector<TCollector, G>
where
    G: StaticGraphViewOps,
{
    field: String,
    collector: TCollector,
    graph: G,
}

impl<TCollector, G> NodeFilterCollector<TCollector, G>
where
    TCollector: Collector + Send + Sync,
    G: StaticGraphViewOps,
{
    pub fn new(field: String, collector: TCollector, graph: G) -> Self {
        Self {
            field,
            collector,
            graph,
        }
    }
}

impl<TCollector, G> Collector for NodeFilterCollector<TCollector, G>
where
    TCollector: Collector + Send + Sync,
    G: StaticGraphViewOps,
{
    type Fruit = TCollector::Fruit;

    type Child = NodeFilterSegmentCollector<TCollector::Child, G>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let column_opt = segment_reader.fast_fields().column_opt(&self.field)?;

        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;

        Ok(NodeFilterSegmentCollector {
            column_opt,
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

pub struct NodeFilterSegmentCollector<TSegmentCollector, G> {
    column_opt: Option<Column<u64>>,
    segment_collector: TSegmentCollector,
    graph: G,
}

impl<TSegmentCollector, G> NodeFilterSegmentCollector<TSegmentCollector, G>
where
    G: StaticGraphViewOps,
{
    #[inline]
    fn accept_document(&self, doc_id: DocId) -> bool {
        if let Some(column) = &self.column_opt {
            for val in column.values_for_doc(doc_id) {
                return self.graph.has_node(VID(val as usize));
            }
        }
        false
    }
}

impl<TSegmentCollector, G> SegmentCollector for NodeFilterSegmentCollector<TSegmentCollector, G>
where
    TSegmentCollector: SegmentCollector,
    G: StaticGraphViewOps,
{
    type Fruit = TSegmentCollector::Fruit;

    fn collect(&mut self, doc: u32, score: Score) {
        if self.accept_document(doc) {
            self.segment_collector.collect(doc, score);
        }
    }

    fn harvest(self) -> TSegmentCollector::Fruit {
        self.segment_collector.harvest()
    }
}
