use crate::{
    db::api::state::ops::GraphView,
    errors::GraphError,
    parquet_encoder::{run_encode, RecordBatchSink, SECONDARY_INDEX_COL, TIME_COL},
    prelude::{GraphViewOps, Prop, PropertiesOps},
};
use arrow::datatypes::{DataType, Field, SchemaRef};
use itertools::Itertools;
use raphtory_api::core::{entities::properties::prop::SerdeArrowProp, storage::arc_str::ArcStr};
use raphtory_core::storage::timeindex::EventTime;
use serde::{ser::SerializeMap, Serialize};
use std::collections::HashMap;

pub fn encode_graph_tprop<G: GraphView, S: RecordBatchSink>(
    g: &G,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError> {
    run_encode(
        g,
        g.graph_props_meta().temporal_prop_mapper(),
        1,
        sink_factory_fn,
        |_| {
            vec![
                Field::new(TIME_COL, DataType::Int64, false),
                Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
            ]
        },
        |_, g, decoder, sink| {
            // Collect into owned props here to avoid lifetime issues on prop_view.
            // Ideally we want to be returning refs to the props but this
            // is not possible with the current API.
            let collect_props = g.properties().temporal().iter().collect::<Vec<_>>();

            // Each prop key can have multiple values over time.
            // Flatten into (time, key, value) tuples to group by time.
            let merged_props = collect_props
                .iter()
                .map(|(prop_key, prop_view)| {
                    // Collect all the props for a given prop key
                    prop_view
                        .iter_indexed()
                        .map(move |(time, prop_value)| (time, prop_key.clone(), prop_value))
                })
                .kmerge_by(|(left_t, _, _), (right_t, _, _)| left_t <= right_t);

            // Group property (key, value) tuples by time to create rows.
            let rows: Vec<Row> = merged_props
                .chunk_by(|(t, _, _)| *t)
                .into_iter()
                .map(|(timestamp, group)| {
                    let row = group
                        .map(|(_, prop_key, prop_value)| (prop_key, prop_value))
                        .collect();

                    Row { t: timestamp, row }
                })
                .collect();

            decoder.serialize(&rows)?;

            if let Some(rb) = decoder.flush()? {
                RecordBatchSink::send_batch(sink, rb)?;
            }

            Ok(())
        },
    )
}

#[derive(Debug)]
struct Row {
    t: EventTime,
    row: HashMap<ArcStr, Prop>,
}

impl Serialize for Row {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_map(Some(self.row.len()))?;

        for (k, v) in self.row.iter() {
            state.serialize_entry(k, &SerdeArrowProp(v))?;
        }

        state.serialize_entry(TIME_COL, &self.t.0)?;
        state.serialize_entry(SECONDARY_INDEX_COL, &self.t.1)?;

        state.end()
    }
}

pub fn encode_graph_cprop<G: GraphView, S: RecordBatchSink>(
    g: &G,
    sink_factory_fn: impl Fn(SchemaRef, usize, usize) -> Result<S, GraphError> + Sync,
) -> Result<(), GraphError> {
    run_encode(
        g,
        g.graph_props_meta().metadata_mapper(),
        1,
        sink_factory_fn,
        |_| vec![Field::new(TIME_COL, DataType::Int64, true)],
        |_, g, decoder, sink| {
            let row = g.metadata().as_map();
            let time = EventTime::new(0, 0); // const props don't have time
            let rows = vec![Row { t: time, row }];

            decoder.serialize(&rows)?;

            if let Some(rb) = decoder.flush()? {
                RecordBatchSink::send_batch(sink, rb)?;
            }

            Ok(())
        },
    )
}
