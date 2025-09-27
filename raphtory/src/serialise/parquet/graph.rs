use crate::{
    errors::GraphError,
    prelude::{GraphViewOps, Prop, PropertiesOps},
    serialise::parquet::{
        model::ParquetProp, run_encode,
        EVENT_GRAPH_TYPE, GRAPH_C_PATH, GRAPH_TYPE, GRAPH_T_PATH, PERSISTENT_GRAPH_TYPE,
        SECONDARY_INDEX_COL, TIME_COL
    },
};
use arrow_schema::{DataType, Field};
use itertools::Itertools;
use parquet::format::KeyValue;
use raphtory_api::{core::storage::arc_str::ArcStr, GraphType};
use raphtory_core::storage::timeindex::TimeIndexEntry;
use raphtory_storage::graph::graph::GraphStorage;
use serde::{ser::SerializeMap, Serialize};
use std::{collections::HashMap, path::Path};

pub fn encode_graph_tprop(g: &GraphStorage, path: impl AsRef<Path>) -> Result<(), GraphError> {
    run_encode(
        g,
        g.graph_meta().temporal_mapper(),
        1,
        path,
        GRAPH_T_PATH,
        |_| {
            vec![
                Field::new(TIME_COL, DataType::Int64, false),
                Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
            ]
        },
        |_, g, decoder, writer| {
            // Each prop key can have multiple values over time.
            // Flatten into (time, key, value) tuples to group by time.
            let merged_props = g
                .properties()
                .temporal()
                .into_iter()
                .map(|(prop_key, prop_view)| {
                    // Collect all the props for a given prop key
                    prop_view.iter_indexed()
                        .map(move |(time, prop_value)| (time, prop_key.clone(), prop_value))
                        .collect::<Vec<_>>() // Need to collect to avoid ref issues with prop_view
                        .into_iter()
                })
                .flatten();

            // Group property (key, value) tuples by time to create rows.
            let rows: Vec<Row> = merged_props
                .chunk_by(|(t, _, _)| *t)
                .into_iter()
                .map(|(timestamp, group)| {
                    let row = group.map(
                        |(_, prop_key, prop_value)| (prop_key, prop_value)
                    ).collect();

                    Row { t: timestamp, row }
                })
                .collect();

            decoder.serialize(&rows)?;

            if let Some(rb) = decoder.flush()? {
                writer.write(&rb)?;
                writer.flush()?;
            }

            Ok(())
        },
    )
}

#[derive(Debug)]
struct Row {
    t: TimeIndexEntry,
    row: HashMap<ArcStr, Prop>,
}

impl Serialize for Row {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_map(Some(self.row.len()))?;

        for (k, v) in self.row.iter() {
            state.serialize_entry(k, &ParquetProp(v))?;
        }

        state.serialize_entry(TIME_COL, &self.t.0)?;
        state.serialize_entry(SECONDARY_INDEX_COL, &self.t.1)?;

        state.end()
    }
}

pub fn encode_graph_cprop(
    g: &GraphStorage,
    graph_type: GraphType,
    path: impl AsRef<Path>,
) -> Result<(), GraphError> {
    run_encode(
        g,
        g.graph_meta().metadata_mapper(),
        1,
        path,
        GRAPH_C_PATH,
        |_| vec![Field::new(TIME_COL, DataType::Int64, true)],
        |_, g, decoder, writer| {
            let row = g.metadata().as_map();
            let time = TimeIndexEntry::new(0, 0); // const props don't have time
            let rows = vec![Row { t: time, row }];

            decoder.serialize(&rows)?;

            if let Some(rb) = decoder.flush()? {
                writer.write(&rb)?;
                writer.flush()?;
            }

            match graph_type {
                GraphType::EventGraph => writer.append_key_value_metadata(KeyValue::new(
                    GRAPH_TYPE.to_string(),
                    Some(EVENT_GRAPH_TYPE.to_string()),
                )),
                GraphType::PersistentGraph => writer.append_key_value_metadata(KeyValue::new(
                    GRAPH_TYPE.to_string(),
                    Some(PERSISTENT_GRAPH_TYPE.to_string()),
                )),
            };

            Ok(())
        },
    )
}
