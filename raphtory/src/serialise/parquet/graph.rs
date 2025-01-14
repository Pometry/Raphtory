use crate::{
    core::utils::errors::GraphError,
    prelude::{GraphViewOps, Prop},
    serialise::parquet::{model::ParquetProp, run_encode, GRAPH_C_PATH, GRAPH_T_PATH, TIME_COL},
};
use arrow_schema::{DataType, Field};
use itertools::Itertools;
use raphtory_api::core::storage::arc_str::ArcStr;
use serde::{ser::SerializeMap, Serialize};
use std::{collections::HashMap, path::Path};
use crate::db::api::storage::graph::storage_ops::GraphStorage;

pub fn encode_graph_tprop(g: &GraphStorage, path: impl AsRef<Path>) -> Result<(), GraphError> {
    run_encode(
        g,
        g.graph_meta().temporal_prop_meta(),
        1,
        path,
        GRAPH_T_PATH,
        |_| vec![Field::new(TIME_COL, DataType::Int64, false)],
        |_, g, decoder, writer| {
            let merged_props = g
                .properties()
                .temporal()
                .into_iter()
                .map(|(k, view)| view.into_iter().map(move |(t, prop)| (k.clone(), t, prop)))
                .kmerge_by(|(_, t1, _), (_, t2, _)| t1 < t2);

            let mut row = HashMap::<ArcStr, Prop>::new();
            let mut rows = vec![];
            let mut last_t: Option<i64> = None;
            for (key, t1, prop) in merged_props {
                if let Some(last_t) = last_t {
                    if last_t != t1 {
                        let mut old = HashMap::<ArcStr, Prop>::new();
                        std::mem::swap(&mut row, &mut old);
                        rows.push(Row {
                            t: last_t,
                            row: old,
                        });
                    }
                }

                row.insert(key, prop);
                last_t = Some(t1);
            }
            if !row.is_empty() {
                rows.push(Row {
                    t: last_t.unwrap(),
                    row,
                });
            }

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
    t: i64,
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
        state.serialize_entry(TIME_COL, &self.t)?;
        state.end()
    }
}

pub fn encode_graph_cprop(g: &GraphStorage, path: impl AsRef<Path>) -> Result<(), GraphError> {
    run_encode(
        g,
        g.graph_meta().const_prop_meta(),
        1,
        path,
        GRAPH_C_PATH,
        |_| vec![Field::new(TIME_COL, DataType::Int64, true)],
        |_, g, decoder, writer| {
            let row = g.properties().constant().as_map();

            let rows = vec![Row { t: 0, row }];
            decoder.serialize(&rows)?;
            if let Some(rb) = decoder.flush()? {
                writer.write(&rb)?;
                writer.flush()?;
            }

            Ok(())
        },
    )
}
