use crate::{
    core::utils::{errors::GraphError, iter::GenLockedIter},
    db::{
        api::{properties::internal::TemporalPropertiesRowView, view::internal::CoreGraphOps},
        graph::node::NodeView,
    },
    serialise::parquet::{
        model::{ParquetCNode, ParquetTNode},
        run_encode, NODES_C_PATH, NODES_T_PATH, NODE_ID, TIME_COL, TYPE_COL,
    },
};
use arrow_schema::{DataType, Field};
use itertools::Itertools;
use raphtory_api::{core::entities::VID, iter::IntoDynBoxed};
use std::path::Path;
use crate::db::api::storage::graph::storage_ops::GraphStorage;

pub(crate) fn encode_nodes_tprop(g: &GraphStorage, path: impl AsRef<Path>) -> Result<(), GraphError> {
    run_encode(
        g,
        g.node_meta().temporal_prop_meta(),
        g.unfiltered_num_nodes(),
        path,
        NODES_T_PATH,
        |id_type| {
            vec![
                Field::new(NODE_ID, id_type.clone(), false),
                Field::new(TIME_COL, DataType::Int64, false),
                Field::new(TYPE_COL, DataType::Utf8, true),
            ]
        },
        |nodes, g, decoder, writer| {
            let row_group_size = 100_000.min(nodes.len());

            let cols = g
                .node_meta()
                .temporal_prop_meta()
                .get_keys()
                .into_iter()
                .collect_vec();
            let cols = &cols;
            for node_rows in nodes
                .into_iter()
                .map(VID)
                .map(|vid| NodeView::new_internal(g, vid))
                .flat_map(move |node| {
                    GenLockedIter::from(node, |node| {
                        node.rows()
                            .into_iter()
                            .map(|(t, props)| ParquetTNode {
                                node: *node,
                                cols,
                                t,
                                props,
                            })
                            .into_dyn_boxed()
                    })
                })
                .chunks(row_group_size)
                .into_iter()
                .map(|chunk| chunk.collect_vec())
            {
                decoder.serialize(&node_rows)?;
                if let Some(rb) = decoder.flush()? {
                    writer.write(&rb)?;
                    writer.flush()?;
                }
            }
            Ok(())
        },
    )
}

pub(crate) fn encode_nodes_cprop(g: &GraphStorage, path: impl AsRef<Path>) -> Result<(), GraphError> {
    run_encode(
        g,
        g.node_meta().const_prop_meta(),
        g.unfiltered_num_nodes(),
        path,
        NODES_C_PATH,
        |id_type| {
            vec![
                Field::new(NODE_ID, id_type.clone(), false),
                Field::new(TYPE_COL, DataType::Utf8, true),
            ]
        },
        |nodes, g, decoder, writer| {
            let row_group_size = 100_000.min(nodes.len());

            for node_rows in nodes
                .into_iter()
                .map(VID)
                .map(|vid| NodeView::new_internal(g, vid))
                .map(move |node| ParquetCNode { node })
                .chunks(row_group_size)
                .into_iter()
                .map(|chunk| chunk.collect_vec())
            {
                decoder.serialize(&node_rows)?;
                if let Some(rb) = decoder.flush()? {
                    writer.write(&rb)?;
                    writer.flush()?;
                }
            }
            Ok(())
        },
    )
}
