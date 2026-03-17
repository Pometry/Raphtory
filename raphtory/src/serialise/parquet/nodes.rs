use crate::{
    core::utils::iter::GenLockedIter,
    db::{api::state::ops::GraphView, graph::node::NodeView},
    errors::GraphError,
    prelude::{GraphViewOps, NodeViewOps},
    serialise::parquet::{
        model::{ParquetCNode, ParquetTNode},
        run_encode_indexed, NODES_C_PATH, NODES_T_PATH, NODE_ID_COL, NODE_VID_COL, ROW_GROUP_SIZE,
        SECONDARY_INDEX_COL, TIME_COL, TYPE_COL, TYPE_ID_COL,
    },
};
use arrow::datatypes::{DataType, Field};
use itertools::Itertools;
use raphtory_api::iter::IntoDynBoxed;
use raphtory_storage::graph::graph::GraphStorage;
use std::path::Path;

pub(crate) fn encode_nodes_tprop<G: GraphView>(
    g: &G,
    path: impl AsRef<Path>,
) -> Result<(), GraphError> {
    run_encode_indexed(
        g,
        g.node_meta().temporal_prop_mapper(),
        g.core_graph().lock().nodes().row_groups_par_iter(),
        path,
        NODES_T_PATH,
        |_| {
            vec![
                Field::new(NODE_VID_COL, DataType::UInt64, false),
                Field::new(TIME_COL, DataType::Int64, false),
                Field::new(SECONDARY_INDEX_COL, DataType::UInt64, true),
            ]
        },
        |nodes, g, decoder, writer| {
            let nodes = nodes.collect::<Vec<_>>();
            let nodes = nodes.into_iter();

            let cols = g.node_meta().temporal_prop_mapper().all_keys();
            let cols = &cols;
            for node_rows in nodes
                .map(|vid| NodeView::new_internal(g, vid))
                .flat_map(move |node| {
                    GenLockedIter::from(node, |node| {
                        node.rows()
                            .map(|(t, props)| ParquetTNode {
                                export_vid: node.node.0,
                                cols,
                                t,
                                props,
                            })
                            .into_dyn_boxed()
                    })
                })
                .chunks(ROW_GROUP_SIZE)
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

pub(crate) fn encode_nodes_cprop<G: GraphView>(
    g: &G,
    path: impl AsRef<Path>,
) -> Result<(), GraphError> {
    run_encode_indexed(
        g,
        g.node_meta().metadata_mapper(),
        g.core_graph().lock().nodes().row_groups_par_iter(),
        path,
        NODES_C_PATH,
        |id_type| {
            vec![
                Field::new(NODE_ID_COL, id_type.clone(), false),
                Field::new(NODE_VID_COL, DataType::UInt64, false),
                Field::new(TYPE_COL, DataType::Utf8, true),
                Field::new(TYPE_ID_COL, DataType::UInt64, true),
            ]
        },
        |nodes, g, decoder, writer| {
            for node_rows in nodes
                .map(|vid| NodeView::new_internal(g, vid))
                .map(move |node| ParquetCNode {
                    node,
                    export_vid: node.node.0,
                    export_node_type_id: node.node_type_id(),
                })
                .chunks(ROW_GROUP_SIZE)
                .into_iter()
                .map(|chunk| chunk.collect_vec())
            // scope for the decoder
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
