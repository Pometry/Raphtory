use crate::{
    core::{arrow_dtype_from_prop_type, utils::errors::GraphError},
    db::{
        api::{
            mutation::internal::InternalAdditionOps, storage::graph::storage_ops::GraphStorage,
            view::internal::CoreGraphOps,
        },
        graph::views::deletion_graph::PersistentGraph,
    },
    io::parquet_loaders::{
        load_edge_deletions_from_parquet, load_edge_props_from_parquet, load_edges_from_parquet,
        load_graph_props_from_parquet, load_node_props_from_parquet, load_nodes_from_parquet,
    },
    prelude::*,
    serialise::parquet::{
        edges::encode_edge_deletions,
        graph::{encode_graph_cprop, encode_graph_tprop},
        model::get_id_type,
        nodes::{encode_nodes_cprop, encode_nodes_tprop},
    },
};
use arrow_json::{reader::Decoder, ReaderBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use edges::{encode_edge_cprop, encode_edge_tprop};
use itertools::Itertools;
use model::ParquetTEdge;
use parquet::{
    arrow::{arrow_reader::ArrowReaderMetadata, ArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};
use raphtory_api::{
    core::entities::{properties::props::PropMapper, GidType},
    GraphType,
};
use rayon::iter::{ParallelBridge, ParallelIterator};
use std::{
    fs::File,
    ops::Range,
    path::{Path, PathBuf},
};

mod edges;
mod model;
mod nodes;

mod graph;

pub trait ParquetEncoder {
    fn encode_parquet(&self, path: impl AsRef<Path>) -> Result<(), GraphError>;
}

pub trait ParquetDecoder {
    fn decode_parquet(path: impl AsRef<Path>) -> Result<Self, GraphError>
    where
        Self: Sized;
}

const NODE_ID: &str = "rap_node_id";
const TYPE_COL: &str = "rap_node_type";
const TIME_COL: &str = "rap_time";
const SRC_COL: &str = "rap_src";
const DST_COL: &str = "rap_dst";
const LAYER_COL: &str = "rap_layer";
const EDGES_T_PATH: &str = "edges_t";
const EDGES_D_PATH: &str = "edges_d"; // deletions
const EDGES_C_PATH: &str = "edges_c";
const NODES_T_PATH: &str = "nodes_t";
const NODES_C_PATH: &str = "nodes_c";

const GRAPH_T_PATH: &str = "graph_t";
const GRAPH_C_PATH: &str = "graph_c";

const GRAPH_TYPE: &str = "graph_type";

const EVENT_GRAPH_TYPE: &str = "rap_event_graph";

const PERSISTENT_GRAPH_TYPE: &str = "rap_persistent_graph";

impl ParquetEncoder for Graph {
    fn encode_parquet(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        let gs = self.core_graph().clone();
        encode_graph_storage(&gs, path, GraphType::EventGraph)
    }
}

impl ParquetEncoder for PersistentGraph {
    fn encode_parquet(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        let gs = self.core_graph().clone();
        encode_graph_storage(&gs, path, GraphType::PersistentGraph)
    }
}

fn encode_graph_storage(
    g: &GraphStorage,
    path: impl AsRef<Path>,
    graph_type: GraphType,
) -> Result<(), GraphError> {
    encode_edge_tprop(g, path.as_ref())?;
    encode_edge_cprop(g, path.as_ref())?;
    encode_edge_deletions(g, path.as_ref())?;
    encode_nodes_tprop(g, path.as_ref())?;
    encode_nodes_cprop(g, path.as_ref())?;
    encode_graph_tprop(g, path.as_ref())?;
    encode_graph_cprop(g, graph_type, path.as_ref())?;
    Ok(())
}

pub(crate) fn run_encode(
    g: &GraphStorage,
    meta: &PropMapper,
    size: usize,
    path: impl AsRef<Path>,
    suffix: &str,
    default_fields_fn: impl Fn(&DataType) -> Vec<Field>,
    encode_fn: impl Fn(
            Range<usize>,
            &GraphStorage,
            &mut Decoder,
            &mut ArrowWriter<File>,
        ) -> Result<(), GraphError>
        + Sync,
) -> Result<(), GraphError> {
    let schema = derive_schema(meta, g.id_type(), default_fields_fn)?;
    let root_dir = path.as_ref().join(suffix);
    std::fs::create_dir_all(&root_dir)?;

    if size > 0 {
        let chunk_size = (size / rayon::current_num_threads()).max(128);
        let iter = (0..size).step_by(chunk_size);

        let num_digits = iter.len().to_string().len();

        iter.enumerate()
            .par_bridge()
            .try_for_each(|(chunk, first)| {
                let props = WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .build();
                let items = first..(first + chunk_size).min(size);

                let node_file =
                    File::create(root_dir.join(format!("{chunk:0num_digits$}.parquet")))?;
                let mut writer = ArrowWriter::try_new(node_file, schema.clone(), Some(props))?;

                let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder()?;

                encode_fn(items, g, &mut decoder, &mut writer)?;

                writer.close()?;
                Ok::<_, GraphError>(())
            })?;
    }
    Ok(())
}

pub(crate) fn derive_schema(
    prop_meta: &PropMapper,
    id_type: Option<GidType>,
    default_fields_fn: impl Fn(&DataType) -> Vec<Field>,
) -> Result<SchemaRef, GraphError> {
    let fields = arrow_fields(prop_meta)?;
    let id_type = get_id_type(id_type);

    let make_schema = |id_type: DataType, prop_columns: Vec<Field>| {
        let default_fields = default_fields_fn(&id_type);

        Schema::new(
            default_fields
                .into_iter()
                .chain(prop_columns.into_iter())
                .collect::<Vec<_>>(),
        )
        .into()
    };

    let schema = if let Ok(id_type) = id_type {
        make_schema(id_type, fields)
    } else {
        make_schema(DataType::UInt64, fields)
    };
    Ok(schema)
}

fn arrow_fields(meta: &PropMapper) -> Result<Vec<Field>, GraphError> {
    meta.get_keys()
        .into_iter()
        .filter_map(|name| {
            let prop_id = meta.get_id(&name)?;
            meta.get_dtype(prop_id)
                .map(move |prop_type| (name, prop_type))
        })
        .map(|(name, prop_type)| {
            arrow_dtype_from_prop_type(&prop_type).map(|d_type| Field::new(name, d_type, true))
        })
        .collect()
}

fn ls_parquet_files(dir: &Path) -> Result<impl Iterator<Item = PathBuf>, GraphError> {
    Ok(std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file() && path.extension().map_or(false, |ext| ext == "parquet")))
}

fn collect_prop_columns(
    path: &Path,
    exclude: &[&str],
) -> Result<(Vec<String>, Option<GraphType>), GraphError> {
    let prop_columns_fn =
        |path: &Path, exclude: &[&str]| -> Result<(Vec<String>, Option<GraphType>), GraphError> {
            let reader = ArrowReaderMetadata::load(&File::open(path)?, Default::default())?;
            let cols = reader
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .filter(|f_name| !exclude.iter().any(|ex| ex == f_name))
                .collect_vec();
            let graph_type = reader
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .and_then(|meta| {
                    meta.iter()
                        .find(|kv| &kv.key == GRAPH_TYPE)
                        .and_then(|kv| kv.value.as_ref())
                        .and_then(|v| match v.as_ref() {
                            EVENT_GRAPH_TYPE => Some(GraphType::EventGraph),
                            PERSISTENT_GRAPH_TYPE => Some(GraphType::PersistentGraph),
                            _ => None,
                        })
                });
            Ok((cols, graph_type))
        };
    let mut prop_columns = vec![];
    let mut g_type: Option<GraphType> = None;
    for path in ls_parquet_files(path)? {
        let (columns, tpe) = prop_columns_fn(&path, exclude)?;
        if g_type.is_none() {
            g_type = tpe;
        }
        prop_columns.extend_from_slice(&columns);
    }
    prop_columns.sort();
    prop_columns.dedup();
    Ok((prop_columns, g_type))
}

fn decode_graph_storage(
    path: impl AsRef<Path>,
    expected_gt: GraphType,
) -> Result<GraphStorage, GraphError> {
    let g = Graph::new();

    let c_graph_path = path.as_ref().join(GRAPH_C_PATH);

    let g_type = {
        let exclude = vec![TIME_COL];
        let (c_props, g_type) = collect_prop_columns(&c_graph_path, &exclude)?;
        load_graph_props_from_parquet(&g, &c_graph_path, TIME_COL, None, Some(&c_props))?;

        g_type.ok_or_else(|| GraphError::LoadFailure("Graph type not found".to_string()))?
    };

    if g_type != expected_gt {
        return Err(GraphError::LoadFailure(format!(
            "Expected graph type {:?}, got {:?}",
            expected_gt, g_type
        )));
    }

    let t_graph_path = path.as_ref().join(GRAPH_T_PATH);

    if std::fs::exists(&t_graph_path)? {
        let exclude = vec![TIME_COL];
        let (t_props, _) = collect_prop_columns(&t_graph_path, &exclude)?;
        load_graph_props_from_parquet(&g, &t_graph_path, TIME_COL, Some(&t_props), None)?;
    }

    let exclude = vec![TIME_COL, SRC_COL, DST_COL, LAYER_COL];
    let t_edge_path = path.as_ref().join(EDGES_T_PATH);

    if std::fs::exists(&t_edge_path)? {
        let (t_prop_columns, _) = collect_prop_columns(&t_edge_path, &exclude)?;
        let t_prop_columns = t_prop_columns
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();

        load_edges_from_parquet(
            &g,
            &t_edge_path,
            TIME_COL,
            SRC_COL,
            DST_COL,
            &t_prop_columns,
            &[],
            None,
            None,
            Some(LAYER_COL),
        )?;
    }

    let c_edge_path = path.as_ref().join(EDGES_C_PATH);
    if std::fs::exists(&c_edge_path)? {
        let (c_prop_columns, _) = collect_prop_columns(&c_edge_path, &exclude)?;
        let constant_properties = c_prop_columns
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();

        load_edge_props_from_parquet(
            &g,
            &c_edge_path,
            SRC_COL,
            DST_COL,
            &constant_properties,
            None,
            None,
            Some(LAYER_COL),
        )?;
    }

    let d_edge_path = path.as_ref().join(EDGES_D_PATH);
    if std::fs::exists(&d_edge_path)? {
        load_edge_deletions_from_parquet(
            g.core_graph(),
            &d_edge_path,
            TIME_COL,
            SRC_COL,
            DST_COL,
            None,
            Some(LAYER_COL),
        )?;
    }

    let t_node_path = path.as_ref().join(NODES_T_PATH);

    if std::fs::exists(&t_node_path)? {
        let exclude = vec![NODE_ID, TIME_COL, TYPE_COL];
        let (t_prop_columns, _) = collect_prop_columns(&t_node_path, &exclude)?;
        let t_prop_columns = t_prop_columns
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();

        load_nodes_from_parquet(
            &g,
            &t_node_path,
            TIME_COL,
            NODE_ID,
            None,
            Some(TYPE_COL),
            &t_prop_columns,
            &[],
            None,
        )?;
    }

    let c_node_path = path.as_ref().join(NODES_C_PATH);
    if std::fs::exists(&c_node_path)? {
        let exclude = vec![NODE_ID, TYPE_COL];
        let (c_prop_columns, _) = collect_prop_columns(&c_node_path, &exclude)?;
        let c_prop_columns = c_prop_columns
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();

        load_node_props_from_parquet(
            &g,
            &c_node_path,
            NODE_ID,
            None,
            Some(TYPE_COL),
            &c_prop_columns,
            None,
        )?;
    }

    Ok(g.core_graph().clone())
}
impl ParquetDecoder for Graph {
    fn decode_parquet(path: impl AsRef<Path>) -> Result<Self, GraphError>
    where
        Self: Sized,
    {
        let gs = decode_graph_storage(path, GraphType::EventGraph)?;
        Ok(Graph::from_internal_graph(gs))
    }
}

impl ParquetDecoder for PersistentGraph {
    fn decode_parquet(path: impl AsRef<Path>) -> Result<Self, GraphError>
    where
        Self: Sized,
    {
        let gs = decode_graph_storage(path, GraphType::PersistentGraph)?;
        Ok(PersistentGraph::from_internal_graph(gs))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        db::{
            api::{
                storage::graph::edges::edge_storage_ops::EdgeStorageOps,
                view::internal::TimeSemantics,
            },
            graph::graph::assert_graph_equal,
        },
        test_utils::{
            build_edge_list_dyn, build_graph, build_graph_strat, build_nodes_dyn, GraphFixture,
            NodeFixture,
        },
    };
    use chrono::{DateTime, Utc};
    use proptest::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn node_temp_props() {
        let nodes: NodeFixture = [(0, 0, vec![("a".to_string(), Prop::U8(5))])].into();
        check_parquet_encoding(nodes.into());
    }

    #[test]
    fn edge_const_props_maps() {
        let g_fixture = GraphFixture {
            edges: vec![],
            no_props_edges: vec![(1, 1, 1)],
            edge_deletions: vec![],
            edge_const_props: vec![
                (
                    (0u64, 0u64),
                    vec![("x".to_string(), Prop::map([("n", Prop::U64(23))]))],
                ),
                (
                    (0u64, 1u64),
                    vec![(
                        "a".to_string(),
                        Prop::map([("a", Prop::U8(1)), ("b", Prop::str("baa"))]),
                    )],
                ),
            ]
            .into_iter()
            .collect(),
            nodes: Default::default(),
        };

        check_parquet_encoding(g_fixture);
    }

    #[test]
    fn write_edges_to_parquet() {
        let dt = "2012-12-12 12:12:12+00:00"
            .parse::<DateTime<Utc>>()
            .unwrap();
        check_parquet_encoding(
            [
                (0, 1, 12, vec![("one".to_string(), Prop::DTime(dt))], None),
                (
                    1,
                    2,
                    12,
                    vec![
                        ("two".to_string(), Prop::I32(2)),
                        ("three".to_string(), Prop::I64(3)),
                        (
                            "four".to_string(),
                            Prop::List(vec![Prop::I32(1), Prop::I32(2)].into()),
                        ),
                    ],
                    Some("b"),
                ),
                (
                    2,
                    3,
                    12,
                    vec![
                        ("three".to_string(), Prop::I64(3)),
                        ("one".to_string(), Prop::DTime(dt)),
                        ("five".to_string(), Prop::List(vec![Prop::str("a")].into())),
                    ],
                    Some("a"),
                ),
            ]
            .into(),
        );
    }

    #[test]
    fn write_edges_empty_prop_first() {
        check_parquet_encoding(
            [
                (
                    0,
                    1,
                    12,
                    vec![("a".to_string(), Prop::List(vec![].into()))],
                    None,
                ),
                (
                    1,
                    2,
                    12,
                    vec![("a".to_string(), Prop::List(vec![Prop::str("aa")].into()))],
                    None,
                ),
            ]
            .into(),
        );
    }

    #[test]
    fn edges_dates() {
        let dt = "2012-12-12 12:12:12+00:00"
            .parse::<DateTime<Utc>>()
            .unwrap();
        check_parquet_encoding(
            [(
                0,
                0,
                0,
                vec![("a".to_string(), Prop::List(vec![Prop::DTime(dt)].into()))],
                None,
            )]
            .into(),
        );
    }
    #[test]
    fn edges_maps() {
        let dt = "2012-12-12 12:12:12+00:00"
            .parse::<DateTime<Utc>>()
            .unwrap();
        check_parquet_encoding(
            [(
                0,
                0,
                0,
                vec![(
                    "a".to_string(),
                    Prop::map([("a", Prop::DTime(dt)), ("b", Prop::str("s"))]),
                )],
                None,
            )]
            .into(),
        );
    }

    #[test]
    fn edges_maps2() {
        check_parquet_encoding(
            [
                (
                    0,
                    0,
                    0,
                    vec![("a".to_string(), Prop::map([("a", Prop::I32(1))]))],
                    None,
                ),
                (
                    0,
                    0,
                    0,
                    vec![("a".to_string(), Prop::map([("b", Prop::str("x"))]))],
                    None,
                ),
            ]
            .into(),
        );
    }

    #[test]
    fn edges_maps3() {
        check_parquet_encoding(
            [
                (0, 0, 0, vec![("a".to_string(), Prop::U8(5))], None),
                (
                    0,
                    0,
                    0,
                    vec![("b".to_string(), Prop::map([("c", Prop::U8(66))]))],
                    None,
                ),
            ]
            .into(),
        );
    }

    #[test]
    fn edges_map4() {
        let g_fix = GraphFixture {
            edges: vec![(0, 0, 0, vec![("a".to_string(), Prop::U8(5))], None)],
            edge_deletions: vec![(0, 0, 1)],
            no_props_edges: vec![],
            edge_const_props: vec![(
                (0u64, 0u64),
                vec![(
                    "x".to_string(),
                    Prop::List(
                        vec![
                            Prop::map([("n", Prop::I64(23))]),
                            Prop::map([("b", Prop::F64(0.2))]),
                        ]
                        .into(),
                    ),
                )],
            )]
            .into_iter()
            .collect(),
            nodes: Default::default(),
        };

        check_parquet_encoding(g_fix)
    }

    // proptest
    fn check_parquet_encoding(edges: GraphFixture) {
        let g = build_graph(edges);
        let temp_dir = tempfile::tempdir().unwrap();
        g.encode_parquet(&temp_dir).unwrap();
        let g2 = Graph::decode_parquet(&temp_dir).unwrap();
        assert_graph_equal(&g, &g2);
    }

    #[test]
    fn nodes_props_1() {
        let dt = "2012-12-12 12:12:12+00:00"
            .parse::<DateTime<Utc>>()
            .unwrap();
        let node_fixtures = NodeFixture {
            nodes: vec![(
                0,
                0,
                vec![
                    ("a".to_string(), Prop::U8(5)),
                    ("a".to_string(), Prop::U8(5)),
                ],
            )],
            node_const_props: vec![(0, vec![("b".to_string(), Prop::DTime(dt))])]
                .into_iter()
                .collect(),
        };

        check_parquet_encoding(node_fixtures.into());
    }

    fn check_graph_props(nf: NodeFixture) {
        let g = Graph::new();
        let temp_dir = tempfile::tempdir().unwrap();
        for (_, t, props) in nf.nodes {
            g.add_properties(t, props).unwrap();
        }

        let const_props = nf
            .node_const_props
            .into_iter()
            .flat_map(|(_, props)| props)
            .collect::<HashMap<_, _>>();
        g.add_constant_properties(const_props).unwrap();

        g.encode_parquet(&temp_dir).unwrap();
        let g2 = Graph::decode_parquet(&temp_dir).unwrap();
        assert_graph_equal(&g, &g2);
    }

    #[test]
    fn graph_props() {
        let mut nf: NodeFixture = [(0, 1, vec![("a".to_string(), Prop::U8(5))])].into();
        nf.node_const_props = vec![(1, vec![("b".to_string(), Prop::str("baa"))])]
            .into_iter()
            .collect();
        check_graph_props(nf)
    }

    #[test]
    fn edge_props_1() {
        let gp_fix = GraphFixture {
            edge_const_props: vec![((0u64, 0u64), vec![("a".to_string(), Prop::I64(5))])]
                .into_iter()
                .collect(),
            edge_deletions: vec![(6, 2, 4444)],
            edges: vec![(
                0,
                0,
                -67,
                vec![
                    ("x".to_string(), Prop::I64(5)),
                    ("b".to_string(), Prop::Bool(false)),
                ],
                Some("a"),
            )],
            no_props_edges: vec![(7, 0, 469)],
            nodes: NodeFixture::default(),
        };
        check_parquet_encoding(gp_fix);
    }

    #[test]
    fn graph_const_props() {
        let mut nf: NodeFixture = NodeFixture::default();
        nf.node_const_props = vec![(1, vec![("b".to_string(), Prop::str("baa"))])]
            .into_iter()
            .collect();
        check_parquet_encoding(nf.into())
    }

    #[test]
    fn write_graph_props_to_parquet() {
        proptest!(|(nodes in build_nodes_dyn(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 10))| {
            check_graph_props(nodes);
        });
    }

    #[test]
    fn write_nodes_any_props_to_parquet() {
        proptest!(|(nodes in build_nodes_dyn(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 10))| {
            check_parquet_encoding(nodes.into());
        });
    }
    #[test]
    fn write_edges_any_props_to_parquet() {
        proptest!(|(edges in build_edge_list_dyn(10, 10))| {
            check_parquet_encoding(edges);
        });
    }

    #[test]
    fn write_graph_to_parquet() {
        proptest!(|(edges in build_graph_strat(10, 10))| {
            check_parquet_encoding(edges);
        })
    }
}
