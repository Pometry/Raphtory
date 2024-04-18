use std::sync::Arc;

use arrow_array::{builder, UInt64Array};
use arrow_schema::DataType;
use datafusion::{
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{
        config::SessionConfig,
        context::{SQLOptions, SessionContext, SessionState},
        runtime_env::RuntimeEnv,
    },
    logical_expr::{create_udf, ColumnarValue, Volatility},
    physical_plan::SendableRecordBatchStream,
};
use executor::{table_provider::edge::EdgeListTableProvider, ExecError};
use parser::ast::*;
use raphtory::arrow::graph_impl::ArrowGraph;

use crate::executor::table_provider::node::NodeTableProvider;

pub mod executor;
pub mod hop;
pub mod parser;
pub mod transpiler;

pub async fn run_cypher(query: &str, g: &ArrowGraph) -> Result<DataFrame, ExecError> {
    println!("Running query: {:?}", query);
    let query = parser::parse_cypher(query)?;

    let config = SessionConfig::from_env()?.with_information_schema(true);
    // config.options_mut().optimizer.skip_failed_rules = true; // should probably raise these with Datafusion

    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::new_with_config_rt(config, runtime);
    let ctx = SessionContext::new_with_state(state);

    let graph = g.as_ref();
    for layer in graph.layer_names() {
        let edge_list_table = EdgeListTableProvider::new(layer, g.clone())?;
        ctx.register_table(layer, Arc::new(edge_list_table))?;
    }

    let node_table_provider = NodeTableProvider::new(g.clone())?;
    ctx.register_table("nodes", Arc::new(node_table_provider))?;
    let layer_names = graph.layer_names().to_vec();

    ctx.register_udf(create_udf(
        "type",
        vec![DataType::UInt64],
        DataType::Utf8.into(),
        Volatility::Immutable,
        Arc::new(move |cols| {
            let layer_id_col = match &cols[0] {
                ColumnarValue::Array(a) => a.clone(),
                ColumnarValue::Scalar(a) => a.to_array()?,
            };

            let layer_id_col = layer_id_col
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Expected column of type u64".to_string())
                })?;

            let mut type_col = builder::StringBuilder::new();
            for layer_id in layer_id_col.values() {
                let layer_name = layer_names
                    .get(*layer_id as usize)
                    .ok_or_else(|| DataFusionError::Execution("Layer not found".to_string()))?;
                type_col.append_value(layer_name);
            }
            Ok(ColumnarValue::Array(Arc::new(type_col.finish())))
        }),
    ));
    ctx.refresh_catalogs().await?;
    let query = transpiler::to_sql(query, g);

    println!("SQL: {:?}", query.to_string());
    let plan = ctx
        .state()
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
            query,
        )))
        .await?;
    let opts = SQLOptions::new();
    opts.verify_plan(&plan)?;

    let plan = ctx.state().optimize(&plan)?;
    // println!("PLAN! {:?}", plan);
    let df = ctx.execute_logical_plan(plan).await?;
    Ok(df)
}

pub async fn run_cypher_to_streams(
    query: &str,
    graph: &ArrowGraph,
) -> Result<Vec<SendableRecordBatchStream>, ExecError> {
    let df = run_cypher(query, graph).await?;
    let stream = df.execute_stream_partitioned().await?;
    Ok(stream)
}

pub async fn run_sql(query: &str, graph: &ArrowGraph) -> Result<DataFrame, ExecError> {
    let ctx = SessionContext::new();

    for layer in graph.as_ref().layer_names() {
        let table = EdgeListTableProvider::new(layer, graph.clone())?;
        ctx.register_table(layer, Arc::new(table))?;
    }

    let df = ctx.sql(query).await?;
    Ok(df)
}

#[cfg(test)]
mod test {

    use std::path::Path;

    use raphtory::{arrow::graph_impl::ArrowGraph, prelude::*};
    use tempfile::tempdir;

    // FIXME: actually assert the tests below
    // use pretty_assertions::assert_eq;
    use arrow::util::pretty::print_batches;

    use crate::run_cypher;

    lazy_static::lazy_static! {
    static ref EDGES: Vec<(u64, u64, i64, f64)> = vec![
            (0, 1, 1, 3.),
            (0, 1, 2, 4.),
            (0, 3, 0, 1.),
            (1, 2, 2, 4.),
            (1, 2, 3, 4.),
            (1, 5, 1, 1.),
            (2, 3, 5, 5.),
            (3, 4, 1, 6.),
            (3, 4, 3, 6.),
            (3, 5, 7, 6.),
            (4, 5, 9, 7.),
        ];

    static ref EDGES2: Vec<(u64, u64, i64, f64, String)> = vec![
            (0, 1, 1, 3., "baa".to_string()),
            (0, 2, 2, 7., "buu".to_string()),
            (2, 3, 1, 9., "xbaa".to_string()),
            (2, 3, 2, 1., "xbaa".to_string()),
            (3, 0, 3, 4., "beea".to_string()),
            (3, 0, 3, 1., "beex".to_string()),
            (4, 1, 5, 5., "baaz".to_string()),
            (4, 5, 1, 6., "bxx".to_string()),
            (5, 6, 3, 6., "mbaa".to_string()),
            (6, 4, 7, 8., "baa".to_string()),
            (6, 4, 9, 7., "bzz".to_string()),
        ];

        // a star graph with 5 nodes an 4 edges
    static ref EDGES3: Vec<(u64, u64, i64, f64)> = vec![
            (0, 2, 2, 7.),
            (0, 3, 3, 9.),
            (0, 4, 4, 1.),
            (1, 0, 1, 3.),
        ];


    // (id, name, age, city)
    static ref NODES: Vec<(u64, String, i64, Option<String>)> = vec![
            (0, "Alice", 30, None),
            (1, "Bob", 25, Some( "Paris" )),
            (2, "Charlie", 35, Some( "Berlin" )),
            (3, "David", 40, None),
            (4, "Eve", 45, Some( "London" )),
            (5, "Frank", 50, Some( "Berlin" )),
            (6, "Grace", 55, Some( "Paris" )),
    ].into_iter().map(|(id, name, age, city)| {
        (id, name.to_string(), age, city.map(|s| s.to_string()))
    }).collect();
    }

    //TODO: need better way of testing these, since they run in parallel order of batches is non-deterministic

    #[tokio::test]
    async fn select_table() {
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &EDGES, 100, 100);

        let df = run_cypher("match ()-[e]->() RETURN *", &graph)
            .await
            .unwrap();

        let data = df.collect().await.unwrap();

        print_batches(&data).expect("failed to print batches");
    }

    mod arrow2_load {
        use std::{num::NonZeroUsize, path::PathBuf};

        use arrow2::{
            array::{PrimitiveArray, StructArray},
            datatypes::*,
        };
        use raphtory::arrow::graph_impl::{ArrowGraph, ParquetLayerCols};
        use tempfile::tempdir;

        use crate::run_cypher;
        use arrow::util::pretty::print_batches;

        fn schema() -> Schema {
            let srcs = Field::new("srcs", DataType::UInt64, false);
            let dsts = Field::new("dsts", DataType::UInt64, false);
            let time = Field::new("bla_time", DataType::Int64, false);
            let weight = Field::new("weight", DataType::Float64, true);
            Schema::from(vec![srcs, dsts, time, weight])
        }

        #[tokio::test]
        async fn select_table_time_column_different_name() {
            let graph_dir = tempdir().unwrap();

            let srcs = PrimitiveArray::from_vec(vec![1u64, 2u64, 2u64, 2u64]).boxed();
            let dsts = PrimitiveArray::from_vec(vec![3u64, 3u64, 4u64, 4u64]).boxed();
            let time = PrimitiveArray::from_vec(vec![2i64, 3i64, 4i64, 5i64]).boxed();
            let weight = PrimitiveArray::from_vec(vec![3.14f64, 4.14f64, 5.14f64, 6.14f64]).boxed();

            let chunk = StructArray::new(
                DataType::Struct(schema().fields),
                vec![srcs, dsts, time, weight],
                None,
            );

            // let node_gids = PrimitiveArray::from_vec((1u64..=4u64).collect()).boxed();

            let edge_lists = vec![chunk];

            let graph = ArrowGraph::load_from_edge_lists(
                &edge_lists,
                NonZeroUsize::new(1).unwrap(),
                20,
                20,
                graph_dir,
                0,
                1,
                2,
            )
            .unwrap();

            let df = run_cypher("match ()-[e]->() RETURN *", &graph)
                .await
                .unwrap();

            let data = df.collect().await.unwrap();

            print_batches(&data).expect("failed to print batches");
        }

        #[tokio::test]
        async fn select_table_parquet_column_different_name() {
            let graph_dir = tempdir().unwrap();
            // relative to current dir back to parent dir then ./resource/netflowsorted
            let netflow_layer_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .map(|p| p.join("raphtory/resources/test/netflow2.parquet"))
                .unwrap();

            let v1_layer_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .map(|p| p.join("raphtory/resources/test/wls2.parquet"))
                .unwrap();

            let layer_parquet_cols = vec![
                ParquetLayerCols {
                    parquet_dir: netflow_layer_path.to_str().unwrap(),
                    layer: "netflow",
                    src_col: "source",
                    dst_col: "destination",
                    time_col: "time",
                },
                ParquetLayerCols {
                    parquet_dir: v1_layer_path.to_str().unwrap(),
                    layer: "wls",
                    src_col: "src",
                    dst_col: "dst",
                    time_col: "epoch_time",
                },
            ];

            let graph = ArrowGraph::load_from_parquets(
                graph_dir,
                layer_parquet_cols,
                None,
                100,
                100,
                None,
                None,
                1,
            )
            .unwrap();

            let df = run_cypher("match ()-[e]->() RETURN *", &graph)
                .await
                .unwrap();

            let data = df.collect().await.unwrap();

            print_batches(&data).expect("failed to print batches");
        }
    }

    #[tokio::test]
    async fn fork_path_on_star_graph() {

        let graph_dir = tempdir().unwrap();

        let graph = Graph::new();
        load_nodes(&graph);
        load_star_edges(&graph);

        let graph = ArrowGraph::from_graph(&graph, graph_dir).unwrap();

        // WITH
        // e1 AS (SELECT * FROM _default),
        // e2 AS (SELECT * FROM _default),
        // e3 AS (SELECT * FROM _default),
        // a AS (SELECT * FROM nodes),
        // b AS (SELECT * FROM nodes),
        // c AS (SELECT * FROM nodes),
        // d AS (SELECT * FROM nodes)
        // SELECT a.id, b.id, c.id, d.id
        // FROM e1
        // JOIN a ON e1.src = a.id
        // JOIN b ON e1.dst = b.id
        // JOIN e2 ON b.id = e2.src
        // JOIN e3 ON b.id = e3.src
        // JOIN d ON e3.dst = d.id
        // JOIN c ON e2.dst = c.id

        // FIXME: match (b)-[e3]->(d), (a)-[e1]->(b)-[e2]->(c) RETURN a.id, b.id, c.id, d.id
        // WITH
        // e3 AS (SELECT * FROM _default),
        // e1 AS (SELECT * FROM _default),
        // e2 AS (SELECT * FROM _default),
        // b AS (SELECT * FROM nodes),
        // d AS (SELECT * FROM nodes),
        // a AS (SELECT * FROM nodes),
        // c AS (SELECT * FROM nodes)
        // SELECT a.id, b.id, c.id, d.id
        // FROM e3
        // JOIN b ON e3.src = b.id
        // JOIN d ON e3.dst = d.id
        //
        // match ()-[e1]->(b)-[e2]->(), (b)-[e3]->() RETURN e1.src, b.id, e2.dst, e3.dst
        // WITH
        // e1 AS (SELECT * FROM _default),
        // e2 AS (SELECT * FROM _default),
        // e3 AS (SELECT * FROM _default),
        // b AS (SELECT * FROM nodes)
        // SELECT e1.src, b.id, e2.dst, e3.dst
        // FROM e1
        // JOIN b ON e1.dst = b.id
        // JOIN e2 ON b.id = e2.src
        // JOIN e3 ON b.id = e3.src

        let df = run_cypher("match ()-[e1]->(b)-[e2]->(), (b)-[e3]->() RETURN e1.src, b.id, e2.dst, e3.dst", &graph)
            .await
            .unwrap();

        let data = df.collect().await.unwrap();

        print_batches(&data).expect("failed to print batches");
    }

    #[tokio::test]
    async fn select_table_filter_weight() {
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &EDGES, 10, 10);

        let df = run_cypher("match ()-[e {src: 0}]->() RETURN *", &graph)
            .await
            .unwrap();

        let data = df.collect().await.unwrap();

        print_batches(&data).expect("failed to print batches");

        let df = run_cypher(
            "match ()-[e]->() where e.rap_time >2 and e.weight<7 RETURN *",
            &graph,
        )
        .await
        .unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).expect("failed to print batches");
    }

    #[tokio::test]
    async fn two_hops() {
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &EDGES, 100, 100);

        let df = run_cypher(
            "match ()-[e1]->()-[e2]->() return e1.src as start, e1.dst as mid, e2.dst as end",
            &graph,
        )
        .await
        .unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn three_hops() {
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &EDGES, 100, 100);

        let df = run_cypher(
            "match ()-[e1]->()-[e2]->()<-[e3]-() where e2.weight > 5 return *",
            &graph,
        )
        .await
        .unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn five_hops() {
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &EDGES, 100, 100);

        let df = run_cypher(
            "match ()-[e1]->()-[e2]->()-[e3]->()-[e4]->()-[e5]->() return *",
            &graph,
        )
        .await
        .unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    fn make_graph_with_str_col(graph_dir: impl AsRef<Path>) -> ArrowGraph {
        let graph = Graph::new();

        load_edges_with_str_props(&graph, None);

        ArrowGraph::from_graph(&graph, graph_dir).unwrap()
    }

    fn make_graph_with_node_props(graph_dir: impl AsRef<Path>) -> ArrowGraph {
        let graph = Graph::new();

        load_nodes(&graph);
        load_edges_with_str_props(&graph, None);

        ArrowGraph::from_graph(&graph, graph_dir).unwrap()
    }

    fn load_nodes(graph: &Graph) {
        for (id, name, age, city) in NODES.iter() {
            let nv = graph.add_node(0, *id, NO_PROPS, None).unwrap();
            nv.add_constant_properties(vec![
                ("name", Prop::str(name.as_ref())),
                ("age", Prop::I64(*age)),
            ])
            .unwrap();
            if let Some(city) = city {
                nv.add_constant_properties(vec![("city", Prop::str(city.as_ref()))])
                    .unwrap();
            }
        }
    }

    fn load_edges_with_str_props(graph: &Graph, layer: Option<&str>) {
        for (src, dst, t, weight, name) in EDGES2.iter() {
            graph
                .add_edge(
                    *t,
                    *src,
                    *dst,
                    [
                        ("weight", Prop::F64(*weight)),
                        ("name", Prop::Str(name.to_owned().into())),
                    ],
                    layer,
                )
                .unwrap();
        }
    }

    fn load_edges_1(graph: &Graph, layer: Option<&str>) {
        for (src, dst, t, weight) in EDGES.iter() {
            graph
                .add_edge(*t, *src, *dst, [("weight", Prop::F64(*weight))], layer)
                .unwrap();
        }
    }

    fn load_star_edges(graph: &Graph) {
        for (src, dst, t, weight) in EDGES3.iter() {
            graph
                .add_edge(*t, *src, *dst, [("weight", Prop::F64(*weight))], None)
                .unwrap();
        }
    }

    #[tokio::test]
    async fn select_contains() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_str_col(graph_dir);

        let df = run_cypher(
            "match ()-[e]->() where e.name ends WITH 'z' RETURN e",
            &graph,
        )
        .await
        .unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_contains_count() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_str_col(graph_dir);

        let df = run_cypher(
            "match ()-[e]-() where e.name ends with 'z' return count(e.name)",
            &graph,
        )
        .await
        .unwrap();
        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_all_nodes() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_node_props(graph_dir);

        let df = run_cypher("match (n) return n", &graph).await.unwrap();
        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_node_names_from_edges() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_node_props(graph_dir);

        let df = run_cypher("match (a)-[e]->(b) return a.name, e, b.name", &graph)
            .await
            .unwrap();
        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_node_names_2_hops() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_node_props(graph_dir);

        let df = run_cypher(
            "match (a)-[e1]->(b)-[e2]->(c) return a.name, b.name, c.name",
            &graph,
        )
        .await
        .unwrap();
        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_count_nodes() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_node_props(graph_dir);

        let df = run_cypher("match (n) return count(n)", &graph)
            .await
            .unwrap();
        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();

        let df = run_cypher("match (n) return count(*)", &graph)
            .await
            .unwrap();
        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_union_multiple_layers() {
        let graph_dir = tempdir().unwrap();
        let g = Graph::new();

        load_edges_1(&g, Some("LAYER1"));
        load_edges_with_str_props(&g, Some("LAYER2"));

        let graph = ArrowGraph::from_graph(&g, graph_dir).unwrap();

        let df = run_cypher(
            "match ()-[e:_default|LAYER1|LAYER2]-() where (e.weight > 3 and e.weight < 5) or e.name starts with 'xb' return e",
            &graph,
        ).await.unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_all_multiple_layers() {
        let graph_dir = tempdir().unwrap();
        let g = Graph::new();

        load_edges_1(&g, Some("LAYER1"));
        load_edges_with_str_props(&g, Some("LAYER2"));

        let graph = ArrowGraph::from_graph(&g, graph_dir).unwrap();

        let df = run_cypher("match ()-[e]->() RETURN *", &graph)
            .await
            .unwrap();

        let data = df.collect().await.unwrap();

        print_batches(&data).expect("failed to print batches");
    }

    #[tokio::test]
    async fn select_all_layers_expand_layer_type() {
        let graph_dir = tempdir().unwrap();
        let g = Graph::new();

        load_edges_1(&g, Some("LAYER1"));
        load_edges_with_str_props(&g, Some("LAYER2"));

        let graph = ArrowGraph::from_graph(&g, graph_dir).unwrap();
        let df = run_cypher("match ()-[e]-() return type(e), e", &graph)
            .await
            .unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_contains_count_star() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_str_col(graph_dir);

        let df = run_cypher("match ()-[e]-() return count(*)", &graph)
            .await
            .unwrap();
        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_contains_limit() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_str_col(graph_dir);

        let df = run_cypher(
            "match ()-[e]-() where e.name contains 'a' return e limit 2",
            &graph,
        )
        .await
        .unwrap();
        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }
}
