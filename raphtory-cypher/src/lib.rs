#[cfg(feature = "storage")]
pub use cypher::*;

#[cfg(feature = "storage")]
pub mod executor;
#[cfg(feature = "storage")]
pub mod hop;
#[cfg(feature = "storage")]
pub mod parser;
#[cfg(feature = "storage")]
pub mod transpiler;

#[cfg(feature = "storage")]
mod cypher {
    use arrow::compute::take;
    use std::sync::Arc;

    use arrow_array::{builder, Array, RecordBatch, UInt64Array};
    use arrow_schema::{ArrowError, DataType};
    use datafusion::{
        dataframe::DataFrame,
        error::DataFusionError,
        execution::{
            config::SessionConfig,
            context::{SQLOptions, SessionContext, SessionState},
            runtime_env::RuntimeEnv,
        },
        logical_expr::{create_udf, ColumnarValue, LogicalPlan, Volatility},
        physical_plan::SendableRecordBatchStream,
    };

    use super::{
        executor::{table_provider::edge::EdgeListTableProvider, ExecError},
        *,
    };
    use raphtory::disk_graph::DiskGraphStorage;

    use crate::{
        executor::table_provider::node::NodeTableProvider,
        hop::rule::{HopQueryPlanner, HopRule},
    };

    pub use polars_arrow as arrow2;

    pub async fn run_cypher(
        query: &str,
        g: &DiskGraphStorage,
        enable_hop_optim: bool,
    ) -> Result<DataFrame, ExecError> {
        let (ctx, plan) = prepare_plan(query, g, enable_hop_optim).await?;
        println!("{}", plan.display_indent().to_string());
        let df = ctx.execute_logical_plan(plan).await?;
        Ok(df)
    }

    pub async fn prepare_plan(
        query: &str,
        g: &DiskGraphStorage,
        enable_hop_optim: bool,
    ) -> Result<(SessionContext, LogicalPlan), ExecError> {
        // println!("Running query: {:?}", query);
        let query = super::parser::parse_cypher(query)?;

        let config = SessionConfig::from_env()?.with_information_schema(true);

        // config.options_mut().optimizer.skip_failed_rules = true;
        // config.options_mut().optimizer.top_down_join_key_reordering = false;

        let runtime = Arc::new(RuntimeEnv::default());
        let state = if enable_hop_optim {
            SessionState::new_with_config_rt(config, runtime)
                .with_query_planner(Arc::new(HopQueryPlanner {}))
                .add_optimizer_rule(Arc::new(HopRule::new(g.clone())))
        } else {
            SessionState::new_with_config_rt(config, runtime)
        };
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
        // println!("SQL AST: {:?}", query);
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
        Ok((ctx, plan))
    }

    pub async fn run_cypher_to_streams(
        query: &str,
        graph: &DiskGraphStorage,
    ) -> Result<Vec<SendableRecordBatchStream>, ExecError> {
        let df = run_cypher(query, graph, true).await?;
        let stream = df.execute_stream_partitioned().await?;
        Ok(stream)
    }

    pub async fn run_sql(query: &str, graph: &DiskGraphStorage) -> Result<DataFrame, ExecError> {
        let ctx = SessionContext::new();

        for layer in graph.as_ref().layer_names() {
            let table = EdgeListTableProvider::new(layer, graph.clone())?;
            ctx.register_table(layer, Arc::new(table))?;
        }

        let node_table_provider = NodeTableProvider::new(graph.clone())?;
        ctx.register_table("nodes", Arc::new(node_table_provider))?;

        // let state = ctx.state();
        // let dialect = state.config().options().sql_parser.dialect.as_str();
        // let sql_ast = ctx.state().sql_to_statement(query, dialect)?;
        // println!("SQL AST: {:?}", sql_ast);

        let df = ctx.sql(query).await?;
        Ok(df)
    }

    pub fn take_record_batch(
        record_batch: &RecordBatch,
        indices: &dyn Array,
    ) -> Result<RecordBatch, ArrowError> {
        let columns = record_batch
            .columns()
            .iter()
            .map(|c| take(c, indices, None))
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(record_batch.schema(), columns)
    }

    #[cfg(test)]
    mod test {
        use arrow::compute::concat_batches;
        use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
        use std::path::Path;

        // FIXME: actually assert the tests below
        // use pretty_assertions::assert_eq;
        use arrow::util::pretty::print_batches;
        use arrow_array::RecordBatch;
        use tempfile::tempdir;

        use raphtory::{disk_graph::DiskGraphStorage, prelude::*};

        use crate::{run_cypher, run_sql};

        lazy_static::lazy_static! {
            static ref EDGES: Vec<(u64, u64, i64, f64)> = vec![
                (0, 1, 1, 3.),
                (0, 1, 2, 4.),
                (0, 2, 0, 1.),
                (1, 2, 2, 4.),
                (1, 3, 3, 4.),
                (1, 4, 1, 1.),
                (3, 2, 5, 5.),
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
                (1, 0, 3, 4., "beea".to_string()),
                (1, 0, 3, 1., "beex".to_string()),
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
            let graph = DiskGraphStorage::make_simple_graph(graph_dir, &EDGES, 3, 2);

            let df = run_cypher("match ()-[e]->() RETURN *", &graph, true)
                .await
                .unwrap();

            let data = df.collect().await.unwrap();

            print_batches(&data).expect("failed to print batches");
        }

        #[tokio::test]
        async fn select_table_order_by() {
            let graph_dir = tempdir().unwrap();
            let graph = DiskGraphStorage::make_simple_graph(graph_dir, &EDGES, 3, 2);

            let df = run_cypher("match ()-[e]->() RETURN * ORDER by e.weight", &graph, true)
                .await
                .unwrap();

            let data = df.collect().await.unwrap();

            print_batches(&data).expect("failed to print batches");
        }

        mod arrow2_load {
            use std::path::PathBuf;

            use crate::arrow2::{
                array::{PrimitiveArray, StructArray},
                datatypes::*,
            };
            use arrow::util::pretty::print_batches;
            use tempfile::tempdir;

            use raphtory::disk_graph::{graph_impl::ParquetLayerCols, DiskGraphStorage};

            use crate::run_cypher;

            fn schema() -> ArrowSchema {
                let srcs = Field::new("srcs", ArrowDataType::UInt64, false);
                let dsts = Field::new("dsts", ArrowDataType::UInt64, false);
                let time = Field::new("bla_time", ArrowDataType::Int64, false);
                let weight = Field::new("weight", ArrowDataType::Float64, true);
                ArrowSchema::from(vec![srcs, dsts, time, weight])
            }

            #[tokio::test]
            async fn select_table_time_column_different_name() {
                let graph_dir = tempdir().unwrap();

                let srcs = PrimitiveArray::from_vec(vec![1u64, 2u64, 2u64, 2u64]).boxed();
                let dsts = PrimitiveArray::from_vec(vec![3u64, 3u64, 4u64, 4u64]).boxed();
                let time = PrimitiveArray::from_vec(vec![2i64, 3i64, 4i64, 5i64]).boxed();
                let weight =
                    PrimitiveArray::from_vec(vec![3.14f64, 4.14f64, 5.14f64, 6.14f64]).boxed();

                let chunk = StructArray::new(
                    ArrowDataType::Struct(schema().fields),
                    vec![srcs, dsts, time, weight],
                    None,
                );

                // let node_gids = PrimitiveArray::from_vec((1u64..=4u64).collect()).boxed();

                let edge_lists = vec![chunk];

                let graph =
                    DiskGraphStorage::load_from_edge_lists(&edge_lists, 20, 20, graph_dir, 2, 0, 1)
                        .unwrap();

                let df = run_cypher("match ()-[e]->() RETURN *", &graph, true)
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

                let graph = DiskGraphStorage::load_from_parquets(
                    graph_dir,
                    layer_parquet_cols,
                    None,
                    100,
                    100,
                    None,
                    None,
                    1,
                    None,
                )
                .unwrap();

                let df = run_cypher("match ()-[e]->() RETURN *", &graph, true)
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

            let graph = DiskGraphStorage::from_graph(&graph, graph_dir).unwrap();

            let df = run_cypher("match ()-[e1]->(b)-[e2]->(), (b)-[e3]->() RETURN e1.src, e1.id, b.id, e2.id, e2.dst, e3.id, e3.dst", &graph, true)
                .await
                .unwrap();
            let data = df.collect().await.unwrap();
            print_batches(&data).expect("failed to print batches");

            let df = run_cypher("match (b)-[e3]->(), ()-[e1]->(b)-[e2]->() RETURN e1.src, e1.id, b.id, e2.id, e2.dst, e3.id, e3.dst", &graph, true)
                .await
                .unwrap();
            let data = df.collect().await.unwrap();
            print_batches(&data).expect("failed to print batches");
        }

        #[tokio::test]
        async fn select_table_filter_weight() {
            let graph_dir = tempdir().unwrap();
            let graph = DiskGraphStorage::make_simple_graph(graph_dir, &EDGES, 10, 10);

            let df = run_cypher("match ()-[e {src: 0}]->() RETURN *", &graph, true)
                .await
                .unwrap();

            let data = df.collect().await.unwrap();

            print_batches(&data).expect("failed to print batches");

            let df = run_cypher(
                "match ()-[e]->() where e.rap_time >2 and e.weight<7 RETURN *",
                &graph,
                true,
            )
            .await
            .unwrap();

            let data = df.collect().await.unwrap();
            print_batches(&data).expect("failed to print batches");
        }

        #[tokio::test]
        async fn two_hops() {
            let graph_dir = tempdir().unwrap();
            let graph = DiskGraphStorage::make_simple_graph(graph_dir, &EDGES, 100, 100);

            let query = "match ()-[e1]->()-[e2]->() return e1.src as start, e1.dst as mid, e2.dst as end ORDER BY start, mid, end";

            let rb_hop = run_to_rb(&graph, query, true).await;
            let rb_join = run_to_rb(&graph, query, false).await;

            assert_eq!(rb_hop, rb_join);
        }

        async fn run_to_rb(
            graph: &DiskGraphStorage,
            query: &str,
            enable_hop_optim: bool,
        ) -> RecordBatch {
            let df = run_cypher(query, &graph, enable_hop_optim).await.unwrap();
            let data = df.collect().await.unwrap();
            print_batches(&data).unwrap();
            let schema = data.first().map(|rb| rb.schema()).unwrap();
            concat_batches(&schema, data.iter()).unwrap()
        }

        #[tokio::test]
        #[ignore] // Hop optimization is not yet fully implemented
        async fn three_hops() {
            let graph_dir = tempdir().unwrap();
            let graph = DiskGraphStorage::make_simple_graph(graph_dir, &EDGES, 100, 100);

            let query = "match ()-[e1]->()-[e2]->()-[e3]->() return * ORDER BY e1.src, e1.dst, e2.src, e2.dst, e3.src, e3.dst";
            let hop_rb = run_to_rb(&graph, query, true).await;
            let hop_join = run_to_rb(&graph, query, false).await;

            assert_eq!(hop_rb.num_rows(), hop_join.num_rows());
            assert_eq!(hop_rb, hop_join);
        }

        #[tokio::test]
        async fn three_hops_with_condition() {
            let graph_dir = tempdir().unwrap();
            let graph = DiskGraphStorage::make_simple_graph(graph_dir, &EDGES, 100, 100);

            let df = run_cypher(
                "match ()-[e1]->()-[e2]->()<-[e3]-() where e2.weight > 5 return *",
                &graph,
                true,
            )
            .await
            .unwrap();

            let data = df.collect().await.unwrap();
            print_batches(&data).unwrap();
        }

        #[tokio::test]
        async fn five_hops() {
            let graph_dir = tempdir().unwrap();
            let graph = DiskGraphStorage::make_simple_graph(graph_dir, &EDGES, 100, 100);

            let df = run_cypher(
                "match ()-[e1]->()-[e2]->()-[e3]->()-[e4]->()-[e5]->() return *",
                &graph,
                true,
            )
            .await
            .unwrap();

            let data = df.collect().await.unwrap();
            print_batches(&data).unwrap();
        }

        fn make_graph_with_str_col(graph_dir: impl AsRef<Path>) -> DiskGraphStorage {
            let graph = Graph::new();

            load_edges_with_str_props(&graph, None);

            DiskGraphStorage::from_graph(&graph, graph_dir).unwrap()
        }

        fn make_graph_with_node_props(graph_dir: impl AsRef<Path>) -> DiskGraphStorage {
            let graph = Graph::new();

            load_nodes(&graph);
            load_edges_with_str_props(&graph, None);

            DiskGraphStorage::from_graph(&graph, graph_dir).unwrap()
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
                true,
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
                "match ()-[e]->() where e.name ends with 'z' return count(e.name)",
                &graph,
                true,
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

            let df = run_cypher("match (n) return n", &graph, true)
                .await
                .unwrap();
            let data = df.collect().await.unwrap();
            print_batches(&data).unwrap();
        }

        #[tokio::test]
        async fn select_node_names_from_edges() {
            let graph_dir = tempdir().unwrap();
            let graph = make_graph_with_node_props(graph_dir);

            // let df = run_sql("WITH e AS (SELECT * FROM _default), b AS (SELECT * FROM nodes) SELECT e.*, b.gid FROM e JOIN b ON e.dst = b.id", &graph).await.unwrap();
            let df = run_cypher("match ()-[e]->(b) return e,b.gid", &graph, false)
                .await
                .unwrap();
            let data = df.collect().await.unwrap();
            print_batches(&data).unwrap();
        }

        #[tokio::test]
        async fn select_node_names_from_edges_both() {
            let graph_dir = tempdir().unwrap();
            let graph = make_graph_with_node_props(graph_dir);

            let df = run_cypher("match (a)-[e]->(b) return a.gid, e, b.gid", &graph, false)
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
                true,
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

            let df = run_cypher("match (n) return count(n)", &graph, true)
                .await
                .unwrap();
            let data = df.collect().await.unwrap();
            print_batches(&data).unwrap();

            let df = run_cypher("match (n) return count(*)", &graph, true)
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

            let graph = DiskGraphStorage::from_graph(&g, graph_dir).unwrap();

            let df = run_cypher(
                "match ()-[e:_default|LAYER1|LAYER2]->() where (e.weight > 3 and e.weight < 5) or e.name starts with 'xb' return e",
                &graph,
                true).await.unwrap();

            let data = df.collect().await.unwrap();
            print_batches(&data).unwrap();
        }

        #[tokio::test]
        async fn hop_two_different_layers() {
            let graph_dir = tempdir().unwrap();
            let g = Graph::new();

            load_edges_1(&g, Some("LAYER1"));
            load_edges_with_str_props(&g, Some("LAYER2"));

            let graph = DiskGraphStorage::from_graph(&g, graph_dir).unwrap();

            let df = run_cypher("match ()-[e2:LAYER2]->() RETURN *", &graph, true)
                .await
                .unwrap();
            let data = df.collect().await.unwrap();
            print_batches(&data).expect("failed to print batches");

            let df = run_cypher(
                "match ()-[e1:LAYER1]->()-[e2:LAYER2]->() RETURN count(*)",
                &graph,
                true,
            )
            .await
            .unwrap();

            let data = df.collect().await.unwrap();
            print_batches(&data).expect("failed to print batches");
        }

        #[tokio::test]
        async fn select_all_multiple_layers() {
            let graph_dir = tempdir().unwrap();
            let g = Graph::new();

            load_edges_1(&g, Some("LAYER1"));
            load_edges_with_str_props(&g, Some("LAYER2"));

            let graph = DiskGraphStorage::from_graph(&g, graph_dir).unwrap();

            let df = run_cypher("match ()-[e]->() RETURN *", &graph, true)
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

            let graph = DiskGraphStorage::from_graph(&g, graph_dir).unwrap();
            let df = run_cypher("match ()-[e]->() return type(e), e", &graph, true)
                .await
                .unwrap();

            let data = df.collect().await.unwrap();
            print_batches(&data).unwrap();
        }

        #[tokio::test]
        async fn select_contains_count_star() {
            let graph_dir = tempdir().unwrap();
            let graph = make_graph_with_str_col(graph_dir);

            let df = run_cypher("match ()-[e]->() return count(*)", &graph, true)
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
                "match ()-[e]->() where e.name contains 'a' return e limit 2",
                &graph,
                true,
            )
            .await
            .unwrap();
            let data = df.collect().await.unwrap();
            print_batches(&data).unwrap();
        }
    }
}
