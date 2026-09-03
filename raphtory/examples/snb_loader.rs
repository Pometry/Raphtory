use raphtory::{
    arrow_loader::df_loaders::edges::ColumnNames,
    errors::GraphError,
    io::parquet_loaders::{load_edges_from_parquet, load_nodes_from_parquet},
    prelude::*,
};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Construct the path to a named Parquet file inside `parquet_dir`.
fn pq(parquet_dir: &Path, name: &str) -> PathBuf {
    parquet_dir.join(format!("{}.parquet", name))
}

#[cfg(target_os = "macos")]
use tikv_jemallocator::Jemalloc;
use raphtory_storage::core_ops::CoreGraphOps;

#[cfg(target_os = "macos")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

struct NodeParquetInput {
    path: PathBuf,
    time_col: String,
    id_col: String,
    node_type: Option<String>,
    node_type_col: Option<String>,
    property_cols: Vec<String>,
}

impl NodeParquetInput {
    fn new<'a>(
        path: impl AsRef<Path>,
        time_col: &str,
        id_col: &str,
        node_type: Option<&str>,
        node_type_col: Option<&str>,
        property_cols: Vec<&str>,
    ) -> NodeParquetInput {
        NodeParquetInput {
            path: path.as_ref().into(),
            time_col: time_col.into(),
            id_col: id_col.into(),
            node_type: node_type.map(Into::into),
            node_type_col: node_type_col.map(Into::into),
            property_cols: property_cols.into_iter().map(|s| s.into()).collect(),
        }
    }

    fn path_as_string(&self) -> &str {
        self.path.iter().last().and_then(|p| p.to_str()).unwrap()
    }
}
struct EdgeParquetInput {
    path: PathBuf,
    time_col: String,
    src_col: String,
    dst_col: String,
    layer: Option<String>,
    property_cols: Vec<String>,
}

impl EdgeParquetInput {
    fn new(
        path: impl AsRef<Path>,
        time_col: &str,
        src_col: &str,
        dst_col: &str,
        layer: Option<&str>,
        property_cols: Vec<&str>,
    ) -> EdgeParquetInput {
        EdgeParquetInput {
            path: path.as_ref().into(),
            time_col: time_col.into(),
            src_col: src_col.into(),
            dst_col: dst_col.into(),
            layer: layer.map(Into::into),
            property_cols: property_cols.into_iter().map(Into::into).collect(),
        }
    }

    fn path_as_string(&self) -> &str {
        self.path.iter().last().and_then(|p| p.to_str()).unwrap()
    }
}

fn load_snb_graph_v2(
    nodes: impl IntoIterator<Item = NodeParquetInput>,
    edges: impl IntoIterator<Item = EdgeParquetInput>,
    graph: &Graph,
) -> Result<(), GraphError> {
    for node in nodes {
        println!("Loading nodes from Parquet file with time column '{}', id column '{}', label column '{:?}', and property columns {:?}...", node.time_col, node.id_col, node.node_type, node.property_cols);
        load_nodes_from_parquet(
            graph,
            &node.path,
            &node.time_col,
            None,
            &node.id_col,
            node.node_type.as_deref(),
            node.node_type_col.as_deref(),
            &node
                .property_cols
                .iter()
                .map(|s| s.as_ref())
                .collect::<Vec<_>>(),
            &[],
            None,
            None,
            None,
            None,
            None,
            true,
            None,
        )?;
        println!(
            " ✓ Finished loading nodes from Parquet file with id column '{}'",
            node.id_col
        );
    }

    for edge in edges {
        println!("Loading edges from Parquet file with time column '{}', src column '{}', dst column '{}', label column '{:?}', and property columns {:?}...", edge.time_col, edge.src_col, edge.dst_col, edge.layer, edge.property_cols);
        load_edges_from_parquet(
            graph,
            &edge.path,
            ColumnNames::new(&edge.time_col, None, &edge.src_col, &edge.dst_col, None),
            true,
            &edge
                .property_cols
                .iter()
                .map(|s| s.as_ref())
                .collect::<Vec<_>>(),
            &[],
            None,
            edge.layer.as_deref(),
            None,
            None,
        )?;
        println!(
            " ✓ Finished loading edges from Parquet file with src column '{}'",
            edge.src_col
        );
    }
    Ok(())
}

/// Load SNB data from Parquet files into a Raphtory Graph.
fn load_snb_graph(
    parquet_dir: &Path,
    filter: Option<Filter>,
    graph: &Graph,
) -> Result<(), GraphError> {
    let node_inputs = [
        NodeParquetInput::new(
            pq(parquet_dir, "place"),
            "_time",
            "_node_id",
            None,
            Some("type"),
            vec!["name", "url", "id"],
        ),
        NodeParquetInput::new(
            pq(parquet_dir, "organisation"),
            "_time",
            "_node_id",
            None,
            Some("type"),
            vec!["name", "url", "id"],
        ),
        NodeParquetInput::new(
            pq(parquet_dir, "tag"),
            "_time",
            "_node_id",
            Some("Tag"),
            None,
            vec!["name", "url", "id"],
        ),
        NodeParquetInput::new(
            pq(parquet_dir, "tagclass"),
            "_time",
            "_node_id",
            Some("TagClass"),
            None,
            vec!["name", "url", "id"],
        ),
        NodeParquetInput::new(
            pq(parquet_dir, "person"),
            "creationDate",
            "_node_id",
            Some("Person"),
            None,
            vec![
                "firstName",
                "lastName",
                "gender",
                "birthday",
                "locationIP",
                "browserUsed",
                "language",
                "email",
                "id",
                "creationDate",
            ],
        ),
        NodeParquetInput::new(
            pq(parquet_dir, "forum"),
            "creationDate",
            "_node_id",
            Some("Forum"),
            None,
            vec!["title", "id", "creationDate"],
        ),
        NodeParquetInput::new(
            pq(parquet_dir, "post"),
            "creationDate",
            "_node_id",
            Some("Post"),
            None,
            vec![
                "imageFile",
                "locationIP",
                "browserUsed",
                "language",
                "content",
                "length",
                "id",
                "creationDate",
            ],
        ),
        NodeParquetInput::new(
            pq(parquet_dir, "comment"),
            "creationDate",
            "_node_id",
            Some("Comment"),
            None,
            vec![
                "locationIP",
                "browserUsed",
                "content",
                "length",
                "id",
                "creationDate",
            ],
        ),
    ];

    let edge_inputs = [
        EdgeParquetInput::new(
            pq(parquet_dir, "place_IS_PART_OF_place"),
            "_time",
            "START_ID",
            "END_ID",
            Some("IS_PART_OF"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "organisation_IS_LOCATED_IN_place"),
            "_time",
            "START_ID",
            "END_ID",
            Some("IS_LOCATED_IN"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "person_IS_LOCATED_IN_place"),
            "_time",
            "START_ID",
            "END_ID",
            Some("IS_LOCATED_IN"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "post_IS_LOCATED_IN_place"),
            "_time",
            "START_ID",
            "END_ID",
            Some("IS_LOCATED_IN"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "forum_HAS_MODERATOR_person"),
            "_time",
            "START_ID",
            "END_ID",
            Some("HAS_MODERATOR"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "post_HAS_CREATOR_person"),
            "_time",
            "START_ID",
            "END_ID",
            Some("HAS_CREATOR"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "comment_HAS_CREATOR_person"),
            "_time",
            "START_ID",
            "END_ID",
            Some("HAS_CREATOR"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "forum_CONTAINER_OF_post"),
            "_time",
            "START_ID",
            "END_ID",
            Some("CONTAINER_OF"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "comment_REPLY_OF_post"),
            "_time",
            "START_ID",
            "END_ID",
            Some("REPLY_OF"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "comment_REPLY_OF_comment"),
            "_time",
            "START_ID",
            "END_ID",
            Some("REPLY_OF"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "person_KNOWS_person"),
            "creationDate",
            "START_ID",
            "END_ID",
            Some("KNOWS"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "person_LIKES_post"),
            "creationDate",
            "START_ID",
            "END_ID",
            Some("LIKES"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "person_LIKES_comment"),
            "creationDate",
            "START_ID",
            "END_ID",
            Some("LIKES"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "forum_HAS_MEMBER_person"),
            "joinDate",
            "START_ID",
            "END_ID",
            Some("HAS_MEMBER"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "person_STUDY_AT_organisation"),
            "_time",
            "START_ID",
            "END_ID",
            Some("STUDY_AT"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "person_WORK_AT_organisation"),
            "_time",
            "START_ID",
            "END_ID",
            Some("WORK_AT"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "post_HAS_TAG_tag"),
            "_time",
            "START_ID",
            "END_ID",
            Some("HAS_TAG"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "comment_HAS_TAG_tag"),
            "_time",
            "START_ID",
            "END_ID",
            Some("HAS_TAG"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "forum_HAS_TAG_tag"),
            "_time",
            "START_ID",
            "END_ID",
            Some("HAS_TAG"),
            vec![],
        ),
        EdgeParquetInput::new(
            pq(parquet_dir, "person_HAS_INTEREST_tag"),
            "_time",
            "START_ID",
            "END_ID",
            Some("HAS_INTEREST"),
            vec![],
        ),
    ];

    let edge_inputs = edge_inputs
        .into_iter()
        .filter(|edge| {
            filter
                .as_ref()
                .and_then(|filter| filter.edges.as_ref())
                .map(|e_f| e_f.iter().any(|name| edge.path_as_string().contains(name)))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();

    let node_inputs = node_inputs
        .into_iter()
        .filter(|node| {
            filter
                .as_ref()
                .and_then(|filter| filter.nodes.as_ref())
                .map(|e_f| e_f.iter().any(|name| node.path_as_string().contains(name)))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();

    println!(
        "edge_inputs: {:?}, node_inputs: {:?}",
        edge_inputs
            .iter()
            .map(|e| e.path_as_string())
            .collect::<Vec<_>>(),
        node_inputs
            .iter()
            .map(|e| e.path_as_string())
            .collect::<Vec<_>>(),
    );

    load_snb_graph_v2(node_inputs, edge_inputs, graph)?;

    println!(
        "\n✅ Graph loaded: {} nodes, {} edges",
        graph.count_nodes(),
        graph.count_edges()
    );
    Ok(())
}

#[derive(Deserialize)]
struct Filter {
    nodes: Option<Vec<String>>,
    edges: Option<Vec<String>>,
}

fn main() {
    let parquet_dir = std::env::args()
        .nth(1)
        .map(|dir| PathBuf::from(dir))
        .unwrap_or_else(|| panic!("Usage: snb_loader <data_dir>"));
    let filter = std::env::args()
        .nth(2)
        .map(|s| serde_json::from_str::<Filter>(&s))
        .transpose()
        .unwrap();

    let graph_path = std::env::args()
        .nth(3)
        .map(|graph| PathBuf::from(graph))
        .unwrap_or_else(|| parquet_dir.join("..").join("graph"));
    if !graph_path.exists() {
        let graph = Graph::new_at_path(&graph_path).unwrap();
        load_snb_graph(&parquet_dir, filter, &graph).unwrap()
    } else {
        let graph = Graph::load(&graph_path).unwrap();
        let now = Instant::now();
        graph.core_graph().build_node_prop_index(None).unwrap();
        println!("Building node index took {:?}", now.elapsed());
    };
}
