#[cfg(feature = "io")]
use raphtory::io::{
    arrow::df_loaders::edges::ColumnNames,
    parquet_loaders::{load_edges_from_parquet, load_nodes_from_parquet},
};
use raphtory::{errors::GraphError, prelude::*};
use std::path::{Path, PathBuf};

/// Construct the path to a named Parquet file inside `parquet_dir`.
fn pq(parquet_dir: &Path, name: &str) -> PathBuf {
    parquet_dir.join(format!("{}.parquet", name))
}

#[cfg(target_os = "macos")]
use tikv_jemallocator::Jemalloc;

#[cfg(target_os = "macos")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Load SNB data from Parquet files into a Raphtory Graph.
#[cfg(feature = "io")]
fn load_snb_graph(parquet_dir: &Path, graph: &Graph) -> Result<(), GraphError> {
    // ── Static Nodes ──────────────────────────────────────────────────────

    // println!("Loading Places...");
    // load_nodes_from_parquet(
    //     graph,
    //     &pq(parquet_dir, "place"),
    //     "_time",
    //     None,
    //     "_node_id",
    //     None,
    //     Some("type"),
    //     &["name", "url", "id"],
    //     &[],
    //     None,
    //     None,
    //     true,
    //     None,
    // )?;
    // println!("  ✓ places");

    // let fp = pq(parquet_dir, "place_IS_PART_OF_place");
    // if fp.exists() {
    //     load_edges_from_parquet(
    //         graph,
    //         &fp,
    //         ColumnNames::new("_time", None, "START_ID", "END_ID", None),
    //         true,
    //         &[],
    //         &[],
    //         None,
    //         Some("IS_PART_OF"),
    //         None,
    //         None,
    //     )?;
    //     graph.flush()?;
    //     println!("  ✓ IS_PART_OF edges");
    // }

    // println!("Loading Organisations...");
    // load_nodes_from_parquet(
    //     graph,
    //     &pq(parquet_dir, "organisation"),
    //     "_time",
    //     None,
    //     "_node_id",
    //     None,
    //     Some("type"),
    //     &["name", "url", "id"],
    //     &[],
    //     None,
    //     None,
    //     true,
    //     None,
    // )?;
    // println!("  ✓ organisations");

    // load_edges_from_parquet(
    //     graph,
    //     &pq(parquet_dir, "organisation_IS_LOCATED_IN_place"),
    //     ColumnNames::new("_time", None, "START_ID", "END_ID", None),
    //     true,
    //     &[],
    //     &[],
    //     None,
    //     Some("IS_LOCATED_IN"),
    //     None,
    //     None,
    // )?;
    // graph.flush()?;
    // println!("  ✓ Organisation IS_LOCATED_IN edges");

    // println!("Loading Tags...");
    // load_nodes_from_parquet(
    //     graph,
    //     &pq(parquet_dir, "tag"),
    //     "_time",
    //     None,
    //     "_node_id",
    //     Some("Tag"),
    //     None,
    //     &["name", "url", "id"],
    //     &[],
    //     None,
    //     None,
    //     true,
    //     None,
    // )?;
    // println!("  ✓ tags");

    // let fp = pq(parquet_dir, "tagclass");
    // if fp.exists() {
    //     println!("Loading TagClasses...");
    //     load_nodes_from_parquet(
    //         graph,
    //         &fp,
    //         "_time",
    //         None,
    //         "_node_id",
    //         Some("TagClass"),
    //         None,
    //         &["name", "url", "id"],
    //         &[],
    //         None,
    //         None,
    //         true,
    //         None,
    //     )?;
    //     println!("  ✓ tag classes");
    // }

    // let fp = pq(parquet_dir, "tag_HAS_TYPE_tagclass");
    // if fp.exists() {
    //     load_edges_from_parquet(
    //         graph,
    //         &fp,
    //         ColumnNames::new("_time", None, "START_ID", "END_ID", None),
    //         true,
    //         &[],
    //         &[],
    //         None,
    //         Some("HAS_TYPE"),
    //         None,
    //         None,
    //     )?;
    //     graph.flush()?;
    //     println!("  ✓ HAS_TYPE edges");
    // }

    // let fp = pq(parquet_dir, "tagclass_IS_SUBCLASS_OF_tagclass");
    // if fp.exists() {
    //     load_edges_from_parquet(
    //         graph,
    //         &fp,
    //         ColumnNames::new("_time", None, "START_ID", "END_ID", None),
    //         true,
    //         &[],
    //         &[],
    //         None,
    //         Some("IS_SUBCLASS_OF"),
    //         None,
    //         None,
    //     )?;
    //     graph.flush()?;
    //     println!("  ✓ IS_SUBCLASS_OF edges");
    // }

    // // ── Dynamic Nodes ─────────────────────────────────────────────────────

    // println!("Loading Persons...");
    // load_nodes_from_parquet(
    //     graph,
    //     &pq(parquet_dir, "person"),
    //     "creationDate",
    //     None,
    //     "_node_id",
    //     Some("Person"),
    //     None,
    //     &[
    //         "firstName",
    //         "lastName",
    //         "gender",
    //         "birthday",
    //         "locationIP",
    //         "browserUsed",
    //         "language",
    //         "email",
    //         "id",
    //         "creationDate",
    //     ],
    //     &[],
    //     None,
    //     None,
    //     true,
    //     None,
    // )?;
    // println!("  ✓ persons");

    // load_edges_from_parquet(
    //     graph,
    //     &pq(parquet_dir, "person_IS_LOCATED_IN_place"),
    //     ColumnNames::new("_time", None, "START_ID", "END_ID", None),
    //     true,
    //     &[],
    //     &[],
    //     None,
    //     Some("IS_LOCATED_IN"),
    //     None,
    //     None,
    // )?;
    // graph.flush()?;

    // println!("Loading Forums...");
    // load_nodes_from_parquet(
    //     graph,
    //     &pq(parquet_dir, "forum"),
    //     "creationDate",
    //     None,
    //     "_node_id",
    //     Some("Forum"),
    //     None,
    //     &["title", "id", "creationDate"],
    //     &[],
    //     None,
    //     None,
    //     true,
    //     None,
    // )?;
    // println!("  ✓ forums");

    // load_edges_from_parquet(
    //     graph,
    //     &pq(parquet_dir, "forum_HAS_MODERATOR_person"),
    //     ColumnNames::new("_time", None, "START_ID", "END_ID", None),
    //     true,
    //     &[],
    //     &[],
    //     None,
    //     Some("HAS_MODERATOR"),
    //     None,
    //     None,
    // )?;
    // graph.flush()?;

    // println!("Loading Posts...");
    // load_nodes_from_parquet(
    //     graph,
    //     &pq(parquet_dir, "post"),
    //     "creationDate",
    //     None,
    //     "_node_id",
    //     Some("Post"),
    //     None,
    //     &[
    //         "imageFile",
    //         "locationIP",
    //         "browserUsed",
    //         "language",
    //         "content",
    //         "length",
    //         "id",
    //         "creationDate",
    //     ],
    //     &[],
    //     None,
    //     None,
    //     true,
    //     None,
    // )?;
    // println!("  ✓ posts");

    // load_edges_from_parquet(
    //     graph,
    //     &pq(parquet_dir, "post_HAS_CREATOR_person"),
    //     ColumnNames::new("_time", None, "START_ID", "END_ID", None),
    //     true,
    //     &[],
    //     &[],
    //     None,
    //     Some("HAS_CREATOR"),
    //     None,
    //     None,
    // )?;
    // graph.flush()?;

    // load_edges_from_parquet(
    //     graph,
    //     &pq(parquet_dir, "post_IS_LOCATED_IN_place"),
    //     ColumnNames::new("_time", None, "START_ID", "END_ID", None),
    //     true,
    //     &[],
    //     &[],
    //     None,
    //     Some("IS_LOCATED_IN"),
    //     None,
    //     None,
    // )?;
    // graph.flush()?;

    // load_edges_from_parquet(
    //     graph,
    //     &pq(parquet_dir, "forum_CONTAINER_OF_post"),
    //     ColumnNames::new("_time", None, "START_ID", "END_ID", None),
    //     true,
    //     &[],
    //     &[],
    //     None,
    //     Some("CONTAINER_OF"),
    //     None,
    //     None,
    // )?;
    // graph.flush()?;

    println!("Loading Comments...");
    load_nodes_from_parquet(
        graph,
        &pq(parquet_dir, "comment"),
        "creationDate",
        None,
        "_node_id",
        Some("Comment"),
        None,
        &[
            "locationIP",
            "browserUsed",
            "content",
            "length",
            "id",
            "creationDate",
        ],
        &[],
        None,
        None,
        true,
        None,
    )?;
    println!("  ✓ comments");
    // graph.flush()?;

    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "comment_HAS_CREATOR_person"),
        ColumnNames::new("_time", None, "START_ID", "END_ID", None),
        true,
        &[],
        &[],
        None,
        Some("HAS_CREATOR"),
        None,
        None,
    )?;
    // graph.flush()?;

    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "comment_IS_LOCATED_IN_place"),
        ColumnNames::new("_time", None, "START_ID", "END_ID", None),
        true,
        &[],
        &[],
        None,
        Some("IS_LOCATED_IN"),
        None,
        None,
    )?;
    // graph.flush()?;

    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "comment_REPLY_OF_post"),
        ColumnNames::new("_time", None, "START_ID", "END_ID", None),
        true,
        &[],
        &[],
        None,
        Some("REPLY_OF"),
        None,
        None,
    )?;

    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "comment_REPLY_OF_comment"),
        ColumnNames::new("_time", None, "START_ID", "END_ID", None),
        true,
        &[],
        &[],
        None,
        Some("REPLY_OF"),
        None,
        None,
    )?;
    // graph.flush()?;

    // ── Edge-only relationships ───────────────────────────────────────────

    println!("Loading KNOWS edges...");
    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "person_KNOWS_person"),
        ColumnNames::new("creationDate", None, "START_ID", "END_ID", None),
        true,
        &["creationDate"],
        &[],
        None,
        Some("KNOWS"),
        None,
        None,
    )?;
    graph.flush()?;
    println!("  ✓ KNOWS edges");

    println!("Loading LIKES edges...");
    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "person_LIKES_post"),
        ColumnNames::new("creationDate", None, "START_ID", "END_ID", None),
        true,
        &["creationDate"],
        &[],
        None,
        Some("LIKES"),
        None,
        None,
    )?;
    graph.flush()?;
    println!("  ✓ LIKES (Post) edges");

    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "person_LIKES_comment"),
        ColumnNames::new("creationDate", None, "START_ID", "END_ID", None),
        true,
        &["creationDate"],
        &[],
        None,
        Some("LIKES"),
        None,
        None,
    )?;
    graph.flush()?;
    println!("  ✓ LIKES (Comment) edges");

    println!("Loading HAS_MEMBER edges...");
    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "forum_HAS_MEMBER_person"),
        ColumnNames::new("joinDate", None, "START_ID", "END_ID", None),
        true,
        &["joinDate"],
        &[],
        None,
        Some("HAS_MEMBER"),
        None,
        None,
    )?;
    graph.flush()?;
    println!("  ✓ HAS_MEMBER edges");

    println!("Loading STUDY_AT edges...");
    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "person_STUDY_AT_organisation"),
        ColumnNames::new("_time", None, "START_ID", "END_ID", None),
        true,
        &["classYear"],
        &[],
        None,
        Some("STUDY_AT"),
        None,
        None,
    )?;
    graph.flush()?;
    println!("  ✓ STUDY_AT edges");

    println!("Loading WORK_AT edges...");
    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "person_WORK_AT_organisation"),
        ColumnNames::new("_time", None, "START_ID", "END_ID", None),
        true,
        &["workFrom"],
        &[],
        None,
        Some("WORK_AT"),
        None,
        None,
    )?;
    graph.flush()?;
    println!("  ✓ WORK_AT edges");

    println!("Loading HAS_TAG edges...");
    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "post_HAS_TAG_tag"),
        ColumnNames::new("_time", None, "START_ID", "END_ID", None),
        true,
        &[],
        &[],
        None,
        Some("HAS_TAG"),
        None,
        None,
    )?;
    graph.flush()?;

    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "comment_HAS_TAG_tag"),
        ColumnNames::new("_time", None, "START_ID", "END_ID", None),
        true,
        &[],
        &[],
        None,
        Some("HAS_TAG"),
        None,
        None,
    )?;
    graph.flush()?;

    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "forum_HAS_TAG_tag"),
        ColumnNames::new("_time", None, "START_ID", "END_ID", None),
        true,
        &[],
        &[],
        None,
        Some("HAS_TAG"),
        None,
        None,
    )?;
    graph.flush()?;
    println!("  ✓ HAS_TAG edges");

    println!("Loading HAS_INTEREST edges...");
    load_edges_from_parquet(
        graph,
        &pq(parquet_dir, "person_HAS_INTEREST_tag"),
        ColumnNames::new("_time", None, "START_ID", "END_ID", None),
        true,
        &[],
        &[],
        None,
        Some("HAS_INTEREST"),
        None,
        None,
    )?;
    graph.flush()?;
    println!("  ✓ HAS_INTEREST edges");

    println!(
        "\n✅ Graph loaded: {} nodes, {} edges",
        graph.count_nodes(),
        graph.count_edges()
    );
    Ok(())
}

#[cfg(feature = "io")]
fn main() {
    let parquet_dir = std::env::args()
        .nth(1)
        .map(|dir| PathBuf::from(dir))
        .unwrap_or_else(|| panic!("Usage: snb_loader <data_dir>"));
    let graph_path = std::env::args()
        .nth(2)
        .map(|graph| PathBuf::from(graph))
        .unwrap_or_else(|| parquet_dir.join("..").join("graph"));
    let graph = Graph::new_at_path(&graph_path).unwrap();
    load_snb_graph(&parquet_dir, &graph).unwrap()
}

#[cfg(not(feature = "io"))]
fn main() {}
