//! `GraphLoader` provides some default implementations for loading a pre-built graph.
//! This base class is used to load in-built graphs such as the LOTR, reddit and StackOverflow.
use crate::graph::PyGraph;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

/// Load the Lord of the Rings dataset into a graph.
/// The dataset is available at https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv
/// and is a list of interactions between characters in the Lord of the Rings books
/// and movies. The dataset is a CSV file with the following columns:
///
/// * src_id: The ID of the source character
/// * dst_id: The ID of the destination character
/// * time: The time of the interaction (in page)
///
/// Dataset statistics:
///    * Number of nodes (subreddits) 139
///    * Number of edges (hyperlink between subreddits) 701
///
///
/// Arguments:
///    shards: The number of shards to use for the graph
///
/// Returns:
///   A Graph containing the LOTR dataset
#[pyfunction]
#[pyo3(signature = (shards=1))]
pub(crate) fn lotr_graph(shards: usize) -> PyResult<Py<PyGraph>> {
    PyGraph::py_from_db_graph(raphtory::graph_loader::example::lotr_graph::lotr_graph(
        shards,
    ))
}

/// Load (a subset of) Reddit hyperlinks dataset into a graph.
/// The dataset is available at http://snap.stanford.edu/data/soc-redditHyperlinks-title.tsv
/// The hyperlink network represents the directed connections between two subreddits (a subreddit
/// is a community on Reddit). We also provide subreddit embeddings. The network is extracted
/// from publicly available Reddit data of 2.5 years from Jan 2014 to April 2017.
/// *NOTE: It may take a while to download the dataset
///
/// Dataset statistics:
///   * Number of nodes (subreddits) 35,776
///   * Number of edges (hyperlink between subreddits) 137,821
///   * Timespan Jan 2014 - April 2017
///
/// Source:
///     * S. Kumar, W.L. Hamilton, J. Leskovec, D. Jurafsky. Community Interaction and Conflict
///     on the Web. World Wide Web Conference, 2018.
///
/// Properties:
///
///  * SOURCE_SUBREDDIT: the subreddit where the link originates
///  * TARGET_SUBREDDIT: the subreddit where the link ends
///  * POST_ID: the post in the source subreddit that starts the link
///  * TIMESTAMP: time time of the post
///  * POST_LABEL: label indicating if the source post is explicitly negative towards the target
/// post. The value is -1 if the source is negative towards the target, and 1 if it is neutral or
/// positive. The label is created using crowd-sourcing and training a text based classifier, and
/// is better than simple sentiment analysis of the posts. Please see the reference paper for details.
///  * POST_PROPERTIES: a vector representing the text properties of the source post, listed as a
/// list of comma separated numbers. This can be found on the source website
///
/// Arguments:
///   shards: The number of shards to use for the graph
///   timeout_seconds: The number of seconds to wait for the dataset to download
///
/// Returns:
///  A Graph containing the Reddit hyperlinks dataset
#[pyfunction]
#[pyo3(signature = (shards=1,timeout_seconds=600))]
pub(crate) fn reddit_hyperlink_graph(shards: usize, timeout_seconds: u64) -> PyResult<Py<PyGraph>> {
    PyGraph::py_from_db_graph(
        raphtory::graph_loader::example::reddit_hyperlinks::reddit_graph(
            shards,
            timeout_seconds,
            false,
        ),
    )
}
#[pyfunction]
#[pyo3(signature = (uri,username,password,database="neo4j".to_string(),shards=1))]
pub(crate) fn neo4j_movie_graph(
    uri: String,
    username: String,
    password: String,
    database: String,
    shards: usize,
) -> PyResult<Py<PyGraph>> {
    let g = Runtime::new().unwrap().block_on(
        raphtory::graph_loader::example::neo4j_examples::neo4j_movie_graph(
            uri, username, password, database, shards,
        ),
    );
    PyGraph::py_from_db_graph(g)
}
