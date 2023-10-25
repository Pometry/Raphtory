//! `GraphLoader` provides some default implementations for loading a pre-built graph.
//! This base class is used to load in-built graphs such as the LOTR, reddit and StackOverflow.
use crate::python::graph::graph::PyGraph;
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
pub fn lotr_graph() -> PyResult<Py<PyGraph>> {
    PyGraph::py_from_db_graph(crate::graph_loader::example::lotr_graph::lotr_graph())
}

/// Load (a subset of) Reddit hyperlinks dataset into a graph.
/// The dataset is available at http://snap.stanford.edu/data/soc-redditHyperlinks-title.tsv
/// The hyperlink network represents the directed connections between two subreddits (a subreddit
/// is a community_detection on Reddit). We also provide subreddit embeddings. The network is extracted
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
#[pyo3(signature = (timeout_seconds=600))]
pub fn reddit_hyperlink_graph(timeout_seconds: u64) -> PyResult<Py<PyGraph>> {
    PyGraph::py_from_db_graph(
        crate::graph_loader::example::reddit_hyperlinks::reddit_graph(timeout_seconds, false),
    )
}

#[pyfunction]
#[pyo3(signature = (path=None,subset=None))]
pub fn stable_coin_graph(path: Option<String>, subset: Option<bool>) -> PyResult<Py<PyGraph>> {
    PyGraph::py_from_db_graph(
        crate::graph_loader::example::stable_coins::stable_coin_graph(
            path,
            subset.unwrap_or(false),
        ),
    )
}

#[pyfunction]
#[pyo3(signature = (uri,username,password,database="neo4j".to_string()))]
pub fn neo4j_movie_graph(
    uri: String,
    username: String,
    password: String,
    database: String,
) -> PyResult<Py<PyGraph>> {
    let g = Runtime::new().unwrap().block_on(
        crate::graph_loader::example::neo4j_examples::neo4j_movie_graph(
            uri, username, password, database,
        ),
    );
    PyGraph::py_from_db_graph(g)
}

/// `karate_club_graph` constructs a karate club graph.
///
/// This function uses the Zachary's karate club dataset to create
/// a graph object. Vertices represent members of the club, and edges
/// represent relationships between them. Vertex properties indicate
/// the club to which each member belongs.
///
/// Background:
///     These are data collected from the members of a university karate club by Wayne
///     Zachary. The ZACHE matrix represents the presence or absence of ties among the members of the
///     club; the ZACHC matrix indicates the relative strength of the associations (number of
///     situations in and outside the club in which interactions occurred).
///     Zachary (1977) used these data and an information flow model of network conflict resolution
///     to explain the split-up of this group following disputes among the members.
///
/// Reference:
///   Zachary W. (1977). An information flow model for conflict and fission in small groups. Journal of Anthropological Research, 33, 452-473.
///
/// Returns:
///     A `Graph` object representing the karate club network.
#[pyfunction]
#[pyo3(signature = ())]
pub fn karate_club_graph() -> PyResult<Py<PyGraph>> {
    PyGraph::py_from_db_graph(crate::graph_loader::example::karate_club::karate_club_graph())
}
