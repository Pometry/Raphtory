###############################################################################
#                                                                             #
#                      AUTOGENERATED TYPE STUB FILE                           #
#                                                                             #
#    This file was automatically generated. Do not modify it directly.        #
#    Any changes made here may be lost when the file is regenerated.          #
#                                                                             #
###############################################################################

from typing import *
from raphtory import *
from raphtory.algorithms import *
from raphtory.vectors import *
from raphtory.node_state import *
from raphtory.graphql import *
from raphtory.typing import *
from datetime import datetime
from pandas import DataFrame

class GraphqlGraphs(object):
    """
    A class for accessing graphs hosted in a Raphtory GraphQL server and running global search for
    graph documents
    """

    def get(self, name):
        """Return the `VectorisedGraph` with name `name` or `None` if it doesn't exist"""

    def search_graph_documents(self, query, limit, window):
        """
        Return the top documents with the smallest cosine distance to `query`

        # Arguments
          * query - the text or the embedding to score against
          * limit - the maximum number of documents to return
          * window - the window where documents need to belong to in order to be considered

        # Returns
          A list of documents
        """

    def search_graph_documents_with_scores(self, query, limit, window):
        """Same as `search_graph_documents` but it also returns the scores alongside the documents"""

class GraphServer(object):
    """A class for defining and running a Raphtory GraphQL server"""

    def __new__(
        cls,
        work_dir,
        cache_capacity=None,
        cache_tti_seconds=None,
        log_level=None,
        tracing=None,
        otlp_agent_host=None,
        otlp_agent_port=None,
        otlp_tracing_service_name=None,
        config_path=None,
    ) -> GraphServer:
        """Create and return a new object.  See help(type) for accurate signature."""

    def run(self, port: int = 1736, timeout_ms: int = 180000):
        """
        Run the server until completion.

        Arguments:
          port (int): The port to use. Defaults to 1736.
          timeout_ms (int): Timeout for waiting for the server to start. Defaults to 180000.
        """

    def set_embeddings(
        self,
        cache: str,
        embedding: Optional[Callable] = None,
        graphs: bool | str = ...,
        nodes: bool | str = ...,
        edges: bool | str = ...,
    ) -> GraphServer:
        """
        Setup the server to vectorise graphs with a default template.

        Arguments:
          cache (str):  the directory to use as cache for the embeddings.
          embedding (Callable, optional):  the embedding function to translate documents to embeddings.
          graphs (bool | str): if graphs have to be embedded or not or the custom template to use if a str is provided (defaults to True)
          nodes (bool | str): if nodes have to be embedded or not or the custom template to use if a str is provided (defaults to True)
          edges (bool | str): if edges have to be embedded or not or the custom template to use if a str is provided (defaults to True)

        Returns:
           GraphServer: A new server object with embeddings setup.
        """

    def start(self, port: int = 1736, timeout_ms: int = 5000) -> RunningGraphServer:
        """
        Start the server and return a handle to it.

        Arguments:
          port (int):  the port to use. Defaults to 1736.
          timeout_ms (int): wait for server to be online. Defaults to 5000.
            The server is stopped if not online within timeout_ms but manages to come online as soon as timeout_ms finishes!

        Returns:
          RunningGraphServer: The running server
        """

    def turn_off_index(self):
        """Turn off index for all graphs"""

    def with_global_search_function(
        self, name: str, input: dict, function: Callable
    ) -> GraphServer:
        """
        Register a function in the GraphQL schema for document search among all the graphs.

        The function needs to take a `GraphqlGraphs` object as the first argument followed by a
        pre-defined set of keyword arguments. Supported types are `str`, `int`, and `float`.
        They have to be specified using the `input` parameter as a dict where the keys are the
        names of the parameters and the values are the types, expressed as strings.

        Arguments:
          name (str): the name of the function in the GraphQL schema.
          input (dict):  the keyword arguments expected by the function.
          function (Callable): the function to run.

        Returns:
           GraphServer: A new server object with the function registered
        """

    def with_vectorised_graphs(
        self,
        graph_names: list[str],
        graphs: bool | str = ...,
        nodes: bool | str = ...,
        edges: bool | str = ...,
    ) -> GraphServer:
        """
        Vectorise a subset of the graphs of the server.

        Arguments:
          graph_names (list[str]): the names of the graphs to vectorise. All by default.
          graphs (bool | str): if graphs have to be embedded or not or the custom template to use if a str is provided (defaults to True)
          nodes (bool | str): if nodes have to be embedded or not or the custom template to use if a str is provided (defaults to True)
          edges (bool | str): if edges have to be embedded or not or the custom template to use if a str is provided (defaults to True)

        Returns:
           GraphServer: A new server object containing the vectorised graphs.
        """

class RunningGraphServer(object):
    """A Raphtory server handler that also enables querying the server"""

    def __enter__(self): ...
    def __exit__(self, _exc_type, _exc_val, _exc_tb): ...
    def get_client(self): ...
    def stop(self):
        """Stop the server and wait for it to finish"""

class RaphtoryClient(object):
    """A client for handling GraphQL operations in the context of Raphtory."""

    def __new__(cls, url) -> RaphtoryClient:
        """Create and return a new object.  See help(type) for accurate signature."""

    def copy_graph(self, path, new_path):
        """
        Copy graph from a path `path` on the server to a `new_path` on the server

        Arguments:
          * `path`: the path of the graph to be copied
          * `new_path`: the new path of the copied graph

        Returns:
           Copy status as boolean
        """

    def delete_graph(self, path):
        """
        Delete graph from a path `path` on the server

        Arguments:
          * `path`: the path of the graph to be deleted

        Returns:
           Delete status as boolean
        """

    def is_server_online(self):
        """
        Check if the server is online.

        Returns:
           Returns true if server is online otherwise false.
        """

    def move_graph(self, path, new_path):
        """
        Move graph from a path `path` on the server to a `new_path` on the server

        Arguments:
          * `path`: the path of the graph to be moved
          * `new_path`: the new path of the moved graph

        Returns:
           Move status as boolean
        """

    def new_graph(self, path, graph_type):
        """
        Create a new Graph on the server at `path`

        Arguments:
          * `path`: the path of the graph to be created
          * `graph_type`: the type of graph that should be created - this can be EVENT or PERSISTENT

        Returns:
           None

        """

    def query(self, query, variables=None):
        """
        Make a graphQL query against the server.

        Arguments:
          * `query`: the query to make.
          * `variables`: a dict of variables present on the query and their values.

        Returns:
           The `data` field from the graphQL response.
        """

    def receive_graph(self, path):
        """
        Receive graph from a path `path` on the server

        Arguments:
          * `path`: the path of the graph to be received

        Returns:
           Graph as string
        """

    def remote_graph(self, path):
        """
        Get a RemoteGraph reference to a graph on the server at `path`

        Arguments:
          * `path`: the path of the graph to be created

        Returns:
           RemoteGraph

        """

    def send_graph(self, path, graph, overwrite=False):
        """
        Send a graph to the server

        Arguments:
          * `path`: the path of the graph
          * `graph`: the graph to send
          * `overwrite`: overwrite existing graph (defaults to False)

        Returns:
           The `data` field from the graphQL response after executing the mutation.
        """

    def upload_graph(self, path, file_path, overwrite=False):
        """
        Upload graph file from a path `file_path` on the client

        Arguments:
          * `path`: the name of the graph
          * `file_path`: the path of the graph on the client
          * `overwrite`: overwrite existing graph (defaults to False)

        Returns:
           The `data` field from the graphQL response after executing the mutation.
        """

class RemoteGraph(object):
    def add_constant_properties(self, properties: dict):
        """
        Adds constant properties to the remote graph.

        Arguments:
            properties (dict): The constant properties of the graph.
        """

    def add_edge(
        self,
        timestamp: int | str | datetime,
        src: str | int,
        dst: str | int,
        properties: Optional[dict] = None,
        layer: Optional[str] = None,
    ):
        """
        Adds a new edge with the given source and destination nodes and properties to the remote graph.

        Arguments:
           timestamp (int |str | datetime): The timestamp of the edge.
           src (str | int): The id of the source node.
           dst (str | int): The id of the destination node.
           properties (dict, optional): The properties of the edge, as a dict of string and properties.
           layer (str, optional): The layer of the edge.

        Returns:
          RemoteEdge
        """

    def add_edges(self, updates: List[RemoteEdgeAddition]):
        """
        Batch add edge updates to the remote graph

        Arguments:
          updates (List[RemoteEdgeAddition]): The list of updates you want to apply to the remote graph
        """

    def add_node(
        self,
        timestamp: int | str | datetime,
        id: str | int,
        properties: Optional[dict] = None,
        node_type: Optional[str] = None,
    ):
        """
        Adds a new node with the given id and properties to the remote graph.

        Arguments:
           timestamp (int|str|datetime): The timestamp of the node.
           id (str|int): The id of the node.
           properties (dict, optional): The properties of the node.
           node_type (str, optional): The optional string which will be used as a node type
        Returns:
          RemoteNode
        """

    def add_nodes(self, updates: List[RemoteNodeAddition]):
        """
        Batch add node updates to the remote graph

        Arguments:
          updates (List[RemoteNodeAddition]): The list of updates you want to apply to the remote graph
        """

    def add_property(self, timestamp: int | str | datetime, properties: dict):
        """
        Adds properties to the remote graph.

        Arguments:
           timestamp (int|str|datetime): The timestamp of the temporal property.
           properties (dict): The temporal properties of the graph.
        """

    def create_node(
        self,
        timestamp: int | str | datetime,
        id: str | int,
        properties: Optional[dict] = None,
        node_type: Optional[str] = None,
    ):
        """
        Create a new node with the given id and properties to the remote graph and fail if the node already exists.

        Arguments:
           timestamp (int|str|datetime): The timestamp of the node.
           id (str|int): The id of the node.
           properties (dict, optional): The properties of the node.
           node_type (str, optional): The optional string which will be used as a node type
        Returns:
          RemoteNode
        """

    def delete_edge(
        self,
        timestamp: int,
        src: str | int,
        dst: str | int,
        layer: Optional[str] = None,
    ):
        """
        Deletes an edge in the remote graph, given the timestamp, src and dst nodes and layer (optional)

        Arguments:
          timestamp (int): The timestamp of the edge.
          src (str|int): The id of the source node.
          dst (str|int): The id of the destination node.
          layer (str, optional): The layer of the edge.

        Returns:
          RemoteEdge
        """

    def edge(self, src: str | int, dst: str | int):
        """
        Gets a remote edge with the specified source and destination nodes

        Arguments:
            src (str|int): the source node id
            dst (str|int): the destination node id

        Returns:
            RemoteEdge
        """

    def node(self, id: str | int):
        """
        Gets a remote node with the specified id

        Arguments:
          id (str|int): the node id

        Returns:
          RemoteNode
        """

    def update_constant_properties(self, properties: dict):
        """
        Updates constant properties on the remote graph.

        Arguments:
            properties (dict): The constant properties of the graph.
        """

class RemoteEdge(object):
    def __new__(cls, path, client, src, dst) -> RemoteEdge:
        """Create and return a new object.  See help(type) for accurate signature."""

    def add_constant_properties(
        self, properties: Dict[str, Prop], layer: Optional[str] = None
    ):
        """
        Add constant properties to the edge within the remote graph.
        This function is used to add properties to an edge that remain constant and do not
        change over time. These properties are fundamental attributes of the edge.

        Parameters:
            properties (Dict[str, Prop]): A dictionary of properties to be added to the edge.
            layer (str, optional): The layer you want these properties to be added on to.
        """

    def add_updates(
        self,
        t: int | str | datetime,
        properties: Optional[Dict[str, Prop]] = None,
        layer: Optional[str] = None,
    ):
        """
        Add updates to an edge in the remote graph at a specified time.
        This function allows for the addition of property updates to an edge within the graph. The updates are time-stamped, meaning they are applied at the specified time.

        Parameters:
            t (int | str | datetime): The timestamp at which the updates should be applied.
            properties (Optional[Dict[str, Prop]]): A dictionary of properties to update.
            layer (str, optional): The layer you want the updates to be applied.
        """

    def delete(self, t: int | str | datetime, layer: Optional[str] = None):
        """
        Mark the edge as deleted at the specified time.

        Parameters:
            t (int | str | datetime): The timestamp at which the deletion should be applied.
            layer (str, optional): The layer you want the deletion applied to.
        """

    def update_constant_properties(
        self, properties: Dict[str, Prop], layer: Optional[str] = None
    ):
        """
        Update constant properties of an edge in the remote graph overwriting existing values.
        This function is used to add properties to an edge that remains constant and does not
        change over time. These properties are fundamental attributes of the edge.

        Parameters:
            properties (Dict[str, Prop]): A dictionary of properties to be added to the edge.
            layer (str, optional): The layer you want these properties to be added on to.
        """

class RemoteNode(object):
    def __new__(cls, path, client, id) -> RemoteNode:
        """Create and return a new object.  See help(type) for accurate signature."""

    def add_constant_properties(self, properties: Dict[str, Prop]):
        """
        Add constant properties to a node in the remote graph.
        This function is used to add properties to a node that remain constant and does not
        change over time. These properties are fundamental attributes of the node.

        Parameters:
            properties (Dict[str, Prop]): A dictionary of properties to be added to the node.
        """

    def add_updates(
        self, t: int | str | datetime, properties: Optional[Dict[str, Prop]] = None
    ):
        """
        Add updates to a node in the remote graph at a specified time.
        This function allows for the addition of property updates to a node within the graph. The updates are time-stamped, meaning they are applied at the specified time.

        Parameters:
            t (int | str | datetime): The timestamp at which the updates should be applied.
            properties (Dict[str, Prop], optional): A dictionary of properties to update.
        """

    def set_node_type(self, new_type: str):
        """
        Set the type on the node. This only works if the type has not been previously set, otherwise will
        throw an error

        Parameters:
            new_type (str): The new type to be set
        """

    def update_constant_properties(self, properties: Dict[str, Prop]):
        """
        Update constant properties of a node in the remote graph overwriting existing values.
        This function is used to add properties to a node that remain constant and do not
        change over time. These properties are fundamental attributes of the node.

        Parameters:
            properties (Dict[str, Prop]): A dictionary of properties to be added to the node.
        """

class RemoteNodeAddition(object):
    def __new__(
        cls, name, node_type=None, constant_properties=None, updates=None
    ) -> RemoteNodeAddition:
        """Create and return a new object.  See help(type) for accurate signature."""

class RemoteUpdate(object):
    def __new__(cls, time, properties=None) -> RemoteUpdate:
        """Create and return a new object.  See help(type) for accurate signature."""

class RemoteEdgeAddition(object):
    def __new__(
        cls, src, dst, layer=None, constant_properties=None, updates=None
    ) -> RemoteEdgeAddition:
        """Create and return a new object.  See help(type) for accurate signature."""

def encode_graph(graph): ...
def decode_graph(graph): ...
