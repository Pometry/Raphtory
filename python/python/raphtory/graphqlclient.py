from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import raphtory
from raphtory import internal_graphql


class RaphtoryGraphQLClient:
    """
    A client for handling GraphQL operations in the context of Raphtory.
    """

    def __init__(self, url: str):
        """
        Initialize a GraphQL Client Connection.

        Args:
            url (str): URL to a server with the port appended to the URL.

        Note:
            This constructor creates a GraphQL client connection to the given URL.
        """
        transport = RequestsHTTPTransport(url=url, use_json=True)
        self.client = Client(transport=transport, fetch_schema_from_transport=True)
        # Below attempts to connect to the server with the url
        # self.client.connect_sync()

    def query(self, query: str, variables: dict = {}):
        """
        Execute a GraphQL query.

        Args:
            query (str): The GraphQL query string.
            variables (dict, optional): Variables for the query. Defaults to an empty dictionary.

        Returns:
            dict: Result of the query.
        """
        query = gql(query)
        return self.client.execute(query, variables)

    def load_graphs_from_path(self, path: str) -> dict:
        """
        Load graphs from a directory of bincode files.

        Args:
            path (str): Directory containing bincode files.

        Returns:
            dict: Result after executing the mutation.

        Note:
            Existing graphs with the same name are overwritten.
        """
        mutation_q = gql(
            """
                mutation LoadGraphsFromPath($path: String!) {
                    loadGraphsFromPath(path: $path)
                }
        """
        )
        result = self.client.execute(mutation_q, variable_values={"path": path})
        if len(result["loadGraphsFromPath"]):
            print("Loaded %i graph(s)" % len(result["loadGraphsFromPath"]))
            return result
        else:
            print("Could not find a graph to load")
            return result

    def load_new_graphs_from_path(self, path: str) -> dict:
        """
        Load new graphs from a directory of bincode files.

        Args:
            path (str): Directory containing bincode files.

        Returns:
            dict: Result after executing the mutation.

        Note:
            Existing graphs will not be overwritten.
        """
        mutation_q = gql(
            """
                mutation LoadNewGraphsFromPath($path: String!) {
                    loadNewGraphsFromPath(path: $path)
                }
        """
        )
        result = self.client.execute(mutation_q, variable_values={"path": path})

        if len(result["loadNewGraphsFromPath"]):
            print("Loaded %i graph(s)" % len(result["loadNewGraphsFromPath"]))
            return result
        else:
            print("Could not find a graph to load")
            return result

    def send_graph(self, name: str, graph: raphtory.Graph):
        """
        Upload a graph to the GraphQL Server.

        Args:
            name (str): Name of the graph.
            graph (raphtory.Graph): Graph object to be uploaded.

        Returns:
            dict: Result after executing the mutation.

        Raises:
            Exception: If there's an error sending the graph.
        """
        encoded_graph = internal_graphql.encode_graph(graph)

        mutation_q = gql(
            """
                mutation SendGraph($name: String!, $graph: String!) {
                    sendGraph(name: $name, graph: $graph)
                }
        """
        )
        result = self.client.execute(
            mutation_q, variable_values={"name": name, "graph": encoded_graph}
        )
        if "sendGraph" in result:
            print("Sent graph %s to GraphlQL Server" % len(result["sendGraph"]))
            return result
        else:
            raise Exception("Error Sending Graph %s" % result)
