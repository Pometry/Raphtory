from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import raphtory

class RaphtoryGraphQLClient:
    def __init__(self, url: str):
        transport = RequestsHTTPTransport(url=url, use_json=True)
        self.client = Client(transport=transport, fetch_schema_from_transport=True)
        # Below attempts to connect to the server with the url
        # self.client.connect_sync()

    def query(self, query: str, variables: dict = {}):
        query = gql(query)
        return self.client.execute(query, variables)


    def load_graphs_from_path(self, path: str) -> dict:
        """
        Load graphs from a directory of bincode files (existing graphs with the same name are overwritten)
        """
        mutation_q = gql("""
                mutation LoadGraphsFromPath($path: String!) {
                    loadGraphsFromPath(path: $path)
                }
        """)
        result = self.client.execute(mutation_q, variable_values={ "path": path})
        if len(result['loadGraphsFromPath']):
            print("Loaded %i graph(s)" % len(result['loadGraphsFromPath']))
            return result
        else:
            print("Could not find a graph to load")
            return result
    

    def load_new_graphs_from_path(self, path: str) -> dict:
        """
        Load new graphs from a directory of bincode files (existing graphs will not been overwritten)
        """
        mutation_q = gql("""
                mutation LoadNewGraphsFromPath($path: String!) {
                    loadNewGraphsFromPath(path: $path)
                }
        """)
        result = self.client.execute(mutation_q, variable_values={ "path": path})

        if len(result['loadNewGraphsFromPath']):
            print("Loaded %i graph(s)" % len(result['loadNewGraphsFromPath']))
            return result
        else:
            print("Could not find a graph to load")
            return result
        

    def upload_graph(self, name: str, graph: raphtory.Graph): 
        """
        Use GQL multipart upload to send new graphs to server (Graphs should be of type MaterializedGraph)
        """
        material_graph = graph.materialize()

        mutation_q = gql("""
                mutation UploadGraph($name: String!, $graph: Upload!) {
                    uploadGraph(name: $name, graph: $graph) {
                        nodes {
                            id
                        }
                    }
                }
        """)
        result = self.client.execute(mutation_q, variable_values={ "name": name, "graph":  material_graph})

if __name__ == "__main__":
    # Example usage
    raphtory_client = RaphtoryGraphQLClient(url="http://localhost:1736/")
    gg = raphtory.Graph()
    gg.add_vertex(0, "hi")
    print(raphtory_client.upload_graph("tt", gg))
