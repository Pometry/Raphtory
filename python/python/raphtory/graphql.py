from raphtory import internal_graphql
import asyncio
import threading
import requests
import time

class GraphQLServer:
    def __init__(self, port):
        self.port = port

    def query(self, query):
        r = requests.post("http://localhost:"+str(self.port), json={"query": query})
        if r.status_code == 200:
            return r.json()
        else:
            raise Exception(f"Query failed to run with a {r.status_code}.")

async def __from_map_and_directory(graphs,graph_dir,port):
    await internal_graphql.from_map_and_directory(graphs,graph_dir,port)

async def __from_directory(graph_dir,port):
    await internal_graphql.from_directory(graph_dir,port)

async def __from_map(graphs,port):
    await internal_graphql.from_map(graphs,port)


def __run(func,daemon,port):
    if daemon:
        def __run_in_background():
            asyncio.run(func)
        threading.Thread(target=__run_in_background, daemon=True).start()
        time.sleep(5) #TODO this is obviously a hack
        return GraphQLServer(port)
    else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(func)
        loop.close()

def run_server(graphs=None,graph_dir=None,port=1736,daemon=False):
    """
    Runs the Raphtory GraphQL server.

    Args:
        graphs (dict, optional): A dictionary of graphs to load into the server. Default is None.
        graph_dir (str, optional): The directory to load graphs from. Default is None.
        port (int, optional): The port to run the server on. Default is 1736.
        daemon (bool, optional): Whether to run the server in the background. Default is False.

    Returns:
        GraphQLServer: A GraphQLServer object that can be used to query the server. (Only if daemon is True)
    """

    if graph_dir is not None and graphs is not None:
        return __run(__from_map_and_directory(graphs, graph_dir, port), daemon, port)
    elif graph_dir is not None:
        return __run(__from_directory(graph_dir, port), daemon, port)
    elif graphs is not None:
        return __run(__from_map(graphs, port), daemon, port)
    else:
        print("No graphs or graph directory specified. Exiting.")
