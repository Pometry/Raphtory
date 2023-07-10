from raphtory import internal_graphql
import asyncio

async def __from_map_and_directory(graphs,graph_dir,port):
    await internal_graphql.from_map_and_directory(graphs,graph_dir,port)

async def __from_directory(graph_dir,port):
    await internal_graphql.from_directory(graph_dir,port)

async def __from_map(graphs,port):
    await internal_graphql.from_map(graphs,port)

def run_server(graphs=None,graph_dir=None,port=1736):
    """
    Runs the Raphtory GraphQL server.

    Args:
        graphs (dict, optional): A dictionary of graphs to load into the server. Default is None.
        graph_dir (str, optional): The directory to load graphs from. Default is None.
    """
    loop = asyncio.get_event_loop()
    if graph_dir is not None and graphs is not None:
        loop.run_until_complete(__from_map_and_directory(graphs,graph_dir,port))
        loop.close()
    elif graph_dir is not None:
        loop.run_until_complete(__from_directory(graph_dir,port))
        loop.close()
    elif graphs is not None:
        loop.run_until_complete(__from_map(graphs,port))
        loop.close()
