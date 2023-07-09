from raphtory import internal_graphql
import asyncio

async def __from_map_and_directory(graphs,graph_dir):
    await internal_graphql.from_map_and_directory(graphs,graph_dir)

async def __from_directory(graph_dir):
    await internal_graphql.from_directory(graph_dir)

async def __from_map(graphs):
    await internal_graphql.from_map(graphs)

def run_server(graphs=None,graph_dir=None):
    """
    Runs the Raphtory GraphQL server.

    Args:
        graphs (dict, optional): A dictionary of graphs to load into the server. Default is None.
        graph_dir (str, optional): The directory to load graphs from. Default is None.
    """
    loop = asyncio.get_event_loop()
    if graph_dir is not None and graphs is not None:
        loop.create_task(__from_map_and_directory(graphs,graph_dir))
    elif graph_dir is not None:
        loop.create_task(__from_directory(graph_dir))
    elif graphs is not None:
        loop.create_task(__from_map(graphs))
