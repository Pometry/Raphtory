from raphtory.graphql import RaphtoryServer
import logging

logging.basicConfig(level=logging.INFO)


def load_gql_server():
    logging.info("Loading GQL server")
    RaphtoryServer(graph_dir="./graphs/").run()


load_gql_server()