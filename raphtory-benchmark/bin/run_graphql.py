from raphtory.graphql import GraphServer
import logging

logging.basicConfig(level=logging.INFO)


def load_gql_server():
    logging.info("Loading GQL server")
    GraphServer(graph_dir="./graphs/").run()


load_gql_server()
