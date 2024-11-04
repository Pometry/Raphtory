from raphtory import graphql
from dotenv import load_dotenv

# Load the .env file
load_dotenv()
import argparse

parser = argparse.ArgumentParser(description="For passing the working_dir")
parser.add_argument(
    "--working_dir",
    type=str,
    default="graphs",
    help="path for the working directory of the raphtory server, defaults to 'graphs/'",
)
parser.add_argument(
    "--log_level",
    type=str,
    default="info",
    help="log level for the server, defaults to info",
)
parser.add_argument(
    "--tracing",
    type=bool,
    default=False,
    help="If tracing should be enabled or not, defaults to False",
)
parser.add_argument(
    "--otlp_agent_host",
    type=str,
    default="localhost",
    help="The address of the open telemetry collector, defaults to localhost",
)
parser.add_argument(
    "--otlp_agent_port",
    type=str,
    default="4317",
    help="The port of the open telemetry collector, default to 4317",
)
parser.add_argument(
    "--otlp_tracing_service_name",
    type=str,
    default="Raphtory",
    help="The name this service will be known by for open telemetry, default to Raphtory",
)
parser.add_argument(
    "--cache_capacity",
    type=int,
    default=30,
    help="The maximum amount of graphs to keep in memory at any given time, defaults to 30",
)
parser.add_argument(
    "--cache_tti_seconds",
    type=int,
    default=900,
    help="The amount of time a graph will be kept in memory before being dropped, defaults to 900 seconds",
)
args = parser.parse_args()

server = graphql.GraphServer(args.working_dir,tracing=args.tracing,log_level=args.log_level,otlp_agent_host=args.otlp_agent_host,otlp_agent_port=args.otlp_agent_port,
                             otlp_tracing_service_name=args.otlp_tracing_service_name,cache_capacity=args.cache_capacity,cache_tti_seconds=args.cache_tti_seconds)
server.run()
