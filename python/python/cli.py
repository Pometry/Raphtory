import argparse
from pathlib import Path

import raphtory
from raphtory.graphql import GraphServer, schema


def run_server(args):
    server = GraphServer(
        work_dir=args.work_dir,
        cache_capacity=args.cache_capacity,
        cache_tti_seconds=args.cache_tti_seconds,
        log_level=args.log_level,
        tracing=args.tracing,
        otlp_agent_host=args.otlp_agent_host,
        otlp_agent_port=args.otlp_agent_port,
        otlp_tracing_service_name=args.otlp_tracing_service_name,
        auth_public_key=args.auth_public_key,
        auth_enabled_for_reads=args.auth_enabled_for_reads,
        config_path=args.config_path,
        create_index=args.create_index,
    )
    server.run(port=args.port, timeout_ms=args.timeout)


def print_schema(_args):
    print(schema())


def main():
    parser = argparse.ArgumentParser(prog="raphtory")
    parser.add_argument("--version", action="version", version=f"{raphtory.version()}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: server
    server_parser = subparsers.add_parser("server", help="Run the GraphQL server")

    server_parser.add_argument(
        "--work-dir", type=Path, default=".", help="Working directory"
    )
    server_parser.add_argument("--cache-capacity", type=int, help="Cache capacity")
    server_parser.add_argument(
        "--cache-tti-seconds", type=int, help="Cache time-to-idle in seconds"
    )
    server_parser.add_argument("--log-level", type=str, help="Log level")
    server_parser.add_argument("--tracing", action="store_true", help="Enable tracing")
    server_parser.add_argument("--otlp-agent-host", type=str, help="OTLP agent host")
    server_parser.add_argument("--otlp-agent-port", type=str, help="OTLP agent port")
    server_parser.add_argument(
        "--otlp-tracing-service-name", type=str, help="OTLP tracing service name"
    )
    server_parser.add_argument(
        "--auth-public-key", type=str, help="Public key for auth"
    )
    server_parser.add_argument(
        "--auth-enabled-for-reads", action="store_true", help="Enable auth for reads"
    )
    server_parser.add_argument("--config-path", type=Path, help="Optional config path")
    server_parser.add_argument(
        "--create-index", action="store_true", help="Enable index creation"
    )

    server_parser.add_argument(
        "--port",
        type=int,
        default=1736,
        help="Port for Raphtory to run on, defaults to 1736",
    )
    server_parser.add_argument(
        "--timeout",
        type=int,
        default=180000,
        help="Timeout for starting the server in milliseconds, defaults to 180000ms",
    )

    server_parser.set_defaults(func=run_server)

    schema_parser = subparsers.add_parser("schema", help="Print the GraphQL schema")
    schema_parser.set_defaults(func=print_schema)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
