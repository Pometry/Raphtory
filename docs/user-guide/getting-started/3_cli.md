# Command line interface

The Raphtory CLI tool is included in the Python package and allows you to interact directly with the Raphtory server. This is useful for experimentation and scripting.

Access the version of raphtory

Usage:

```sh
raphtory --version
```

## Server

The server subcommand starts the GraphQL server with the specified configuration.

Usage:

```sh
raphtory server --port 1736
```

| Command                     | Parameter(s)              | Description                                                         |
|-----------------------------|---------------------------|---------------------------------------------------------------------|
| -h, --help                  |                           | Show the help message and exit                                      |
| --log-level                 | LOG_LEVEL                 | Log level                                                           |
| --tracing                   |                           | Enable tracing                                                      |
| --otlp-agent-host           | OTLP_AGENT_HOST           | OTLP agent host                                                     |
| --otlp-agent-port           | OTLP_AGENT_PORT           | OTLP agent port                                                     |
| --otlp-tracing-service-name | OTLP_TRACING_SERVICE_NAME | OTLP tracing service name                                           |
| --auth-public-key           | AUTH_PUBLIC_KEY           | Public key for auth                                                 |
| --auth-enabled-for-reads    |                           | Enable auth for reads                                               |
| --config-path               | CONFIG_PATH               | Optional config path                                                |
| --create-index              |                           | Enable index creation                                               |
| --port                      | PORT                      | Port for Raphtory to run on, defaults to 1736                       |
| --timeout                   | TIMEOUT                   | Timeout for starting the server in milliseconds. Defaults: 180000ms |

## Schema

The Schema subcommand prints the current GraphQL schema.

Usage:

```sh
raphtory schema
```
