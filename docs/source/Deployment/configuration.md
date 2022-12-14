# Configuring Raphtory Environment Variables

## Raphtory Logging

When deploying Raphtory, there are several logging configuration options available. These options can be set as environment variables and will be used by Raphtory when starting.

See the below tables for the available loggers and the corresponding environment variables and default values.

### Default Log Settings

| Logger name | Environment Variable | Default Value |
| ----------- | -------------------- | ----- |
|defaultLogLevel|RAPHTORY_CORE_LOG|INFO|
|defaultInternalsLogLevel|RAPHTORY_INTERNALS_LOG|${defaultLogLevel}|
|defaultInternalsComponentsLogLevel|RAPHTORY_INTERNALS_COMPONENTS_LOG|${defaultInternalsLogLevel}|
|defaultPythonLogLevel|RAPHTORY_PYTHON_LOG|${defaultLogLevel}|

### Logger Configuration

| Logger name | Environment Variable | Default Value |
| ----------- | -------------------- | ----- |
|logFileName|RAPHTORY_LOGFILE|logs/app.log|
|logFileAppend|RAPHTORY_APPEND_LOGFILE|false|
|logger|RAPHTORY_LOGGER|Async|

### Raphtory Logging

| Logger name | Environment Variable | Default Value |
| ----------- | -------------------- | ----- |
|com.raphtory||${defaultLogLevel}|
|com.raphtory.algorithms|RAPHTORY_ALGORITHMS_LOG|${defaultLogLevel}|
|com.raphtory.api|RAPHTORY_API_LOG|${defaultLogLevel}|
|com.raphtory.formats|RAPHTORY_FORMATS_LOG|${defaultLogLevel}|
|com.raphtory.sinks|RAPHTORY_SINKS_LOG|${defaultLogLevel}|
|com.raphtory.spouts|RAPHTORY_SPOUTS_LOG|${defaultLogLevel}|
|com.raphtory.utils|RAPHTORY_UTIL_LOG|${defaultLogLevel}|
|com.raphtory.internals||${defaultInternalsLogLevel}|
|com.raphtory.internals.communication|RAPHTORY_INTERNALS_COMMUNICATION_LOG|${defaultInternalsLogLevel}|
|com.raphtory.internals.graph|RAPHTORY_INTERNALS_GRAPH_LOG|${defaultInternalsLogLevel}|
|com.raphtory.internals.management|RAPHTORY_INTERNALS_MANAGEMENT_LOG|${defaultInternalsLogLevel}|
|com.raphtory.internals.serialisers|RAPHTORY_INTERNALS_SERIALISERS_LOG|${defaultInternalsLogLevel}|
|com.raphtory.internals.storage|RAPHTORY_INTERNALS_STORAGE_LOG|${defaultInternalsLogLevel}|
|com.raphtory.internals.time|RAPHTORY_INTERNALS_TIME_LOG|${defaultInternalsLogLevel}|
|com.raphtory.internals.components||${defaultInternalsComponentsLogLevel}|
|com.raphtory.internals.components.partition|RAPHTORY_INTERNALS_COMPONENTS_PARTITION_LOG|${defaultInternalsComponentsLogLevel}|
|com.raphtory.internals.components.querymanager|RAPHTORY_INTERNALS_COMPONENTS_QUERYMANAGER_LOG|${defaultInternalsComponentsLogLevel}|


## Raphtory Server Configuration

When deploying Raphtory, there are several configuration options available. These options can be set as environment variables and will be used by Raphtory when starting.

See the below table for the available variables and default values.

| Environment Variable | Description | Default Value |
| -------------------- | ----------- | ------------- |
|RAPHTORY_ZOOKEEPER_ADDRESS|Address for zookeeper|127.0.0.1:2181|
|RAPHTORY_BUILDERS_FAIL_ON_ERROR|Toggle failure on error for builder|true|
|RAPHTORY_PARTITIONS_SERVERCOUNT|Count of partition servers|1|
|RAPHTORY_DEPLOY_ADDRESS|Address to start raphtory on|127.0.0.1|
|RAPHTORY_DEPLOY_PORT|Port to start raphtory on|1736|
