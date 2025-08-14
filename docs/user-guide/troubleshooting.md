# Troubleshooting

This page covers common errors and misconfigurations in Raphtory.

## Specifying time measurements

Internally all times in Raphtory are represented as milliseconds using unix epochs. When ingesting data you will need to convert your raw data into the appropriate format. Similarly queries mad using the API should use timestamps relative to the unix epoch in milliseconds.

## Default graph storage

When saving a graph to disk the default location is `/home/raphtory_server`. This is where the Raphtory server will look for graphs unless you specify an alternative working directory. When saving a file or sending a graph to the server you can always specify a custom path.