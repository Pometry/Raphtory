# Troubleshooting

This page covers common errors and misconfigurations in Raphtory.

## Specifying time measurements

Internally all times in Raphtory are represented as milliseconds using unix epochs. When ingesting data you will need to convert your raw data into the appropriate format. Similarly queries mad using the API should use timestamps relative to the unix epoch in milliseconds.
