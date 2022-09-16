# PyRaphtory - Python (Without Conda)

## Pre-requisites

- Java 11
- Python 3.9.13

## Guide

1. Download and run apache pulsar in a seperate terminal

````{tabs}

```{code-tab} bash
conda activate raphtoryenv
wget "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-2.10.1/apache-pulsar-2.10.1-bin.tar.gz" -O pulsar.tar.gz
tar -xf pulsar.tar.gz
./apache-pulsar-2.10.1/bin/pulsar standalone
```

```{tab} Docker
```bash
mkdir pulsardata
docker run -it -p 6650:6650  -p 8080:8080 --mount source=pulsardata,target=/pulsar/data --mount source=pulsarconf,target=/pulsar/conf apachepulsar/pulsar:2.9.1 bin/pulsar standalone
```
```
````

2. Install pyraphtory and dependencies

```bash
pip install pandas pemja cloudpickle
pip install -i https://test.pypi.org/simple/ pyraphtory==0.2.0a1
```
