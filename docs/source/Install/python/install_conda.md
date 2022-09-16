# Python (With Conda)

## Pre-requisites

- Conda, Install from [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html#regular-installation)

## Guide 

An easy way to install pyraphtory is via conda.

1. Create a conda environment

```bash
conda create --name raphtoryenv python=3.9.13 -c conda-forge
```

2. Activate the environment

```bash
conda activate raphtoryenv
```

3. Install java and jupyter

```bash
conda install -y openjdk==11.0.15 jupyterlab -c conda-forge
 ```

4. In a seperate terminal window, download and run Apache Pulsar

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

5. Install dependencies and pyraphtory
```bash
pip install pandas pemja cloudpickle
pip install -i https://test.pypi.org/simple/ pyraphtory==0.2.0a1
```
