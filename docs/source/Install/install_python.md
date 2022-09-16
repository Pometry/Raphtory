# Pyraphtory

Pyraphtory is the python client for Raphtory. 

It can be installed in two ways, with and without conda

## Install via Conda

An easy way to install pyraphtory is via conda.

1. First install conda from [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html#regular-installation)

2. Create a conda environment
   
```bash
conda create --name raphtoryenv python=3.9.13 -c conda-forge
```

3. Activate the environment

```bash
conda activate raphtoryenv
```
   
4. Install java and jupyter

```bash
conda install -y openjdk==11.0.15 jupyterlab -c conda-forge
 ```

5. In a seperate terminal window, download and run Apache Pulsar 

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


7. Install dependencies and pyraphtory

```bash
pip install pandas pemja cloudpickle
pip install -i https://test.pypi.org/simple/ pyraphtory==0.2.0a1
```


##  Data for the Python Demo

Once you have Pulsar running and Raphtory setup, we can now run the [raphtory-example-lotr](https://github.com/Raphtory/Raphtory/tree/master/examples/raphtory-example-lotr) Runner.
This will run a `spout` and `graphbuilder` that ingests and creates a LOTR graph.
Then will run the `EdgeList` and `PageRank` algorithms. The `EdgeList` algorithm will produce an edge list that can be ingested into the graph. `PageRank` will run as a range query over specific times in the data. More information can be found in the projects readme.

**You must set the environment variable `RAPHTORY_PYTHON_ACTIVE` to `true` to ensure Raphtory launches
the python gateway server, it is set to `false` by default.** 
