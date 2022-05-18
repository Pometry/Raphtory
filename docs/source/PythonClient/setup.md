# Python Client Setup 


In order to run the example notebook you must first:

* Install and run Apache Pulsar
* Get the LOTR example running from [raphtory-example-lotr](https://github.com/Raphtory/Raphtory/tree/master/examples/raphtory-example-lotr)
* Set up a python environment

To run Raphtory and Pulsar, please view the prior Raphtory setup/installation guide.

##  Data for the Python Demo

Once you have Pulsar running and Raphtory setup, we can now run the [raphtory-example-lotr](https://github.com/Raphtory/Raphtory/tree/master/examples/raphtory-example-lotr) Runner.
This will run a `spout` and `graphbuilder` that ingests and creates a LOTR graph.
Then will run the `EdgeList` and `PageRank` algorithms. The `EdgeList` algorithm will produce an edge list that can be ingested into the graph. `PageRank` will run as a range query over specific times in the data. More information can be found in the projects readme.


## Setup Python Environment

- Install Python3 and Pip
- Either
    - Install the requirements file via
        - `pip install -r requirements.txt`
        - This will include Jupyter if you do not have it
    - `pip install raphtory-client`  and `pip install jupyter`
- install the addons for pymotif
```
  # Jupyter Lab
  jupyter labextension install @jupyter-widgets/jupyterlab-manager

  # For Jupyter Lab <= 2, you may need to install the extension manually
  jupyter labextension install @cylynx/pymotif

  # For Jupyter Notebook <= 5.2, you may need to enable nbextensions
  jupyter nbextension enable --py [--sys-prefix|--user|--system] pymotif
```
- then run jupyter via `jupyter notebook`