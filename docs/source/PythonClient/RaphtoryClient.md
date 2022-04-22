# Raphtory Client

<p style="margin-left: 1.5em;"> This is the class to create a raphtory client which interacts with pulsar. 
The main purpose is to pull data to allow for analysis in python.  </p>

This library can be installed via `pip install raphtory-client` 

The source code can be found `https://github.com/Raphtory/Raphtory/` 

## RaphtoryClient Objects

```python
class raphtoryclient()
    def __init__(self, pulsar_admin_url="http://127.0.0.1:8080", pulsar_client_args=None, raphtory_deployment_id=None):
```

This is the class to create a raphtory client which interacts with pulsar.

**Attributes**:

- `admin_url` _str_: the url for the pulsar admin client
- `pulsar_client_args`: Dict of arguments to be used in the pulsar client, keys must match pulsar.Client parameters
- `raphtory_deployment_id` _string_ : deployment id of the running raphtory instance

<a id="RaphtoryClient.RaphtoryClient.make_name"></a>

### make\_name

```python
def make_name()
```

Helper function which generates a random
subscription suffix for the reader.

**Arguments**:

  none
  

**Returns**:

- `str` - subscription suffix

<a id="RaphtoryClient.RaphtoryClient.setupClient"></a>

### setupClient

```python
def setupClient(client_args, max_attempts=5)
```

Setups a pulsar client using the pulsar address.
Retries at least 5 times before returning

**Attributes**:

- `client_args` _dict_: Dict of arguments to be used in the pulsar client, keys must match pulsar.Client parameters
- `max_attempts` _int_ : Number of attempts to retry



**Returns**:

- `PulsarClient` - A pulsar client object if successful
- `None` _None_ - None if not successful

<a id="RaphtoryClient.RaphtoryClient.createReaders"></a>

### createReader

```python
def createReaders(topic, subscription_name='', schema=schema.StringSchema())
```

Setups a single pulsar reader, which reads from a pulsar topic.
Retries at least 5 times before returning

**Arguments**:

- `topic` _str_ : Names of topic to read from
- `subscription_name` _str_ : Name for this readers subscription
- `schema` _Pulsar.Schema_ : Schema to use for reader
  

**Returns**:

- `PulsarReader` _Pulsar.reader_ : A pulsar reader object
- `None` _None_ - None if not successful

<a id="RaphtoryClient.RaphtoryClient.getStats"></a>

### getStats

```python
def getStats(topic, tenant="public", namespace="default")
```

Reads stats from a pulsar topic using the admin interface.
If success returns the response as json else returns an empty dict.

**Arguments**:

- `topic` _str_ - Topic to obtain stats from
- `tenant` _str_ - (Optional, default: public) Pulsar tenant to access
- `namepsace` _str_ - (Optional, default: default) Pulsar namespace to access
  

**Returns**:

  json response (dict/json): The response of the request. If unsuccessful then returns an empty dict.

<a id="RaphtoryClient.RaphtoryClient.getResults"></a>

### getDataframe

```python
def getResults(reader, delimiter=',', max_messages=sys.maxsize, col_names=[])
```

Using the reader, reads a complete topic and converts
it into a pandas Dataframe. This will split each message
from the reader using the class delimiter.
By default this expects the results to have three columns
called timestamp, window and id. Any columns after this
are called result_N.

**Arguments**:

- `reader` _Pulsar.Reader_ : Reader where messages will be pulled from
-  `delimiter` _str_ : the delimiter for parsing the results
- `max_messages` _int_ : (Optional, default:sys.maxsize) The number of messages to return.
  By default, it returns the entire topic. This may cause memory
  issues.
- `col_names` _list[string]_ : (Optional: default: ["timestamp", "window", "id"]). These are
  the names of the columns. By default this expects the results
  to have three columns called timestamp, window and id. Any
  columns after this are called result_N.
  

**Returns**:

- `dataframe` _pandas.dataframe_ - A dataframe of the topic

<a id="RaphtoryClient.RaphtoryClient.find_dates"></a>

### find\_dates

```python
def find_dates(all_data, node_a_id=0, node_b_id=1, time_col=2)
```

Given a dataframe of edges, this will find the first time an item was seen.
This is returned as a dict with the key being the id and the value time.
This can be helpful when trying to identify when a node was first created.

**Arguments**:

- `all_data` _dataframe_ - A dataframe containing a column with keys and a column with times/numbers to compare with.
- `node_a_id` _int_ - Position of the id or name to use as key for node A
- `node_b_id` _int_ - Position of the id or name to use as key for node B
- `time_col` _int_ - Position of the time column which is compared
  

**Returns**:

- `first_seen` _dict_ - A dictionary with the key= node_id and the value = time

<a id="RaphtoryClient.RaphtoryClient.add_node_attributes"></a>

### add\_node\_attributes

```python
def add_node_attributes(G, results, abbr, row_id=2, time_col=0, window_col=-1, result_col=3)
```

Given a graph, an array of attributes and a abbreviation.
This will add all the attributes to the graph.
For example, given a graph G, results

**Arguments**:

- `G` _networkx.graph_ - A networkx graph
- `results` _list[dict]_ - A list of dataframes which contain attributes.
  The format for attributes is a dataframe with
      - `id` - node id
      - `timestamp` - time the attribute was created
      - `window` - (optional) the window the attribute was created
      - `result_col` - the value of the attribute
- `abbr`   _list(str)_ : A list of strings which correspond to the abbreviation used when appending the attribute.
- `row_id`  _int/str_: Column position which contains ID / Name of the row id column to use, must be the same across results
- `time_col` _int/str_: Column position which contains the timestamp / Name of the timestamp column to use
- `result_col` _int/str_: Column position which contains result / Name of the result column
- `window_col` _int/str_: (Optional, default:'window') Column position which contains window / Name of the window column, set to '' if not being used

<a id="RaphtoryClient.RaphtoryClient.createGraph"></a>

### createGraphFromEdgeList

```python
def createGraphFromEdgeList(df, isMultiGraph=True)
```

Builds a simple networkx graph from an edge list in Raphtory.

**Arguments**:

- `df` _pandas.Dataframe_: A dataframe of an edgelist where, col 0: timestamp, col 1: source, col 2: destination
- `isMultiGraph` _bool_ - If False will use DiGraph, otherwise will use MultiGraph

**Returns**:

- `G` _networkx.DiGraph_ - The graph as built in networkx

<a id="RaphtoryClient.RaphtoryClient.createLOTRGraph"></a>

### createLOTRGraph

```python
def createLOTRGraph(filePath, from_time=0, to_time=sys.maxsize, source_id=0, target_id=1, timestamp_col=2)
```

Example graph builder in python. Given a csv edgelist this will create a graph using networkx based on the lotr data.

**Arguments**:

- `filePath` _str_ - Location of csv file
- `from_time` _int_ - (Optional, default: 0) timestamp to start building graph from
- `to_time` _int_ - (Optional, default: sys.maxsize) timestamp to stop building graph
- `source_id` _int_ - column for source node
- `target_id` _int_ - column for target node
- `timestamp_col` _int_ - column for lotr timestamp
  

**Returns**:

- `G` _networkx.DiGraph_ - The graph as built in networkx

### setupRaphtory
```python
def setupRaphtory(self, deployment_id):
```

Setups a raphtory java client via the gateway object.
This allows the user to invoke raphtory java/scalaa methods as if they were running on the
raphtory/scala version.
Note that arguements must be correct or the methods will not be called.

**Arguments**:

- `deployment_id` _string_: the deployment id of the raphtory instance to connect to

**Returns**:

- `client`: raphtory client java object

### setupJavaGateway
```python
def setupJavaGateway(self):
```

Creates Java<>Raphtory gateway and imports required files.
The gateway allows the user to run/read java/scala methods and objects from python.

**Returns**:

- `py4j.java_gateway`: py4j java gateway object

