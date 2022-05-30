#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
The Raphtory Python client library to interact with Raphtory from Python.

Currently, the supported Python versions are 3.

## Install from PyPI

    #!shell
    $ pip install raphtory-client

## Examples

Example jupyter notebooks can be found at https://www.github.com/raphtory/raphtory/examples

"""

import pulsar
from pulsar.schema import schema
import random
import string
import requests
import time
from os.path import exists
import csv
import networkx as nx
import sys
import pandas as pd
from datetime import datetime, timezone
from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.java_gateway import java_import as py4j_java_import
from py4j.java_collections import MapConverter
from .serializers import UTF8Deserializer, read_int

class client:
    '''
    This is the class to create a raphtory client which interacts with raphtory and pulsar.
    '''

    def __init__(self, pulsar_admin_url="http://127.0.0.1:8080", _pulsar_client_args=None, raphtory_deployment_id=None, conn_file_info=None):
        '''
        Parameters:
        admin_url: the url for the pulsar admin client
        pulsar_client_args: Dict of arguments to be used in the pulsar client, keys must match pulsar.Client parameters
        raphtory_deployment_id: deployment id of the running raphtory instance
        conn_file_info _string_: absolute file path of java gateway connection file, usually '/tmp/raphtory_python_gateway_'+deployment_id
        '''
        if _pulsar_client_args is None:
            _pulsar_client_args = {'service_url': 'pulsar://127.0.0.1:6650'}
        elif 'service_url' not in _pulsar_client_args:
            print('service_url not given in client_args, exiting..')
            sys.exit(1)
        self._pulsar_client_args = _pulsar_client_args
        self._pulsar_client = self.setupPulsarClient(client_args=self._pulsar_client_args)
        if self._pulsar_client == None:
            print('Could not setup client. Exiting...')
            sys.exit(1)
        self._admin_url = pulsar_admin_url
        self._first_seen = ""
        if raphtory_deployment_id is not None:
            self._raphtory_deployment_id = raphtory_deployment_id
            if conn_file_info == None:
                conn_file_info = "/tmp/" + str(raphtory_deployment_id) + "_python_gateway_connection_file"
            self._conn_file_info = conn_file_info
            self.gateway = self.setupJavaGateway()
            self.importDefaults()
            self.graph = self.setupRaphtory()
        else:
            self.gateway = None

    def importDefaults(self):
        '''
        This function imports a some default classes used by Raphtory
        '''
        self.java_import("com.raphtory.deployment.Raphtory")
        self.java_import("scala.collection.JavaConverters")
        self.java_import("com.raphtory.output.FileOutputFormat")
        self.java_import("com.raphtory.output.PulsarOutputFormat")
        self.java_import("com.raphtory.util.PythonUtil")

    def java_import(self, import_class):
        '''
        Wrapper around py4j java import

        Parameters:
            import_class __string__: Java class to import

        '''
        py4j_java_import(self.gateway.jvm, import_class)

    def setupRaphtory(self):
        '''
        Setups a raphtory java client via the gateway object.
        This allows the user to invoke raphtory java/scalaa methods as if they were running on the
        raphtory/scala version.
        Note that arguements must be correct or the methods will not be called.
        Uses internal raphtory_deployment_id: the deployment id of the raphtory instance to connect to

        Returns:
            graph: raphtory client graph object
        '''
        print("Creating Raphtory java object...")
        customConfig = {"raphtory.deploy.id": self._raphtory_deployment_id, "raphtory.deploy.distributed": True}
        mc_run_map_dict = MapConverter().convert(customConfig, self.gateway._gateway_client)
        jmap = self.gateway.jvm.PythonUtil.toScalaMap(mc_run_map_dict)
        graph = self.gateway.jvm.Raphtory.connect(jmap)
        print("Created Raphtory java object.")
        return graph

    def setupJavaGateway(self, conn_info_file=None):
        '''
        Creates Java<>Raphtory gateway and imports required files.
        The gateway allows the user to run/read java/scala methods and objects from python.

        Parameters:
            conn_info_file: full absolute file path to the connection info, usually '/tmp/raphtory_python_gateway_'+deployment_id

        Returns:
            py4j.java_gateway: py4j java gateway object
        '''

        if exists(self._conn_file_info):
            with open(self._conn_file_info, "rb") as info_file:
                gateway_port = read_int(info_file)
                gateway_secret = UTF8Deserializer().loads(info_file)
        else:
            print("File %s does not exist. Cannot open secure gateway without this.", self._conn_file_info)
            sys.exit(1)

        print("Setting up Java gateway...")
        gateway = JavaGateway(gateway_parameters=GatewayParameters(port=gateway_port, auth_token=gateway_secret))
        print("Java gateway connected.")
        return gateway

    def java(self):
        '''
        short helper function to make code easier to read

        Parameters:
        none

        Returns:
        java gateway jvm: gateway jvm object
        '''
        return self.gateway.jvm

    def make_name(self):
        '''
        Helper function which generates a random
        subscription suffix for the reader.

        Parameters:
        none

        Returns:
        str: subcription suffix
        '''
        return ''.join(random.choice(string.ascii_letters + string.punctuation) for x in range(10))

    def setupPulsarClient(self, client_args, max_attempts=5):
        '''
        Setups a pulsar client using the pulsar address.
        Retries at least 5 times before returning

        Parameters:
        client_args (dict): Dict of arguments to be used in the pulsar client,
                            keys must match pulsar.Client parameters
        max_attempts (int) : Number of attempts to retry

        Returns:
            PulsarClient: A pulsar client object if successful
            None (None): None if not successful
        '''
        print('Creating RaphtoryClient object...')
        attempts = 0
        while attempts <= max_attempts:
            attempts += 1
            try:
                client = pulsar.Client(**client_args)
                print('Created.')
                return client
            except Exception as e:
                print('Could not connect Client to Pulsar, trying again...')
                print(e)
        print('Could not connect client to Pulsar')
        return None

    def createReader(self, topic, subscription_name='', schema=schema.StringSchema()):
        '''
        Setups a single pulsar reader, which reads from a pulsar topic.
        Retries at least 5 times before returning

        Parameters:
            topic (str): Names of topic to read from
            subscription_name (str): Name for this readers subscription
            schema (Pulsar Schema): Schema to use for reader

        Returns:
            PulsarReader: A pulsar reader object
            None (None): None if not successful
        '''
        if subscription_name == '':
            subscription_name = "python_reader_" + self.make_name()
        attempts = 0
        while (attempts <= 5):
            attempts += 1
            try:
                reader = self._pulsar_client.create_reader(
                    topic,
                    pulsar.MessageId.earliest,
                    reader_name=subscription_name + '_' + self.make_name(),
                    schema=schema)
                print("Connected to topic: " + str(topic))
                return reader
                break
            except Exception as e:
                print("Could not connect to " + str(topic) + ", trying again")
                print(e)
        if attempts == 5:
            print("Could not connect to " + str(topic) + " after 5 attempts")
        return None

    def getStats(self, topic, tenant="public", namespace="default"):
        '''
        Reads stats from a pulsar topic using the admin interface.
        If success returns the response as json else returns an empty dict.

        Parameters:
            topic (str): Topic to obtain stats from
            tenant (str): (Optional, default: public) Pulsar tenant to access
            namepsace (str): (Optional, default: default) Pulsar namespace to access

        Returns:
            json response (dict/json): The response of the request. If unsuccessful then returns an empty dict.
        '''
        url = self._admin_url + "/admin/v2/persistent/" + tenant + "/" + namespace + "/" + topic + "/stats"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print("Error " + str(response.status_code) + " when reading topic stats from " + url)
            return {}

    def getDataframe(self, reader, delimiter=',', max_messages=sys.maxsize, col_names=[]):
        '''
        Using the reader, reads a complete topic and converts
        it into a pandas Dataframe. This will split each message
        from the reader using the delimiter.
        If columns names are not then, then by default, columns are called result_N.

        Parameters:
            reader        (Pulsar Reader): Reader where messages will be pulled from
            delimiter (str): the delimiter for parsing the results
            max_messages (int): (Optional, default:sys.maxsize) The number of messages to return.
                                By default, it returns the entire topic. This may cause memory
                                issues.
            col_names (list[string]): (Optional: default: ["timestamp", "window", "id"]). These are
                                        the names of the columns. By default this expects the results
                                        to have three columns called timestamp, window and id. Any
                                        columns after this are called result_N.

        Returns:
            dataframe (pandas.dataframe): A dataframe of the topic
        '''
        print("Obtaining dataframe...\n")
        messages = []
        count = 0
        wait_counter = 0
        while (count < max_messages):
            if count % 100 == 0:
                print("Results processed %i" % (count), end="\r")
            try:
                message_temp = reader.read_next(timeout_millis=3000)
                if message_temp != None:
                    decoded = message_temp.data().decode("utf-8").replace("\n", "").split(delimiter)
                    messages.append(decoded)
                    count += 1
                else:
                    time.sleep(2)
                    wait_counter += 1
                    if wait_counter == 3:
                        break
            except pulsar.Timeout as e:
                wait_counter += 1
                if wait_counter == 3:
                    break
            except Exception as e:
                print("Issue retrieving messages. trying again...")
                print(e)
                wait_counter += 1
                if wait_counter == 6:
                    break
        print("Converting to columns...")
        print("Completed.")
        return pd.DataFrame(messages)

    def find_dates(self, all_data, node_a_id=0, node_b_id=1, time_col=2):
        '''
        Given a dataframe of edges, this will find the first time an item was seen.
        This is returned as a dict with the key being the id and the value time.
        This can be helpful when trying to identify when a node was first created.

        Parameters:
            all_data (dataframe): A dataframe containing a column with keys and a column with times/numbers to compare with.
            node_a_id (int): Position of the id or name to use as key for node A
            node_b_id (int): Position of the id or name to use as key for node B
            time_col (int): Position of the time column which is compared

        Returns:
            first_seen (dict): A dictionary with the key= node_id and the value = time
        '''
        first_seen = {}
        for (i, row) in all_data.iterrows():
            if (row[node_a_id] not in first_seen) or (
                    row[node_a_id] in first_seen and first_seen[row[node_a_id]] > int(row[time_col])):
                first_seen[row[node_a_id]] = int(row[time_col])
            if (row[node_b_id] not in first_seen) or (
                    row[node_b_id] in first_seen and first_seen[row[node_b_id]] > int(row[time_col])):
                first_seen[row[node_b_id]] = int(row[time_col])
        return first_seen

    def add_node_attributes(self, G, results, abbr, row_id=2, time_col=0, window_col=-1,
                            result_col=3):
        '''
        Given a graph, an array of attributes and a abbreviation.
        This will add all the attributes to the graph.
        For example, given a graph G, results

        Parameters:
            G (networkx.graph): A networkx graph
            results (list[dict]): A list of dataframes which contain attributes.
                                  The format for attributes is a dataframe with
                                   id: node id
                                   timestamp: time the attribute was created
                                   window: (optional) the window the attribute was created
                                   result_col: the value of the attribute
            abbr    (list(str)): A list of strings which correspond to the abbreviation used when appending the attribute.
            row_id  (int/str): Column position which contains ID / Name of the row id column to use, must be the same across results
            time_col (int/str): Column position which contains the timestamp / Name of the timestamp column to use
            result_col (int/str): Column position which contains result / Name of the result column
            window_col (int/str): (Optional, default:'window') Column position which contains window /
                                  Name of the window column, set to '' if not being used
        '''
        attributes = {}
        for (j, result) in enumerate(results):
            for (i, row) in result.iterrows():
                if row[row_id] not in attributes:
                    attributes[row[row_id]] = {}
                if window_col != -1:
                    attributes[row[row_id]][abbr[j] + '_' + row[time_col] + '_' + row[window_col]] = row[result_col]
                else:
                    attributes[row[row_id]][abbr[j] + '_' + row[time_col]] = row[result_col]
        nx.set_node_attributes(G, attributes)

    def createGraphFromEdgeList(self, df, isMultiGraph=True):
        '''
        Builds a simple networkx graph from an edge list in Raphtory.

        Parameters:
            df (pandas.Dataframe): A dataframe of an edgelist where, col 0: timestamp, col 1: source, col 2: destination
            isMultiGraph (bool): If False will use DiGraph, otherwise will use MultiGraph
        Returns:
            G (networkx.DiGraph): The graph as built in networkx
        '''
        print('Creating graph...')
        if isMultiGraph:
            G = nx.MultiDiGraph()
        else:
            G = nx.DiGraph()
        for (i, row) in df.iterrows():
            G.add_edge(row[1], row[2], timestamp=row[0])
        print('Done.')
        return G

    def createLOTRGraph(self, filePath, from_time=0, to_time=sys.maxsize, source_id=0, target_id=1, timestamp_col=2):
        '''
        Example graph builder in python. Given a csv edgelist this will create a graph using networkx based on the lotr data.

        Parameters:
            filePath (str): Location of csv file
            from_time (int): (Optional, default: 0) timestamp to start building graph from
            to_time (int): (Optional, default: sys.maxsize) timestamp to stop building graph
            source_id (int): column for source node
            target_id (int): column for target node
            timestamp_col (int): column for lotr timestamp

        Returns:
            G (networkx.DiGraph): The graph as built in networkx
        '''
        G = nx.DiGraph()
        self._first_seen = self.find_dates(pd.read_csv(filePath), node_a_id=source_id, node_b_id=target_id,
                                           time_col=timestamp_col)
        with open(filePath) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                edge_timestamp = int(row[timestamp_col])
                if from_time <= edge_timestamp <= to_time:
                    node_a_time = datetime.fromtimestamp(int(self._first_seen[row[source_id]]), tz=timezone.utc)
                    G.add_node(row[source_id], create_time=int(self._first_seen[row[source_id]]))
                    node_b_time = datetime.fromtimestamp(int(self._first_seen[row[target_id]]), tz=timezone.utc)
                    G.add_node(row[target_id], create_time=int(self._first_seen[row[target_id]]))
                    edge_timestamp = edge_timestamp
                    edge_ts = datetime.fromtimestamp(edge_timestamp * 1000, tz=timezone.utc)
                    G.add_edge(row[source_id], row[target_id], time=edge_timestamp)
        print("Number of nodes " + str(G.number_of_nodes()))
        print("Number of edged " + str(G.number_of_edges()))
        return G
