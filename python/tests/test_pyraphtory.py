from pyraphtory.api.context import PyRaphtory
from pyraphtory.api.sources import SqliteConnection
from pyraphtory.api.sources import SqlEdgeSource
from pyraphtory.api.sources import SqlVertexSource
from pyraphtory.api.vertex import Vertex
import pyraphtory
from pyraphtory._config import get_java_home
import unittest
from numpy import array_equal
from pyraphtory.api.input import *
from unittest import mock
from pathlib import Path
import urllib.request


class PyRaphtoryTest(unittest.TestCase):

    ctx = None

    @classmethod
    def setUpClass(cls):
        print("Starting Pyraphtory context...")
        cls.ctx = PyRaphtory.local()

    @classmethod
    def tearDownClass(cls):
        print("Finished all tests. Closing context")
        cls.ctx.close()

    def _launch_context(self):
        self.assertTrue(self.ctx.classname, 'com.raphtory.internals.context.PyRaphtoryContext')

    def _new_graph(self):
        graph = self.ctx.new_graph()
        self.assertTrue(type(graph), pyraphtory.graph.DeployedTemporalGraph)
        graph.destroy(force=True)

    def test_add_vertex(self):
        graph = self.ctx.new_graph()
        graph.add_vertex(1, 999)
        df = graph.step(lambda vertex: vertex.set_state("name",vertex.name())).select("name").to_df()
        expected_result = ',timestamp,name\n0,1,999\n'
        self.assertTrue(df.to_csv(), expected_result)
        graph.destroy(force=True)

    def test_add_edges(self):
        graph = self.ctx.new_graph()
        graph.add_edge(1, 1, 2)
        df = graph.step(lambda v: v.set_state("name",v.name()))\
                  .step(lambda v: v.set_state("degree",v.degree()))\
                  .step(lambda v: v.set_state("in_edges",v.in_degree()))\
                  .step(lambda v: v.set_state("out_edges",v.out_degree())) \
                  .select("name", "degree", "in_edges", "out_edges") \
                  .to_df()
        expected_result = ',timestamp,name,degree,in_edges,out_edges\n0,1,1,1,0,1\n1,1,2,1,1,0\n'
        self.assertTrue(df.to_csv(), expected_result)
        graph.destroy(force=True)

    def test_run_page_rank_algo(self):
        graph = self.ctx.new_graph()
        graph.add_edge(1, 1, 2)
        graph.add_edge(1, 1, 3)
        graph.add_edge(1, 1, 4)
        graph.add_edge(1, 1, 5)
        graph.add_edge(1, 2, 5)
        graph.add_edge(1, 3, 5)
        graph.add_edge(1, 4, 5)
        df_pagerank = graph.at(1) \
            .past() \
            .step(lambda v: v.set_state("name",v.name())) \
            .transform(self.ctx.algorithms.generic.centrality.PageRank()) \
            .select() \
            .to_df()
        expected_result = ",timestamp,name,prlabel" \
                          "\n0,1,1,0.559167686075918" \
                          "\n1,1,2,0.677990217572423" \
                          "\n2,1,3,0.677990217572423" \
                          "\n3,1,4,0.677990217572423" \
                          "\n4,1,5,2.406861661206814\n"
        df_pagerank = df_pagerank.round(15)
        print(df_pagerank.to_csv())
        self.assertEqual(df_pagerank.to_csv(), expected_result)
        graph.destroy(force=True)

    def test_check_vertex_properties_with_history(self):
        graph = self.ctx.new_graph()
        graph.add_vertex(1, 1, properties=[ImmutableString("name", "TEST"),MutableString("strProp", "TESTstr")])
        graph.add_vertex(2, 1, properties=[MutableString("strProp", "TESTstr2")])
        df = graph.at(1).past().step(lambda v: v.set_state("name",v.name()))\
                                .step(lambda v: v.set_state("strProp",v.get_property_or_else('strProp', '')))\
                                .select("name", "strProp")\
                                .to_df()
        df_time = graph.at(2).past().step(lambda v: v.set_state("name",v.name()))\
                                    .step(lambda v: v.set_state("strProp",v.get_property_or_else('strProp', '')))\
                                    .select("name", "strProp")\
                                    .to_df()
        expected_value = ',timestamp,name,strProp\n0,1,TEST,TESTstr\n'
        expected_value_time = ',timestamp,name,strProp\n0,2,TEST,TESTstr2\n'
        self.assertEqual(df.to_csv(), expected_value)
        self.assertEqual(df_time.to_csv(), expected_value_time)
        graph.destroy(force=True)



    def test_message_vertex(self):
        
        def iterateMessages(v: Vertex):
            queue = v.message_queue()
            messages = [message for message in queue]
            v.set_state("id",v.name())
            v.set_state("message", messages)

        with self.ctx.new_graph() as graph:
            graph.add_edge(1, 1, 2)
            cols = ["id", "message"]
            df = (graph.step(lambda vertex: vertex.message_vertex(1, "message"))
                  .step(lambda vertex: iterateMessages(vertex))
                  .select("id", "message")
                  .explode("message")
                  .to_df())
            print(df.to_csv)
            assert array_equal(df["id"], ["1", "1"])
            assert array_equal(df["message"], ["message", "message"])

    def test_get_java_home_no_java(self):
        # Mock the os.getenv function to return None
        with mock.patch('os.getenv') as mock_getenv:
            mock_getenv.return_value = 'test'
            # Run the function
            java_home = get_java_home()
            # Check that it returns None
            self.assertEqual(java_home, Path('test'))
        # Mock the shutil.which function to return True
        with mock.patch('shutil.which') as mock_which:
            with mock.patch('os.getenv') as mock_getenv:
                mock_getenv.return_value = None
                mock_which.return_value = '/tmp/test/bin/java'
                # Run the function
                java_home = get_java_home()
                # Check that it returns None
                self.assertEqual(java_home, Path('/tmp/test').resolve())
        # Mock the shutil.which function and the mock.patch function to return None
        with mock.patch('shutil.which') as mock_which:
            with mock.patch('os.getenv') as mock_getenv:
                mock_getenv.return_value = None
                mock_which.return_value = None
                # assert that it throws an exception
                self.assertRaises(FileNotFoundError, get_java_home)

    def test_type(self):
        t = Type("test")
        self.assertEqual(t.name(), "test")

    def test_immutable_string(self):
        s = ImmutableString(key="key", value="value")
        self.assertEqual(s.key(), "key")
        self.assertEqual(s.value(), "value")

    def test_mutable_string(self):
        s = MutableString(key="key", value="value")
        self.assertEqual(s.key(), "key")
        self.assertEqual(s.value(), "value")

    def test_mutable_long(self):
        long = MutableLong("test", 1)
        self.assertEqual(long.value(), 1)
        self.assertEqual(long.key(), "test")

    def test_mutable_double(self):
        d = MutableDouble("test", 1.0)
        self.assertEqual(d.value(), 1.0)
        self.assertEqual(d.key(), "test")

    def test_mutable_float(self):
        d = MutableFloat("test", 1.0)
        self.assertEqual(d.value(), 1.0)
        self.assertEqual(d.key(), "test")

    def test_mutable_boolean(self):
        b = MutableBoolean("test", False)
        self.assertEqual(b.value(), False)
        self.assertEqual(b.key(), "test")

    def test_mutable_integer(self):
        i = MutableInteger("test", 2)
        self.assertEqual(i.value(), 2)
        self.assertEqual(i.key(), "test")

    def test_sql_source(self):
        if not Path('/tmp/lotr.db').is_file():
            urllib.request.urlretrieve('https://raw.githubusercontent.com/Raphtory/Data/main/lotr.db', '/tmp/lotr.db')
        graph = self.ctx.new_graph()
        sqlite = SqliteConnection('/tmp/lotr.db')
        graph.load(SqlEdgeSource(sqlite, 'select * from lotr', 'source', 'target', 'line'))
        graph.load(SqlVertexSource(sqlite, 'select * from lotr', 'source', 'line'))
        graph.destroy(force=True)
