from pyraphtory.context import PyRaphtory
from pyraphtory.graph import Row
import pyraphtory
import unittest
from numpy import array_equal
from pyraphtory.input import *



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

    def test_pyraphtory_launch_context(self):
        self.assertTrue(self.ctx.classname, 'com.raphtory.internals.context.PyRaphtoryContext')

    def test_pyraphtory_new_graph(self):
        graph = self.ctx.new_graph()
        self.assertTrue(type(graph), pyraphtory.graph.DeployedTemporalGraph)
        graph.destroy(force=True)

    def test_add_vertex(self):
        graph = self.ctx.new_graph()
        graph.add_vertex(1, 999)
        df = graph.select(lambda vertex: pyraphtory.graph.Row(vertex.name())).to_df(["name"])
        expected_result = ',timestamp,name\n0,1,999\n'
        self.assertTrue(df.to_csv(), expected_result)
        graph.destroy(force=True)

    def test_add_edges(self):
        graph = self.ctx.new_graph()
        graph.add_edge(1, 1, 2)
        df = graph.select(lambda v:
                          pyraphtory.graph.Row(v.name(), v.degree(), v.in_degree(), v.out_degree())) \
            .to_df(["name", "degree", "in_edges", "out_edges"])
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
        cols = ["prlabel"]
        df_pagerank = graph.at(1) \
            .past() \
            .transform(self.ctx.algorithms.generic.centrality.PageRank()) \
            .execute(self.ctx.algorithms.generic.NodeList(*cols)) \
            .to_df(["name"] + cols)
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
        df = graph.at(1).past().select(lambda v: pyraphtory.graph.Row(v.name(),v.get_property_or_else('strProp', '')))\
            .to_df(['name', 'strProp'])
        df_time = graph.at(2).past().select(lambda v: pyraphtory.graph.Row(v.name(),
                                                                           v.get_property_or_else('strProp', '')))\
            .to_df(['name', 'strProp'])
        expected_value = ',timestamp,name,strProp\n0,1,TEST,TESTstr\n'
        expected_value_time = ',timestamp,name,strProp\n0,2,TEST,TESTstr2\n'
        self.assertEqual(df.to_csv(), expected_value)
        self.assertEqual(df_time.to_csv(), expected_value_time)
        graph.destroy(force=True)

    def test_message_vertex(self):
        with self.ctx.new_graph() as graph:
            graph.add_edge(1, 1, 2)
            df = (graph.step(lambda vertex: vertex.message_vertex(1, "message"))
                  .explode_select(lambda vertex: [Row(vertex.name(), message) for message in vertex.message_queue()])).to_df(["id", "message"])
            assert array_equal(df["id"], ["1", "1"])
            assert array_equal(df["message"], ["message", "message"])

