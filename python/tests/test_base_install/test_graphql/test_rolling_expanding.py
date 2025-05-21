import tempfile
import time

import pytest

from raphtory.graphql import GraphServer
from raphtory import Graph, PersistentGraph
import json
import re
from utils import run_graphql_test, run_graphql_error_test, run_group_graphql_error_test


def create_graph_epoch(g):
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 2)
    g.add_edge(3, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(3, 1, 3)
    g.add_edge(4, 1, 3)


# def test_start():
#     tmp_work_dir = tempfile.mkdtemp()
#     with GraphServer(tmp_work_dir).start(1736) as server:
#         graph = Graph()
#         create_graph_epoch(graph)
#         client = server.get_client()
#         client.send_graph(path="g", graph=graph)
#         time.sleep(500000)


def test_graph_epoch():
    graph = Graph()
    create_graph_epoch(graph)
    query = """
    {
      graph(path: "g") {
        rolling(window: {epoch: 1}) {
          list {
            earliestTime
          }
        }
      }
    }
    """
    correct = {
        "graph": {
            "rolling": {
                "list": [
                    {"earliestTime": 1},
                    {"earliestTime": 2},
                    {"earliestTime": 3},
                    {"earliestTime": 4},
                ]
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_zero_step():
    graph = Graph()
    create_graph_epoch(graph)
    queries_and_exceptions = []
    zero_exception = "Failed to parse time string: 0 size step is not supported"
    # graph fail test
    query = """
    {
      graph(path: "g") {
        rolling(window:{duration:"1 day"},step:{duration:"0 day"}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window:{epoch:100},step:{epoch:0}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window:{duration:"0 day"}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window:{epoch:0}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        expanding(step:{duration:"0 day"}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        expanding(step:{epoch:0}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    # node tests
    query = """
        {
      graph(path: "g") {
        node(name: "1") {
            rolling(window:{duration:"1 day"},step:{duration:"0 year"}){
            list {
              earliestTime
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    query = """
        {
      graph(path: "g") {
        node(name: "1") {
            rolling(window:{epoch:100},step:{epoch:0}){
            list {
              earliestTime
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    query = """
        {
      graph(path: "g") {
        node(name: "1") {
            rolling(window:{duration:"0 day"}){
            list {
              earliestTime
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    query = """
        {
      graph(path: "g") {
        node(name: "1") {
            rolling(window:{epoch:0}){
            list {
              earliestTime
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    query = """
        {
      graph(path: "g") {
        node(name: "1") {
            expanding(step:{duration:"0 day"}){
            list {
              earliestTime
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    query = """
        {
      graph(path: "g") {
        node(name: "1") {
            expanding(step:{epoch:0}){
            list {
              earliestTime
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    # nodes tests
    query = """
    {
  graph(path: "g") {
    nodes {
      list {
        rolling(window:{duration:"1 day"},step:{duration:"0 year"}){
          list {
            earliestTime
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    nodes {
      list {
        rolling(window:{epoch:100},step:{epoch:0}){
          list {
            earliestTime
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
  graph(path: "g") {
    nodes {
      list {
        rolling(window:{duration:"0 day"}){
          list {
            earliestTime
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    nodes {
      list {
        rolling(window:{epoch:0}){
          list {
            earliestTime
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
  graph(path: "g") {
    nodes {
      list {
        expanding(step:{duration:"0 day"}){
          list {
            earliestTime
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        nodes {
          list {
            expanding(step:{epoch:0}){
              list {
                earliestTime
              }
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    # path from nodes tests
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        rolling(window:{duration:"1 day"},step:{duration:"0 year"}){
          list {
            list {
              earliestTime
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        rolling(window:{epoch:100},step:{epoch:0}){
          list {
            list {
              earliestTime
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        rolling(window:{duration:"0 year"}){
          list {
            list {
              earliestTime
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        rolling(window:{epoch:0}){
          list {
            list {
              earliestTime
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        expanding(step:{duration:"0 year"}){
          list {
            list {
              earliestTime
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        node(name: "1") {
          neighbours {
            expanding(step:{epoch:0}){
              list {
                list {
                  earliestTime
                }
              }
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    # edge tests
    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
    rolling(window:{duration:"1 day"},step:{duration:"0 year"}){
        list {
          earliestTime
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
        rolling(window:{epoch:100},step:{epoch:0}){
            list {
                earliestTime
            }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
    rolling(window:{duration:"0 year"}){
        list {
          earliestTime
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
        rolling(window:{epoch:0}){
            list {
                earliestTime
            }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
    expanding(step:{duration:"0 year"}){
        list {
          earliestTime
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        edge(src: "1", dst: "2") {
            expanding(step:{epoch:0}){
                list {
                    earliestTime
                }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    # edges tests
    query = """
{
  graph(path: "g") {
    edges {
    rolling(window:{duration:"1 day"},step:{duration:"0 year"}){
        list {
          list{
            earliestTime
          }

        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edges {
    rolling(window:{epoch:100},step:{epoch:0}){
        list {
          list{
            earliestTime
          }

        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edges {
    rolling(window:{duration:"0 year"}){
        list {
          list{
            earliestTime
          }

        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edges {
    rolling(window:{epoch:0}){
        list {
          list{
            earliestTime
          }

        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edges {
    expanding(step:{duration:"0 year"}){
        list {
          list{
            earliestTime
          }

        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        edges {
        expanding(step:{epoch:0}){
            list {
              list{
                earliestTime
              }
    
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))
    run_group_graphql_error_test(queries_and_exceptions, graph)


def test_wrong_window():
    graph = Graph()
    create_graph_epoch(graph)
    queries_and_exceptions = []
    mismatch_exception = "Your window and step must be of the same type: duration (string) or epoch (int)"
    parse_exception = "Failed to parse time string: one of the tokens in the interval string supposed to be a number couldn't be parsed"
    parse_exception2 = "Failed to parse time string: 'monthdas' is not a valid unit"
    too_many_exception = "Invalid value for argument \\"
    # graph fail test
    query = """
    {
      graph(path: "g") {
        rolling(window:{duration:"1 day"},step:{epoch:100}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window:{epoch:100},step:{duration:"1 day"}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window:{duration:"1dasdas day"}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, parse_exception))

    query = """
    {
  graph(path: "g") {
    rolling(window:{duration:"1 day"},step:{duration:"1 monthdas"}){
			list{
        earliestTime
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, parse_exception2))

    query = """
    {
  graph(path: "g") {
    rolling(window:{duration:"1 day",epoch:11}){
			list{
        earliestTime
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, too_many_exception))

    # node tests
    query = """
    {
  graph(path: "g") {
    node(name: "1") {
      rolling(window: {duration: "1 day"}, step: {epoch: 11}) {
        list {
          earliestTime
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
    {
  graph(path: "g") {
    node(name: "1") {
      rolling(window: {epoch: 100}, step: {duration: "1 day"}) {
        list {
          earliestTime
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    # nodes tests
    query = """
    {
  graph(path: "g") {
    nodes {
      list {
      rolling(window: {duration: "1 day"}, step: {epoch: 11}) {
          list {
            earliestTime
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
{
  graph(path: "g") {
    nodes {
      list {
        rolling(window: {epoch: 100}, step: {duration: "1 day"}) {
          list {
            earliestTime
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    # path from nodes tests
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
      rolling(window: {duration: "1 day"}, step: {epoch: 11}) {
          list {
            list {
              earliestTime
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        rolling(window: {epoch: 100}, step: {duration: "1 day"}) {
          list {
            list {
              earliestTime
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    # edge tests
    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
      rolling(window: {duration: "1 day"}, step: {epoch: 11}) {
        list {
          earliestTime
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
      rolling(window: {epoch: 100}, step: {duration: "1 day"}) {
        list {
          earliestTime
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    # edges tests
    query = """
{
  graph(path: "g") {
    edges {
      rolling(window: {duration: "1 day"}, step: {epoch: 11}) {
        list {
          list{
            earliestTime
          }
          
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
{
  graph(path: "g") {
    edges {
      rolling(window: {epoch: 100}, step: {duration: "1 day"}) {
        list {
          list{
            earliestTime
          }
          
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))
    run_group_graphql_error_test(queries_and_exceptions, graph)
