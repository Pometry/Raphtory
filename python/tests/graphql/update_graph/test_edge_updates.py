import tempfile
from datetime import datetime, timezone
import pytest
from dateutil import parser
from raphtory.graphql import GraphServer, RaphtoryClient


def make_props():
    current_datetime = datetime.now(timezone.utc)
    naive_datetime = datetime.now()
    return {
        "prop_string": "blah",
        "prop_float": 2.0,
        "prop_int": 2,
        "prop_bool": True,
        "prop_map": {
            "prop_inner_string": "blah",
            "prop_inner_float": 2.0,
            "prop_inner_int": 2,
            "prop_inner_bool": True,
        },
        "prop_array": [1, 2, 3, 4, 5, 6],
        "prop_datetime": current_datetime,
        "prop_gertime": current_datetime,
        "prop_naive_datetime": naive_datetime,
    }


def make_props2():
    current_datetime = datetime.now(timezone.utc)
    naive_datetime = datetime.now()
    return {
        "prop_string": "blah2",
        "prop_float": 3.0,
        "prop_int": 23,
        "prop_bool": False,
        "prop_map": {
            "prop_inner_string": "b",
            "prop_inner_float": 6.0,
            "prop_inner_int": 332,
            "prop_inner_bool": True,
        },
        "prop_array": [1, 2, 3, 5, 6],
        "prop_datetime": current_datetime,
        "prop_gertime": current_datetime,
        "prop_naive_datetime": naive_datetime,
    }


def test_add_updates():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        edge = rg.add_edge(1, "ben", "hamza")
        edge.add_updates(2, props, layer="test")
        edge.add_updates(3, props)
        edge.add_updates(4, layer="test2")
        edge.add_updates(5)
        rg.edge("ben", "hamza").add_updates(6)
        g = client.receive_graph("path/to/event_graph")
        for k, v in props.items():
            if isinstance(v, dict):
                for inner_k, inner_v in v.items():
                    assert props["prop_map"][inner_k] == inner_v
            elif isinstance(v, datetime):
                actual = parser.parse(g.edge("ben", "hamza").properties.get(k))
                assert v == actual
            else:
                assert g.edge("ben", "hamza").properties.get(k) == v

        e = g.edge("ben", "hamza")
        assert e.properties.temporal.get("prop_float").history() == [2, 3]
        assert e.layer("test").properties.temporal.get("prop_float").history() == [2]
        assert e.history() == [1, 2, 3, 4, 5, 6]


def test_add_constant_properties():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        props2 = make_props2()
        edge = rg.add_edge(1, "ben", "hamza")
        rg.add_edge(1, "ben", "hamza", layer="test")

        edge.add_constant_properties(props)
        rg.edge("ben", "hamza").add_constant_properties(props2, layer="test")
        g = client.receive_graph("path/to/event_graph")
        for k, v in props.items():
            if isinstance(v, dict):
                for inner_k, inner_v in v.items():
                    assert props["prop_map"][inner_k] == inner_v
            elif isinstance(v, datetime):
                actual = parser.parse(
                    g.edge("ben", "hamza").layer("_default").properties.get(k)
                )
                assert v == actual
            else:
                assert g.edge("ben", "hamza").layer("_default").properties.get(k) == v

        for k, v in props2.items():
            if isinstance(v, dict):
                for inner_k, inner_v in v.items():
                    assert props2["prop_map"][inner_k] == inner_v
            elif isinstance(v, datetime):
                actual = parser.parse(
                    g.edge("ben", "hamza").layer("test").properties.get(k)
                )
                assert v == actual
            else:
                assert g.edge("ben", "hamza").layer("test").properties.get(k) == v

        with pytest.raises(Exception) as excinfo:
            rg.edge("ben", "hamza").add_constant_properties({"prop_float": 3.0})
        assert "Tried to mutate constant property prop_float" in str(excinfo.value)


def test_update_constant_properties():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        props2 = make_props2()
        edge = rg.add_edge(1, "ben", "hamza")
        rg.add_edge(1, "ben", "hamza", layer="test")

        edge.update_constant_properties(props)
        rg.edge("ben", "hamza").update_constant_properties(props2, layer="test")
        g = client.receive_graph("path/to/event_graph")
        for k, v in props.items():
            if isinstance(v, dict):
                for inner_k, inner_v in v.items():
                    assert props["prop_map"][inner_k] == inner_v
            elif isinstance(v, datetime):
                actual = parser.parse(
                    g.edge("ben", "hamza").layer("_default").properties.get(k)
                )
                assert v == actual
            else:
                assert g.edge("ben", "hamza").layer("_default").properties.get(k) == v

        for k, v in props2.items():
            if isinstance(v, dict):
                for inner_k, inner_v in v.items():
                    assert props2["prop_map"][inner_k] == inner_v
            elif isinstance(v, datetime):
                actual = parser.parse(
                    g.edge("ben", "hamza").layer("test").properties.get(k)
                )
                assert v == actual
            else:
                assert g.edge("ben", "hamza").layer("test").properties.get(k) == v
        edge.update_constant_properties(props2)
        g = client.receive_graph("path/to/event_graph")
        for k, v in props2.items():
            if isinstance(v, dict):
                for inner_k, inner_v in v.items():
                    assert props2["prop_map"][inner_k] == inner_v
            elif isinstance(v, datetime):
                actual = parser.parse(
                    g.edge("ben", "hamza").layer("_default").properties.get(k)
                )
                assert v == actual
            else:
                assert g.edge("ben", "hamza").layer("_default").properties.get(k) == v


def test_delete():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")

        edge = rg.add_edge(1, "ben", "hamza")
        with pytest.raises(Exception) as excinfo:
            edge.delete(2)
        assert "Event Graph doesn't support deletions" in str(excinfo.value)

        client.new_graph("path/to/persistent_graph", "PERSISTENT")
        rg = client.remote_graph("path/to/persistent_graph")
        rg.add_edge(1, "ben", "hamza")
        rg.edge("ben", "hamza").delete(2)
        edge = rg.add_edge(1, "ben", "lucas", layer="colleagues")
        edge.delete(2, layer="colleagues")
        g = client.receive_graph("path/to/persistent_graph")
        assert g.edge("ben", "hamza").deletions() == [2]
        assert g.edge("ben", "lucas").deletions() == [2]
