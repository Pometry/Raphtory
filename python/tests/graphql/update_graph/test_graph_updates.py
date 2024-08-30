import tempfile

import pytest
from raphtory.graphql import GraphServer, RaphtoryClient
from datetime import datetime, timezone
from dateutil import parser


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


def test_add_constant_properties():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        rg.add_constant_properties(props)
        g = client.receive_graph("path/to/event_graph")
        for k, v in props.items():
            if isinstance(v, dict):
                for inner_k, inner_v in v.items():
                    assert props["prop_map"][inner_k] == inner_v
            elif isinstance(v, datetime):
                actual = parser.parse(g.properties.get(k))
                assert v == actual
            else:
                assert g.properties.get(k) == v

        with pytest.raises(Exception) as excinfo:
            rg.add_constant_properties({"prop_float": 3.0})
        assert "Failed to mutate graph" in str(excinfo.value)


def test_update_constant_properties():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()

        rg.update_constant_properties(props)
        g = client.receive_graph("path/to/event_graph")
        for k, v in props.items():
            if isinstance(v, dict):
                for inner_k, inner_v in v.items():
                    assert props["prop_map"][inner_k] == inner_v
            elif isinstance(v, datetime):
                actual = parser.parse(g.properties.get(k))
                assert v == actual
            else:
                assert g.properties.get(k) == v

        rg.update_constant_properties({"prop_float": 3.0})
        g = client.receive_graph("path/to/event_graph")
        assert g.properties.get("prop_float") == 3.0


def test_add_properties():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        rg.add_property(1, props)
        current_datetime = datetime.now(timezone.utc)
        naive_datetime = datetime.now()
        rg.add_property(current_datetime, props)
        rg.add_property(naive_datetime, props)
        g = client.receive_graph("path/to/event_graph")
        for k, v in props.items():
            if isinstance(v, dict):
                for inner_k, inner_v in v.items():
                    assert props["prop_map"][inner_k] == inner_v
            elif isinstance(v, datetime):
                actual = parser.parse(g.properties.get(k))
                assert v == actual
            else:
                assert g.properties.get(k) == v
        localized_datetime = naive_datetime.replace(tzinfo=timezone.utc)
        timestamps = [
            1,
            int(current_datetime.timestamp() * 1000),
            int(localized_datetime.timestamp() * 1000),
        ]

        assert g.properties.temporal.get("prop_map").history() == timestamps


def test_add_node():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg = client.remote_graph("path/to/event_graph")
        props = make_props()
        current_datetime = datetime.now(timezone.utc)
        rg.add_node(current_datetime, "ben", properties=props, node_type="person")
        rg.add_node(current_datetime, 1)  # This gets stringified on the server
        rg.add_node(current_datetime, "hamza", node_type="person")
        g = client.receive_graph("path/to/event_graph")
        assert g.node("ben").node_type == "person"
        for k, v in props.items():
            if isinstance(v, dict):
                for inner_k, inner_v in v.items():
                    assert props["prop_map"][inner_k] == inner_v
            elif isinstance(v, datetime):
                actual = parser.parse(g.node("ben").properties.get(k))
                assert v == actual
            else:
                assert g.node("ben").properties.get(k) == v
        assert g.node("hamza").node_type == "person"
        assert g.node("1") is not None
