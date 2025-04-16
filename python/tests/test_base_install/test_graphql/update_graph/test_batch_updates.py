import tempfile
from datetime import datetime, timezone
from typing import List
from dateutil import parser
from numpy.testing import assert_equal as check_arr

from raphtory.graphql import (
    GraphServer,
    RaphtoryClient,
    RemoteGraph,
    RemoteUpdate,
    RemoteNodeAddition,
    RemoteEdgeAddition,
)


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
            "prop_map": {
                "prop_inner_string": "b",
                "prop_inner_float": 6.0,
                "prop_inner_int": 332,
                "prop_inner_bool": True,
            },
            "prop_inner_float": 2.0,
            "prop_inner_int": 2,
            "prop_inner_bool": True,
        },
        "prop_array": [1, 2, 3, 4, 5, 6],
        "prop_datetime": current_datetime,
        "prop_naive_datetime": naive_datetime,
    }


def helper_test_props(entity, props):
    for k, v in props.items():
        if isinstance(v, datetime):
            actual = parser.parse(entity.properties.get(k))
            assert v == actual
        else:
            assert entity.properties.get(k) == v


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
        "prop_naive_datetime": naive_datetime,
    }


def create_updates(timestamps: List[int]):
    for time in timestamps:
        if time % 2 == 1:
            update = RemoteUpdate(time, make_props())
        else:
            update = RemoteUpdate(time, make_props2())
        yield update


def test_add_nodes():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg: RemoteGraph = client.remote_graph("path/to/event_graph")
        node_updates = []

        # add node with all fields
        ben_updates = list(create_updates([1, 2, 3, 4]))
        node_updates.append(
            RemoteNodeAddition("ben", "person", make_props(), ben_updates)
        )
        # follow up with only timestamp updates
        node_updates.append(
            RemoteNodeAddition(
                "ben", node_type=None, updates=[RemoteUpdate(5), RemoteUpdate(6)]
            )
        )
        # follow up with nothing
        node_updates.append(RemoteNodeAddition("ben"))
        # Start with just updates
        node_updates.append(
            RemoteNodeAddition("hamza", updates=[RemoteUpdate(1), RemoteUpdate(2)])
        )
        # Start with just updates
        node_updates.append(RemoteNodeAddition("lucas", updates=[RemoteUpdate(1)]))
        # add constant properties
        lucas_props = make_props()
        node_updates.append(
            RemoteNodeAddition("lucas", constant_properties=lucas_props)
        )
        # add node_type
        node_updates.append(RemoteNodeAddition("lucas", node_type="person"))
        rg.add_nodes(node_updates)
        g = client.receive_graph("path/to/event_graph")
        ben = g.node("ben")
        hamza = g.node("hamza")
        lucas = g.node("lucas")
        check_arr(
            ben.properties.temporal.get("prop_float").values(),
            [
                2.0,
                3.0,
                2.0,
                3.0,
            ],
        )
        check_arr(ben.history(), [1, 2, 3, 4, 5, 6])
        assert ben.node_type == "person"
        check_arr(hamza.history(), [1, 2])
        assert hamza.node_type is None
        helper_test_props(lucas, lucas_props)
        assert lucas.node_type == "person"


def test_add_edges():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        rg: RemoteGraph = client.remote_graph("path/to/event_graph")
        edge_updates = []

        ben_hamza_updates = list(create_updates([1, 2, 3, 4]))
        edge_updates.append(
            RemoteEdgeAddition(
                "ben",
                "hamza",
                layer="test",
                constant_properties=make_props(),
                updates=ben_hamza_updates,
            )
        )
        # follow up with only timestamp updates
        edge_updates.append(
            RemoteEdgeAddition(
                "ben", "hamza", layer=None, updates=[RemoteUpdate(5), RemoteUpdate(6)]
            )
        )
        # follow up with nothing
        edge_updates.append(RemoteEdgeAddition("ben", "hamza"))
        # Start with just updates
        edge_updates.append(
            RemoteEdgeAddition(
                "hamza", "lucas", updates=[RemoteUpdate(1), RemoteUpdate(2)]
            )
        )
        # Start with just updates
        edge_updates.append(
            RemoteEdgeAddition("lucas", "hamza", updates=[RemoteUpdate(1)])
        )
        # add constant properties
        lucas_props = make_props()
        edge_updates.append(
            RemoteEdgeAddition("lucas", "hamza", constant_properties=lucas_props)
        )
        rg.add_edges(edge_updates)
        g = client.receive_graph("path/to/event_graph")
        ben_hammza = g.edge("ben", "hamza")
        hamza_lucas = g.edge("hamza", "lucas")
        lucas_hamza = g.edge("lucas", "hamza")
        check_arr(
            ben_hammza.properties.temporal.get("prop_float").values(),
            [
                2.0,
                3.0,
                2.0,
                3.0,
            ],
        )
        check_arr(ben_hammza.history(), [1, 2, 3, 4, 5, 6])
        assert ben_hammza.layer_names == ["_default", "test"]
        check_arr(hamza_lucas.history(), [1, 2])
        assert hamza_lucas.layer_names == ["_default"]
        helper_test_props(lucas_hamza.layer("_default"), lucas_props)
