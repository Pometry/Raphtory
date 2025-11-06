from raphtory import Graph, PersistentGraph
from raphtory import filter
import pytest
from datetime import datetime


@pytest.mark.parametrize("GraphClass", [Graph, PersistentGraph])
def test_graph(GraphClass):

    g = GraphClass()
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(2, 1, 2, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(3, 1, 2, layer="blue", properties={"weight": 3, "name": "greg"})

    g.add_edge(1, 1, 3, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(2, 1, 3, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(3, 1, 3, layer="red", properties={"weight": 3, "name": "greg"})

    weight_e3 = filter.ExplodedEdge.property("weight") == 3
    weight_lt3 = filter.ExplodedEdge.property("weight") < 3
    name_bob = filter.ExplodedEdge.property("name") == "bob"

    f_g = g.filter(filter=weight_e3)
    e1 = f_g.edge(1, 2)
    e2 = f_g.edge(1, 3)

    if type(g) == Graph:
        assert e1.deletions() == []
        assert e2.deletions() == []
    else:
        assert e1.deletions() == [1, 2]
        assert e2.deletions() == [1, 2]

    assert list(e1.history()) == [3]
    assert list(e2.history()) == [3]

    # assert e2.layer_names == ["red"] returning red blue for PersistentGraph which feels wrong?

    assert e1.properties.temporal.get("weight").items() == [(3, 3)]
    assert e2.properties.temporal.get("weight").items() == [(3, 3)]

    f_g = g.filter(filter=weight_lt3 & name_bob)
    e1 = f_g.edge(1, 2)
    e2 = f_g.edge(1, 3)

    if type(g) == Graph:
        assert e1.deletions() == []
        assert e2.deletions() == []
    else:
        assert e1.deletions() == [2, 3]
        assert e2.deletions() == [2, 3]

    assert list(e1.history()) == [1]
    assert list(e2.history()) == [1]

    # assert e2.layer_names == ["blue"] returning red blue for PersistentGraph which feels wrong?

    assert e1.properties.temporal.get("weight").items() == [(1, 1)]
    assert e2.properties.temporal.get("weight").items() == [(1, 1)]

    f_g = g.filter(filter=weight_e3 | name_bob)
    e1 = f_g.edge(1, 2)
    e2 = f_g.edge(1, 3)

    if type(g) == Graph:
        assert e1.deletions() == []
        assert e2.deletions() == []
    else:
        assert e1.deletions() == [2]
        assert e2.deletions() == [2]

    assert list(e1.history()) == [1, 3]
    assert list(e2.history()) == [1, 3]

    assert e2.layer_names == ["blue", "red"]

    assert e1.properties.temporal.get("weight").items() == [(1, 1), (3, 3)]
    assert e2.properties.temporal.get("weight").items() == [(1, 1), (3, 3)]


@pytest.mark.parametrize("GraphClass", [Graph, PersistentGraph])
def test_same_time_event(GraphClass):
    g = GraphClass()
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 3, "name": "greg"})

    g.add_edge(1, 1, 3, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(1, 1, 3, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(1, 1, 3, layer="red", properties={"weight": 3, "name": "greg"})

    weight_e3 = filter.ExplodedEdge.property("weight") == 3
    weight_lt3 = filter.ExplodedEdge.property("weight") < 3
    name_bob = filter.ExplodedEdge.property("name") == "bob"

    f_g = g.filter(filter=weight_lt3 & name_bob)
    e1 = f_g.edge(1, 2)
    e2 = f_g.edge(1, 3)

    if type(g) == Graph:
        assert e1.deletions() == []
        assert e2.deletions() == []
    else:
        assert e1.deletions() == [1, 1]
        assert e2.deletions() == [1, 1]

    assert list(e1.history()) == [1]
    assert list(e2.history()) == [1]

    # assert e2.layer_names == ["blue"] returning red blue which seems wrong

    assert e1.properties.temporal.get("weight").items() == [(1, 1)]
    assert e2.properties.temporal.get("weight").items() == [(1, 1)]

    f_g = g.filter(filter=weight_e3 | name_bob)
    e1 = f_g.edge(1, 2)
    e2 = f_g.edge(1, 3)

    if type(g) == Graph:
        assert e1.deletions() == []
        assert e2.deletions() == []
    else:
        assert e1.deletions() == [1]
        assert e2.deletions() == [1]

    assert list(e1.history()) == [1, 1]
    assert list(e2.history()) == [1, 1]

    assert e2.layer_names == ["blue", "red"]

    assert e1.properties.temporal.get("weight").items() == [(1, 1), (1, 3)]
    assert e2.properties.temporal.get("weight").items() == [(1, 1), (1, 3)]


@pytest.mark.parametrize("GraphClass", [Graph, PersistentGraph])
def test_with_edge_node_filter(GraphClass):
    g = GraphClass()
    g.add_edge(
        timestamp=1, src=1, dst=2, layer="blue", properties={"weight": 1, "name": "bob"}
    )
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 3, "name": "greg"})

    g.add_edge(1, 1, 3, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(1, 1, 3, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(1, 1, 3, layer="red", properties={"weight": 3, "name": "greg"})

    weight_e3 = filter.ExplodedEdge.property("weight") == 3
    name_filter = filter.Node.name() == "2"

    actual = [
        (edge.src.name, edge.dst.name)
        for edge in g.filter(weight_e3 | name_filter).edges.explode()
    ]
    expected = [("1", "2"), ("1", "2"), ("1", "2"), ("1", "3"), ("1", "3"), ("1", "3")]
    assert sorted(actual) == sorted(expected)

    actual = [
        (edge.src.name, edge.dst.name)
        for edge in g.filter(name_filter | weight_e3).edges.explode()
    ]
    expected = [("1", "2"), ("1", "2"), ("1", "2"), ("1", "3"), ("1", "3"), ("1", "3")]
    assert sorted(actual) == sorted(expected)

    actual = [
        (edge.src.name, edge.dst.name)
        for edge in g.filter(weight_e3 & name_filter).edges.explode()
    ]
    expected = []
    assert sorted(actual) == sorted(expected)

    actual = [
        (edge.src.name, edge.dst.name)
        for edge in g.filter(name_filter & weight_e3).edges.explode()
    ]
    expected = []
    assert sorted(actual) == sorted(expected)

    actual = [
        (edge.src.name, edge.dst.name) for edge in g.filter(name_filter).edges.explode()
    ]
    expected = []
    assert sorted(actual) == sorted(expected)


@pytest.mark.parametrize("GraphClass", [Graph, PersistentGraph])
def test_all_property_types(GraphClass):

    g = GraphClass()

    g.add_edge(
        timestamp=1,
        src=1,
        dst=2,
        layer="blue",
        properties={
            "weight": 1,
            "confidence": 0.95,
            "name": "bob",
            "active": True,
            "created": datetime(2023, 1, 1),
            "tags": ["friend", "colleague"],
            "meta": {"role": "engineer"},
        },
    )

    g.add_edge(
        1,
        1,
        2,
        layer="blue",
        properties={
            "weight": 2,
            "confidence": 0.85,
            "name": "dave",
            "active": False,
            "created": datetime(2023, 5, 1),
            "tags": ["project_x"],
            "meta": {"role": "manager"},
        },
    )

    g.add_edge(
        1,
        1,
        2,
        layer="blue",
        properties={
            "weight": 3,
            "confidence": 0.75,
            "name": "greg",
            "active": True,
            "created": datetime(2024, 1, 15),
            "tags": [],
            "meta": {},
        },
    )

    g.add_edge(
        1,
        1,
        3,
        layer="blue",
        properties={
            "weight": 1,
            "confidence": 0.92,
            "name": "bob",
            "active": True,
            "created": datetime(2023, 3, 14),
            "tags": ["team_a"],
            "meta": {"location": "NYC"},
        },
    )

    g.add_edge(
        1,
        1,
        3,
        layer="blue",
        properties={
            "weight": 2,
            "confidence": 0.88,
            "name": "dave",
            "active": False,
            "created": datetime(2024, 6, 10),
            "tags": ["team_b", "remote"],
            "meta": {"location": "SF", "level": 2},
        },
    )

    g.add_edge(
        1,
        1,
        3,
        layer="red",
        properties={
            "weight": 3,
            "confidence": 0.80,
            "name": "greg",
            "active": True,
            "created": datetime(2025, 1, 1),
            "tags": ["consultant"],
            "meta": {"contract": True},
        },
    )

    test_cases = [
        # weight (int)
        (filter.ExplodedEdge.property("weight") == 2, 2),
        (filter.ExplodedEdge.property("weight") != 3, 4),
        (filter.ExplodedEdge.property("weight") < 3, 4),
        (filter.ExplodedEdge.property("weight") > 1, 4),
        (filter.ExplodedEdge.property("weight") <= 2, 4),
        (filter.ExplodedEdge.property("weight") >= 3, 2),
        (filter.ExplodedEdge.property("weight").is_in([1, 2]), 4),
        (filter.ExplodedEdge.property("weight").is_not_in([3]), 4),
        (filter.ExplodedEdge.property("weight").is_some(), 6),
        (filter.ExplodedEdge.property("weight").is_none(), 0),
        (
            filter.ExplodedEdge.property("weight").is_in(["1", 2]),
            2,
        ),  # actually does the filter
        (
            filter.ExplodedEdge.property("weight").is_not_in(["3"]),
            6,
        ),  # actually does the filter
        # confidence (float)
        (filter.ExplodedEdge.property("confidence") == 0.95, 1),
        (filter.ExplodedEdge.property("confidence") != 0.80, 5),
        (filter.ExplodedEdge.property("confidence") < 0.9, 4),
        (filter.ExplodedEdge.property("confidence") > 0.75, 5),
        (filter.ExplodedEdge.property("confidence") <= 0.85, 3),
        (filter.ExplodedEdge.property("confidence") >= 0.88, 3),
        (filter.ExplodedEdge.property("confidence").is_in([0.95, 0.92]), 2),
        (filter.ExplodedEdge.property("confidence").is_not_in([0.75]), 5),
        (filter.ExplodedEdge.property("confidence").is_some(), 6),
        (filter.ExplodedEdge.property("confidence").is_none(), 0),
        (
            filter.ExplodedEdge.property("confidence").is_in(["1", 0.95]),
            1,
        ),  # actually does the filter
        (
            filter.ExplodedEdge.property("confidence").is_not_in(["3", 0.95]),
            5,
        ),  # actually does the filter
        # name (str)
        (filter.ExplodedEdge.property("name") == "bob", 2),
        (filter.ExplodedEdge.property("name") != "greg", 4),
        (filter.ExplodedEdge.property("name").is_in(["bob", "dave"]), 4),
        (filter.ExplodedEdge.property("name").is_not_in(["greg"]), 4),
        (filter.ExplodedEdge.property("name").contains("bo"), 2),
        (filter.ExplodedEdge.property("name").not_contains("eg"), 4),
        (filter.ExplodedEdge.property("name").is_some(), 6),
        (filter.ExplodedEdge.property("name").is_none(), 0),
        (filter.ExplodedEdge.property("name") < "dave", 2),
        (filter.ExplodedEdge.property("name") > "dave", 2),
        (filter.ExplodedEdge.property("name") <= "dave", 4),
        (filter.ExplodedEdge.property("name") >= "dave", 4),
        (filter.ExplodedEdge.property("name").is_in([1, 2]), 0),
        (filter.ExplodedEdge.property("name").is_not_in([3, "dave"]), 4),
        (filter.ExplodedEdge.property("name").fuzzy_search("gabe", 2, False), 2),
        # active (bool)
        (filter.ExplodedEdge.property("active") == True, 4),
        (filter.ExplodedEdge.property("active") != False, 4),
        (filter.ExplodedEdge.property("active").is_in([True]), 4),
        (filter.ExplodedEdge.property("active").is_in([True, False]), 6),
        (filter.ExplodedEdge.property("active").is_not_in([False]), 4),
        (filter.ExplodedEdge.property("active").is_some(), 6),
        (filter.ExplodedEdge.property("active").is_none(), 0),
        (filter.ExplodedEdge.property("active") < True, 2),
        (filter.ExplodedEdge.property("active") > False, 4),
        (filter.ExplodedEdge.property("active") >= False, 6),
        (filter.ExplodedEdge.property("active") <= False, 2),
        (filter.ExplodedEdge.property("active").is_in([1, 2]), 0),
        (filter.ExplodedEdge.property("active").is_not_in([3]), 6),
        # created (datetime)
        (filter.ExplodedEdge.property("created") == datetime(2023, 1, 1), 1),
        (filter.ExplodedEdge.property("created") != datetime(2023, 1, 1), 5),
        (filter.ExplodedEdge.property("created") < datetime(2024, 1, 1), 3),
        (filter.ExplodedEdge.property("created") > datetime(2024, 1, 1), 3),
        (filter.ExplodedEdge.property("created") <= datetime(2023, 5, 1), 3),
        (filter.ExplodedEdge.property("created") >= datetime(2024, 1, 15), 3),
        (
            filter.ExplodedEdge.property("created").is_in(
                [datetime(2023, 1, 1), datetime(2024, 6, 10)]
            ),
            2,
        ),
        (
            filter.ExplodedEdge.property("created").is_not_in(
                [datetime(2024, 6, 10), datetime(2025, 1, 1)]
            ),
            4,
        ),
        (filter.ExplodedEdge.property("created").is_some(), 6),
        (filter.ExplodedEdge.property("created").is_none(), 0),
        (filter.ExplodedEdge.property("created").is_in([1, 2]), 0),
        (filter.ExplodedEdge.property("created").is_not_in([3]), 6),
        # tags (list of str)
        (filter.ExplodedEdge.property("tags") == ["team_b", "remote"], 1),
        (filter.ExplodedEdge.property("tags") != ["team_b", "remote"], 5),
        (
            filter.ExplodedEdge.property("tags").is_in(
                [["team_b", "remote"], ["team_a"]]
            ),
            2,
        ),
        (
            filter.ExplodedEdge.property("tags").is_not_in(
                [["team_b", "remote"], ["team_a"]]
            ),
            4,
        ),
        (filter.ExplodedEdge.property("tags").is_some(), 6),
        (filter.ExplodedEdge.property("tags").is_none(), 0),
        (filter.ExplodedEdge.property("tags").is_in([1, 2]), 0),
        (
            filter.ExplodedEdge.property("tags").is_in([1, 2, ["team_a", 0]]),
            0,
        ),  # actually does the filter, maybe should be a type error on the heterogeneous list
        (
            filter.ExplodedEdge.property("tags").is_not_in([3]),
            6,
        ),  # actually does the filter
        # meta (dict)
        (filter.ExplodedEdge.property("meta") == {"location": "SF", "level": 2}, 1),
        (filter.ExplodedEdge.property("meta") != {"location": "SF", "level": 2}, 5),
        (
            filter.ExplodedEdge.property("meta").is_in(
                [{"location": "SF", "level": 2}, {"contract": True}]
            ),
            2,
        ),
        (
            filter.ExplodedEdge.property("meta").is_not_in(
                [{"location": "SF", "level": 2}, {"contract": True}]
            ),
            4,
        ),
        (filter.ExplodedEdge.property("meta").is_some(), 6),
        (filter.ExplodedEdge.property("meta").is_none(), 0),
        (
            filter.ExplodedEdge.property("meta").is_not_in(
                [2, 4, {"location": "SF", "level": 2}, {"contract": True}]
            ),
            4,
        ),
        (
            filter.ExplodedEdge.property("meta").is_in(
                ["hi", {"location": "SF", "level": 2}, {"contract": True}]
            ),
            2,
        ),
    ]
    print()
    for i, (expr, expected) in enumerate(test_cases):
        result = g.filter(expr).edges.explode()
        assert (
            len(result) == expected
        ), f"Test {i} failed: expected {expected}, got {len(result)}"

    nonsense_filter_cases = [
        # Integers (weight)
        (
            filter.ExplodedEdge.property("weight").contains(2),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("weight").not_contains(3),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("weight").fuzzy_search("blah", 2, False),
            "Operator FUZZY_SEARCH(2,false) is only supported for strings.",
        ),
        # Floats (confidence)
        (
            filter.ExplodedEdge.property("confidence").contains(0.9),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("confidence").not_contains(0.8),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("confidence").fuzzy_search("blah", 2, False),
            "Operator FUZZY_SEARCH(2,false) is only supported for strings.",
        ),
        # Booleans (active)
        (
            filter.ExplodedEdge.property("active").contains(True),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("active").not_contains(False),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("active").fuzzy_search("blah", 2, False),
            "Operator FUZZY_SEARCH(2,false) is only supported for strings.",
        ),
        # Datetimes (created)
        (
            filter.ExplodedEdge.property("created").contains(datetime(2023, 1, 1)),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("created").not_contains(datetime(2023, 1, 1)),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("created").fuzzy_search("blah", 2, False),
            "Operator FUZZY_SEARCH(2,false) is only supported for strings.",
        ),
        # Lists (tags) — odd comparisons
        (
            filter.ExplodedEdge.property("tags").contains("team_a"),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("tags").not_contains("team_z"),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("tags").fuzzy_search("blah", 2, False),
            "Operator FUZZY_SEARCH(2,false) is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("tags") < ["x"],
            "Comparison not implemented for List<Str>",
        ),
        (
            filter.ExplodedEdge.property("tags") > ["a"],
            "Comparison not implemented for List<Str>",
        ),
        (
            filter.ExplodedEdge.property("tags") <= ["team_b"],
            "Comparison not implemented for List<Str>",
        ),
        (
            filter.ExplodedEdge.property("tags") >= ["consultant"],
            "Comparison not implemented for List<Str>",
        ),
        # Dicts (meta) — contains() expects a key, but here simulates wrong context
        (
            filter.ExplodedEdge.property("meta").contains("role"),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("meta").not_contains("salary"),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("meta").fuzzy_search("blah", 2, False),
            "Operator FUZZY_SEARCH(2,false) is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("meta")
            < {"location": "SF", "level": 2, "contract": False, "role": "blah"},
            "Comparison not implemented for Map",
        ),
        (
            filter.ExplodedEdge.property("meta")
            < {"location": "SF", "level": 2, "role": "blah"},
            "Comparison not implemented for Map",
        ),  # check subset of keys also raise the same error
        (
            filter.ExplodedEdge.property("meta")
            <= {"location": "SF", "level": 2, "contract": False, "role": "blah"},
            "Comparison not implemented for Map",
        ),
        (
            filter.ExplodedEdge.property("meta")
            > {"location": "SF", "level": 2, "contract": False, "role": "blah"},
            "Comparison not implemented for Map",
        ),
        (
            filter.ExplodedEdge.property("meta")
            >= {"location": "SF", "level": 2, "contract": False, "role": "blah"},
            "Comparison not implemented for Map",
        ),
    ]

    for i, (expr, message) in enumerate(nonsense_filter_cases):
        with pytest.raises(Exception) as e:
            print(len(g.filter(expr).edges.explode()))
        print(e.value)
        assert message in str(e.value)

    wrong_types = [
        # Integers (weight)
        (
            filter.ExplodedEdge.property("weight") == "2",
            "Wrong type for property weight: expected I64 but actual type is Str",
        ),
        (
            filter.ExplodedEdge.property("weight") != "3",
            "Wrong type for property weight: expected I64 but actual type is Str",
        ),
        (
            filter.ExplodedEdge.property("weight") < "3",
            "Wrong type for property weight: expected I64 but actual type is Str",
        ),
        (
            filter.ExplodedEdge.property("weight") > "1",
            "Wrong type for property weight: expected I64 but actual type is Str",
        ),
        (
            filter.ExplodedEdge.property("weight") <= "2",
            "Wrong type for property weight: expected I64 but actual type is Str",
        ),
        (
            filter.ExplodedEdge.property("weight") >= "3",
            "Wrong type for property weight: expected I64 but actual type is Str",
        ),
        (
            filter.ExplodedEdge.property("weight").contains("bo"),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("weight").not_contains("eg"),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        # Floats (confidence)
        (
            filter.ExplodedEdge.property("confidence") == "2",
            "Wrong type for property confidence: expected F64 but actual type is Str",
        ),
        (
            filter.ExplodedEdge.property("confidence") != "3",
            "Wrong type for property confidence: expected F64 but actual type is Str",
        ),
        (
            filter.ExplodedEdge.property("confidence") < "3",
            "Wrong type for property confidence: expected F64 but actual type is Str",
        ),
        (
            filter.ExplodedEdge.property("confidence") > "1",
            "Wrong type for property confidence: expected F64 but actual type is Str",
        ),
        (
            filter.ExplodedEdge.property("confidence") <= "2",
            "Wrong type for property confidence: expected F64 but actual type is Str",
        ),
        (
            filter.ExplodedEdge.property("confidence") >= "3",
            "Wrong type for property confidence: expected F64 but actual type is Str",
        ),
        (
            filter.ExplodedEdge.property("confidence").contains("bo"),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("confidence").not_contains("eg"),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        # # Strings (name)
        (
            filter.ExplodedEdge.property("name") == 2,
            "Wrong type for property name: expected Str but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("name") != 3,
            "Wrong type for property name: expected Str but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("name") < 3,
            "Wrong type for property name: expected Str but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("name") > 1,
            "Wrong type for property name: expected Str but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("name") <= 2,
            "Wrong type for property name: expected Str but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("name") >= 3,
            "Wrong type for property name: expected Str but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("name").contains(2),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.ExplodedEdge.property("name").not_contains(3),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        # Booleans (active)
        (
            filter.ExplodedEdge.property("active") == 2,
            "Wrong type for property active: expected Bool but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("active") != 3,
            "Wrong type for property active: expected Bool but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("active") < 3,
            "Wrong type for property active: expected Bool but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("active") > 1,
            "Wrong type for property active: expected Bool but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("active") <= 2,
            "Wrong type for property active: expected Bool but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("active") >= 3,
            "Wrong type for property active: expected Bool but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("active").contains(2),
            "Operator CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        (
            filter.ExplodedEdge.property("active").not_contains(3),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        # # Datetimes (created)
        (
            filter.ExplodedEdge.property("created") == 2,
            "Wrong type for property created: expected NDTime but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("created") != 3,
            "Wrong type for property created: expected NDTime but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("created") < 3,
            "Wrong type for property created: expected NDTime but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("created") > 1,
            "Wrong type for property created: expected NDTime but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("created") <= 2,
            "Wrong type for property created: expected NDTime but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("created") >= 3,
            "Wrong type for property created: expected NDTime but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("created").contains(2),
            "Operator CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        (
            filter.ExplodedEdge.property("created").not_contains(3),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        # # Lists (tags)
        (
            filter.ExplodedEdge.property("tags") == 2,
            "Wrong type for property tags: expected List(Str) but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("tags") != 3,
            "Wrong type for property tags: expected List(Str) but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("tags") < 3,
            "Wrong type for property tags: expected List(Str) but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("tags") > 1,
            "Wrong type for property tags: expected List(Str) but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("tags") <= 2,
            "Wrong type for property tags: expected List(Str) but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("tags") >= 3,
            "Wrong type for property tags: expected List(Str) but actual type is I64",
        ),
        (
            filter.ExplodedEdge.property("tags").contains(2),
            "Operator CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        (
            filter.ExplodedEdge.property("tags").not_contains(3),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        # # Dicts (meta)
        (
            filter.ExplodedEdge.property("meta") == 2,
            """Wrong type for property meta: expected Map""",
        ),
        (
            filter.ExplodedEdge.property("meta") != 3,
            """Wrong type for property meta: expected Map""",
        ),
        (
            filter.ExplodedEdge.property("meta") < 3,
            """Wrong type for property meta: expected Map""",
        ),
        (
            filter.ExplodedEdge.property("meta") > 1,
            """Wrong type for property meta: expected Map""",
        ),
        (
            filter.ExplodedEdge.property("meta") <= 2,
            """Wrong type for property meta: expected Map""",
        ),
        (
            filter.ExplodedEdge.property("meta") >= 3,
            """Wrong type for property meta: expected Map""",
        ),
        (
            filter.ExplodedEdge.property("meta").contains(2),
            "Operator CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        (
            filter.ExplodedEdge.property("meta").not_contains(3),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
    ]

    for i, (expr, message) in enumerate(wrong_types):
        with pytest.raises(Exception) as e:
            print(len(g.filter(expr).edges.explode()))
        print(e.value)
        assert message in str(e.value)

    with pytest.raises(Exception) as e:
        filter.ExplodedEdge.property("name").fuzzy_search(2, 2, False)
    assert "'int' object cannot be converted to 'PyString'" in str(e.value)

    missing_prop = [
        (filter.ExplodedEdge.property("blah") == 2),
        (filter.ExplodedEdge.property("blah") != 3),
        (filter.ExplodedEdge.property("blah") < 3),
        (filter.ExplodedEdge.property("blah") > 1),
        (filter.ExplodedEdge.property("blah") <= 2),
        (filter.ExplodedEdge.property("blah") >= 3),
        (filter.ExplodedEdge.property("blah").is_in([1, 2])),
        (filter.ExplodedEdge.property("blah").is_not_in([3])),
        (filter.ExplodedEdge.property("blah").contains(["blah"])),
        (filter.ExplodedEdge.property("blah").contains([])),
        (filter.ExplodedEdge.property("blah").not_contains([])),
        (filter.ExplodedEdge.property("blah").not_contains(["blah"])),
        (filter.ExplodedEdge.property("blah").is_some()),
        (filter.ExplodedEdge.property("blah").is_none()),
    ]

    for expr in missing_prop:
        with pytest.raises(Exception) as e:
            # force evaluation so the exception surfaces here
            _ = g.filter(expr).edges.explode()
        assert "Property blah does not exist" in str(e.value)


@pytest.mark.parametrize("GraphClass", [Graph, PersistentGraph])
def test_temporal_constant(GraphClass):

    g = GraphClass()
    g.add_edge(
        1,
        1,
        2,
        layer="blue",
        properties={"weight": 1, "name": "bob", "p20": "Gold_ship"},
    )
    g.add_edge(
        2,
        1,
        2,
        layer="blue",
        properties={"weight": 2, "name": "dave", "p20": "Gold_ship"},
    )
    g.add_edge(3, 1, 2, layer="blue", properties={"weight": 3, "name": "greg"})

    g.add_edge(
        1,
        1,
        3,
        layer="blue",
        properties={"weight": 1, "name": "bob", "p20": "Old_boat"},
    )
    g.add_edge(
        2,
        1,
        3,
        layer="blue",
        properties={"weight": 2, "name": "dave", "p20": "Gold_ship"},
    )
    g.add_edge(3, 1, 3, layer="red", properties={"weight": 3, "name": "greg"})

    # Temporal shoudl act exactly the same as non-temporal
    test_cases = [
        (filter.ExplodedEdge.property("weight").temporal().any() == 2, 2),
        (filter.ExplodedEdge.property("weight").temporal().any() != 3, 4),
        (filter.ExplodedEdge.property("weight").temporal().any() < 3, 4),
        (filter.ExplodedEdge.property("weight").temporal().any() > 1, 4),
        (filter.ExplodedEdge.property("weight").temporal().any() <= 2, 4),
        (filter.ExplodedEdge.property("weight").temporal().any() >= 3, 2),
        (filter.ExplodedEdge.property("weight").temporal().any().is_in([1, 2]), 4),
        (filter.ExplodedEdge.property("weight").temporal().any().is_not_in([3]), 4),
        (filter.ExplodedEdge.property("weight").temporal().any().is_some(), 6),
        (filter.ExplodedEdge.property("weight").temporal().any().is_none(), 0),
        (filter.ExplodedEdge.property("weight").temporal().last() == 2, 2),
        (filter.ExplodedEdge.property("weight").temporal().last() != 3, 4),
        (filter.ExplodedEdge.property("weight").temporal().last() < 3, 4),
        (filter.ExplodedEdge.property("weight").temporal().last() > 1, 4),
        (filter.ExplodedEdge.property("weight").temporal().last() <= 2, 4),
        (filter.ExplodedEdge.property("weight").temporal().last() >= 3, 2),
        (filter.ExplodedEdge.property("weight").temporal().last().is_in([1, 2]), 4),
        (filter.ExplodedEdge.property("weight").temporal().last().is_not_in([3]), 4),
        (filter.ExplodedEdge.property("weight").temporal().last().is_some(), 6),
        (filter.ExplodedEdge.property("weight").temporal().last().is_none(), 0),
        (filter.ExplodedEdge.property("p20").temporal().first().starts_with("Old"), 1),
        (filter.ExplodedEdge.property("p20").temporal().first().ends_with("boat"), 1),
    ]

    for i, (expr, expected) in enumerate(test_cases):
        result = g.filter(expr).edges.explode()
        assert (
            len(result) == expected
        ), f"Test {i} failed: expected {expected}, got {len(result)}"

    g = GraphClass()
    g.add_edge(1, 1, 2, layer="blue")
    g.add_edge(2, 1, 2, layer="blue")
    e = g.add_edge(3, 1, 2, layer="blue")
    e.add_metadata(metadata={"weight": 1, "name": "bob"})
    g.add_edge(1, 1, 3, layer="blue")
    g.add_edge(2, 1, 3, layer="blue")
    e = g.add_edge(3, 1, 3, layer="blue")
    e.add_metadata(metadata={"weight": 2, "name": "dave"})

    test_cases = [
        (filter.Edge.metadata("weight") == 2, 3),
        (filter.Edge.metadata("weight") != 3, 6),
        (filter.Edge.metadata("weight") < 3, 6),
        (filter.Edge.metadata("weight") > 1, 3),
        (filter.Edge.metadata("weight") <= 2, 6),
        (filter.Edge.metadata("weight") >= 3, 0),
        (filter.Edge.metadata("weight").is_in([1, 2]), 6),
        (filter.Edge.metadata("weight").is_not_in([3]), 6),
        (filter.Edge.metadata("weight").is_some(), 6),
        (filter.Edge.metadata("weight").is_none(), 0),
    ]

    for i, (expr, expected) in enumerate(test_cases):
        result = g.filter(expr).edges.explode()
        print(g.edges.explode().metadata.get("weight"))
        assert (
            len(result) == expected
        ), f"Test {i} failed: expected {expected}, got {len(result)}"
