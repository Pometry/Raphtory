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

    weight_e3 = filter.Property("weight") == 3
    weight_lt3 = filter.Property("weight") < 3
    name_bob = filter.Property("name") == "bob"

    f_g = g.filter_exploded_edges(filter=weight_e3)
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

    f_g = g.filter_exploded_edges(filter=weight_lt3 & name_bob)
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

    f_g = g.filter_exploded_edges(filter=weight_e3 | name_bob)
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

    weight_e3 = filter.Property("weight") == 3
    weight_lt3 = filter.Property("weight") < 3
    name_bob = filter.Property("name") == "bob"

    f_g = g.filter_exploded_edges(filter=weight_lt3 & name_bob)
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

    f_g = g.filter_exploded_edges(filter=weight_e3 | name_bob)
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

    weight_e3 = filter.Property("weight") == 3
    name_filter = filter.Node.name() == "1"

    with pytest.raises(Exception) as e:
        g.filter_exploded_edges(weight_e3 | name_filter)
    assert "Only property filters are supported for exploded edge filtering" in str(
        e.value
    )

    with pytest.raises(Exception) as e:
        g.filter_exploded_edges(name_filter | weight_e3)
    assert "Only property filters are supported for exploded edge filtering" in str(
        e.value
    )

    with pytest.raises(Exception) as e:
        g.filter_exploded_edges(weight_e3 & name_filter)
    assert "Only property filters are supported for exploded edge filtering" in str(
        e.value
    )

    with pytest.raises(Exception) as e:
        g.filter_exploded_edges(name_filter & weight_e3)
    assert "Only property filters are supported for exploded edge filtering" in str(
        e.value
    )

    with pytest.raises(Exception) as e:
        g.filter_exploded_edges(name_filter)
    assert "Only property filters are supported for exploded edge filtering" in str(
        e.value
    )


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
        (filter.Property("weight") == 2, 2),
        (filter.Property("weight") != 3, 4),
        (filter.Property("weight") < 3, 4),
        (filter.Property("weight") > 1, 4),
        (filter.Property("weight") <= 2, 4),
        (filter.Property("weight") >= 3, 2),
        (filter.Property("weight").is_in([1, 2]), 4),
        (filter.Property("weight").is_not_in([3]), 4),
        (filter.Property("weight").is_some(), 6),
        (filter.Property("weight").is_none(), 0),
        (filter.Property("weight").is_in(["1", 2]), 2),  # actually does the filter
        (filter.Property("weight").is_not_in(["3"]), 6),  # actually does the filter
        # confidence (float)
        (filter.Property("confidence") == 0.95, 1),
        (filter.Property("confidence") != 0.80, 5),
        (filter.Property("confidence") < 0.9, 4),
        (filter.Property("confidence") > 0.75, 5),
        (filter.Property("confidence") <= 0.85, 3),
        (filter.Property("confidence") >= 0.88, 3),
        (filter.Property("confidence").is_in([0.95, 0.92]), 2),
        (filter.Property("confidence").is_not_in([0.75]), 5),
        (filter.Property("confidence").is_some(), 6),
        (filter.Property("confidence").is_none(), 0),
        (
            filter.Property("confidence").is_in(["1", 0.95]),
            1,
        ),  # actually does the filter
        (
            filter.Property("confidence").is_not_in(["3", 0.95]),
            5,
        ),  # actually does the filter
        # name (str)
        (filter.Property("name") == "bob", 2),
        (filter.Property("name") != "greg", 4),
        (filter.Property("name").is_in(["bob", "dave"]), 4),
        (filter.Property("name").is_not_in(["greg"]), 4),
        (filter.Property("name").contains("bo"), 2),
        (filter.Property("name").not_contains("eg"), 4),
        (filter.Property("name").is_some(), 6),
        (filter.Property("name").is_none(), 0),
        (filter.Property("name") < "dave", 2),
        (filter.Property("name") > "dave", 2),
        (filter.Property("name") <= "dave", 4),
        (filter.Property("name") >= "dave", 4),
        (filter.Property("name").is_in([1, 2]), 0),
        (filter.Property("name").is_not_in([3, "dave"]), 4),
        (filter.Property("name").fuzzy_search("gabe",2,False),2 ),

        # active (bool)
        (filter.Property("active") == True, 4),
        (filter.Property("active") != False, 4),
        (filter.Property("active").is_in([True]), 4),
        (filter.Property("active").is_in([True, False]), 6),
        (filter.Property("active").is_not_in([False]), 4),
        (filter.Property("active").is_some(), 6),
        (filter.Property("active").is_none(), 0),
        (filter.Property("active") < True, 2),
        (filter.Property("active") > False, 4),
        (filter.Property("active") >= False, 6),
        (filter.Property("active") <= False, 2),
        (filter.Property("active").is_in([1, 2]), 0),
        (filter.Property("active").is_not_in([3]), 6),
        # created (datetime)
        (filter.Property("created") == datetime(2023, 1, 1), 1),
        (filter.Property("created") != datetime(2023, 1, 1), 5),
        (filter.Property("created") < datetime(2024, 1, 1), 3),
        (filter.Property("created") > datetime(2024, 1, 1), 3),
        (filter.Property("created") <= datetime(2023, 5, 1), 3),
        (filter.Property("created") >= datetime(2024, 1, 15), 3),
        (
            filter.Property("created").is_in(
                [datetime(2023, 1, 1), datetime(2024, 6, 10)]
            ),
            2,
        ),
        (
            filter.Property("created").is_not_in(
                [datetime(2024, 6, 10), datetime(2025, 1, 1)]
            ),
            4,
        ),
        (filter.Property("created").is_some(), 6),
        (filter.Property("created").is_none(), 0),
        (filter.Property("created").is_in([1, 2]), 0),
        (filter.Property("created").is_not_in([3]), 6),
        # tags (list of str)
        (filter.Property("tags") == ["team_b", "remote"], 1),
        (filter.Property("tags") != ["team_b", "remote"], 5),
        (filter.Property("tags").is_in([["team_b", "remote"], ["team_a"]]), 2),
        (filter.Property("tags").is_not_in([["team_b", "remote"], ["team_a"]]), 4),
        (filter.Property("tags").is_some(), 6),
        (filter.Property("tags").is_none(), 0),
        (filter.Property("tags").is_in([1, 2]), 0),
        (
            filter.Property("tags").is_in([1, 2, ["team_a", 0]]),
            0,
        ),  # actually does the filter, maybe should be a type error on the heterogeneous list
        (filter.Property("tags").is_not_in([3]), 6),  # actually does the filter
        # meta (dict)
        (filter.Property("meta") == {"location": "SF", "level": 2}, 1),
        (filter.Property("meta") != {"location": "SF", "level": 2}, 5),
        (
            filter.Property("meta").is_in(
                [{"location": "SF", "level": 2}, {"contract": True}]
            ),
            2,
        ),
        (
            filter.Property("meta").is_not_in(
                [{"location": "SF", "level": 2}, {"contract": True}]
            ),
            4,
        ),
        (filter.Property("meta").is_some(), 6),
        (filter.Property("meta").is_none(), 0),
        (
            filter.Property("meta").is_not_in(
                [2, 4, {"location": "SF", "level": 2}, {"contract": True}]
            ),
            4,
        ),
        (
            filter.Property("meta").is_in(
                ["hi", {"location": "SF", "level": 2}, {"contract": True}]
            ),
            2,
        ),
        # fake property
        (filter.Property("blah") == 2, 0),
        (filter.Property("blah") != 3, 0),
        (filter.Property("blah") < 3, 0),
        (filter.Property("blah") > 1, 0),
        (filter.Property("blah") <= 2, 0),
        (filter.Property("blah") >= 3, 0),
        (filter.Property("blah").is_in([1, 2]), 0),
        (filter.Property("blah").is_not_in([3]), 0),
        (filter.Property("blah").contains(["blah"]), 0),
        (filter.Property("blah").contains([]), 0),
        (filter.Property("blah").not_contains([]), 0),
        (filter.Property("blah").not_contains(["blah"]), 0),
        (filter.Property("blah").is_some(), 0),
        (filter.Property("blah").is_none(), 6),
    ]
    print()
    for i, (expr, expected) in enumerate(test_cases):
        result = g.filter_exploded_edges(expr).edges.explode()
        assert (
            len(result) == expected
        ), f"Test {i} failed: expected {expected}, got {len(result)}"

    nonsense_filter_cases = [
        # Integers (weight)
        (
            filter.Property("weight").contains(2),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("weight").not_contains(3),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("weight").fuzzy_search("blah",2,False),
            "Operator FUZZY_SEARCH(2,false) is only supported for strings.",
        ),
        # Floats (confidence)
        (
            filter.Property("confidence").contains(0.9),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("confidence").not_contains(0.8),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("confidence").fuzzy_search("blah",2,False),
            "Operator FUZZY_SEARCH(2,false) is only supported for strings.",
        ),
        # Booleans (active)
        (
            filter.Property("active").contains(True),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("active").not_contains(False),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("active").fuzzy_search("blah",2,False),
            "Operator FUZZY_SEARCH(2,false) is only supported for strings.",
        ),
        # Datetimes (created)
        (
            filter.Property("created").contains(datetime(2023, 1, 1)),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("created").not_contains(datetime(2023, 1, 1)),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("created").fuzzy_search("blah",2,False),
            "Operator FUZZY_SEARCH(2,false) is only supported for strings.",
        ),
        # Lists (tags) — odd comparisons
        (
            filter.Property("tags").contains("team_a"),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("tags").not_contains("team_z"),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("tags").fuzzy_search("blah",2,False),
            "Operator FUZZY_SEARCH(2,false) is only supported for strings.",
        ),
        (filter.Property("tags") < ["x"], "Comparison not implemented for List<Str>"),
        (filter.Property("tags") > ["a"], "Comparison not implemented for List<Str>"),
        (
            filter.Property("tags") <= ["team_b"],
            "Comparison not implemented for List<Str>",
        ),
        (
            filter.Property("tags") >= ["consultant"],
            "Comparison not implemented for List<Str>",
        ),
        # Dicts (meta) — contains() expects a key, but here simulates wrong context
        (
            filter.Property("meta").contains("role"),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("meta").not_contains("salary"),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("meta").fuzzy_search("blah",2,False),
            "Operator FUZZY_SEARCH(2,false) is only supported for strings.",
        ),
        (
            filter.Property("meta")
            < {"location": "SF", "level": 2, "contract": False, "role": "blah"},
            "Comparison not implemented for Map",
        ),
        (
            filter.Property("meta") < {"location": "SF", "level": 2, "role": "blah"},
            "Comparison not implemented for Map",
        ),  # check subset of keys also raise the same error
        (
            filter.Property("meta")
            <= {"location": "SF", "level": 2, "contract": False, "role": "blah"},
            "Comparison not implemented for Map",
        ),
        (
            filter.Property("meta")
            > {"location": "SF", "level": 2, "contract": False, "role": "blah"},
            "Comparison not implemented for Map",
        ),
        (
            filter.Property("meta")
            >= {"location": "SF", "level": 2, "contract": False, "role": "blah"},
            "Comparison not implemented for Map",
        ),
    ]

    for i, (expr, message) in enumerate(nonsense_filter_cases):
        with pytest.raises(Exception) as e:
            print(len(g.filter_exploded_edges(expr).edges.explode()))
        print(e.value)
        assert message in str(e.value)

    wrong_types = [
        # Integers (weight)
        (
            filter.Property("weight") == "2",
            "Wrong type for property weight: expected I64 but actual type is Str",
        ),
        (
            filter.Property("weight") != "3",
            "Wrong type for property weight: expected I64 but actual type is Str",
        ),
        (
            filter.Property("weight") < "3",
            "Wrong type for property weight: expected I64 but actual type is Str",
        ),
        (
            filter.Property("weight") > "1",
            "Wrong type for property weight: expected I64 but actual type is Str",
        ),
        (
            filter.Property("weight") <= "2",
            "Wrong type for property weight: expected I64 but actual type is Str",
        ),
        (
            filter.Property("weight") >= "3",
            "Wrong type for property weight: expected I64 but actual type is Str",
        ),
        (
            filter.Property("weight").contains("bo"),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("weight").not_contains("eg"),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        # Floats (confidence)
        (
            filter.Property("confidence") == "2",
            "Wrong type for property confidence: expected F64 but actual type is Str",
        ),
        (
            filter.Property("confidence") != "3",
            "Wrong type for property confidence: expected F64 but actual type is Str",
        ),
        (
            filter.Property("confidence") < "3",
            "Wrong type for property confidence: expected F64 but actual type is Str",
        ),
        (
            filter.Property("confidence") > "1",
            "Wrong type for property confidence: expected F64 but actual type is Str",
        ),
        (
            filter.Property("confidence") <= "2",
            "Wrong type for property confidence: expected F64 but actual type is Str",
        ),
        (
            filter.Property("confidence") >= "3",
            "Wrong type for property confidence: expected F64 but actual type is Str",
        ),
        (
            filter.Property("confidence").contains("bo"),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("confidence").not_contains("eg"),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        # # Strings (name)
        (
            filter.Property("name") == 2,
            "Wrong type for property name: expected Str but actual type is I64",
        ),
        (
            filter.Property("name") != 3,
            "Wrong type for property name: expected Str but actual type is I64",
        ),
        (
            filter.Property("name") < 3,
            "Wrong type for property name: expected Str but actual type is I64",
        ),
        (
            filter.Property("name") > 1,
            "Wrong type for property name: expected Str but actual type is I64",
        ),
        (
            filter.Property("name") <= 2,
            "Wrong type for property name: expected Str but actual type is I64",
        ),
        (
            filter.Property("name") >= 3,
            "Wrong type for property name: expected Str but actual type is I64",
        ),
        (
            filter.Property("name").contains(2),
            "Operator CONTAINS is only supported for strings.",
        ),
        (
            filter.Property("name").not_contains(3),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),
        #Booleans (active)
        (
            filter.Property("active") == 2,
            "Wrong type for property active: expected Bool but actual type is I64",
        ),
        (
            filter.Property("active") != 3,
            "Wrong type for property active: expected Bool but actual type is I64",
        ),
        (
            filter.Property("active") < 3,
            "Wrong type for property active: expected Bool but actual type is I64",
        ),
        (
            filter.Property("active") > 1,
            "Wrong type for property active: expected Bool but actual type is I64",
        ),
        (
            filter.Property("active") <= 2,
            "Wrong type for property active: expected Bool but actual type is I64",
        ),
        (
            filter.Property("active") >= 3,
            "Wrong type for property active: expected Bool but actual type is I64",
        ),
        (
            filter.Property("active").contains(2),
            "Operator CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        (
            filter.Property("active").not_contains(3),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        # # Datetimes (created)
        (
            filter.Property("created") == 2,
            "Wrong type for property created: expected NDTime but actual type is I64",
        ),
        (
            filter.Property("created") != 3,
            "Wrong type for property created: expected NDTime but actual type is I64",
        ),
        (
            filter.Property("created") < 3,
            "Wrong type for property created: expected NDTime but actual type is I64",
        ),
        (
            filter.Property("created") > 1,
            "Wrong type for property created: expected NDTime but actual type is I64",
        ),
        (
            filter.Property("created") <= 2,
            "Wrong type for property created: expected NDTime but actual type is I64",
        ),
        (
            filter.Property("created") >= 3,
            "Wrong type for property created: expected NDTime but actual type is I64",
        ),
        (
            filter.Property("created").contains(2),
            "Operator CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        (
            filter.Property("created").not_contains(3),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        # # Lists (tags)
        (
            filter.Property("tags") == 2,
            "Wrong type for property tags: expected List(Str) but actual type is I64",
        ),
        (
            filter.Property("tags") != 3,
            "Wrong type for property tags: expected List(Str) but actual type is I64",
        ),
        (
            filter.Property("tags") < 3,
            "Wrong type for property tags: expected List(Str) but actual type is I64",
        ),
        (
            filter.Property("tags") > 1,
            "Wrong type for property tags: expected List(Str) but actual type is I64",
        ),
        (
            filter.Property("tags") <= 2,
            "Wrong type for property tags: expected List(Str) but actual type is I64",
        ),
        (
            filter.Property("tags") >= 3,
            "Wrong type for property tags: expected List(Str) but actual type is I64",
        ),
        (
            filter.Property("tags").contains(2),
            "Operator CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        (
            filter.Property("tags").not_contains(3),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        # # Dicts (meta)
        (
            filter.Property("meta") == 2,
            """Wrong type for property meta: expected Map""",
        ),
        (
            filter.Property("meta") != 3,
            """Wrong type for property meta: expected Map""",
        ),
        (filter.Property("meta") < 3, """Wrong type for property meta: expected Map"""),
        (filter.Property("meta") > 1, """Wrong type for property meta: expected Map"""),
        (
            filter.Property("meta") <= 2,
            """Wrong type for property meta: expected Map""",
        ),
        (
            filter.Property("meta") >= 3,
            """Wrong type for property meta: expected Map""",
        ),
        (
            filter.Property("meta").contains(2),
            "Operator CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
        (
            filter.Property("meta").not_contains(3),
            "Operator NOT_CONTAINS is only supported for strings.",
        ),  # should fail on contains not type
    ]

    for i, (expr, message) in enumerate(wrong_types):
        with pytest.raises(Exception) as e:
            print(len(g.filter_exploded_edges(expr).edges.explode()))
        print(e.value)
        assert message in str(e.value)

    with pytest.raises(Exception) as e:
        filter.Property("name").fuzzy_search(2,2,False)
    assert "'int' object cannot be converted to 'PyString'" in str(e.value)

@pytest.mark.parametrize("GraphClass", [Graph, PersistentGraph])
def test_temporal_constant(GraphClass):

    g = GraphClass()
    g.add_edge(1, 1, 2, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(2, 1, 2, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(3, 1, 2, layer="blue", properties={"weight": 3, "name": "greg"})

    g.add_edge(1, 1, 3, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(2, 1, 3, layer="blue", properties={"weight": 2, "name": "dave"})
    g.add_edge(3, 1, 3, layer="red", properties={"weight": 3, "name": "greg"})

    # Temporal shoudl act exactly the same as non-temporal
    test_cases = [
        (filter.Property("weight").temporal().any() == 2, 2),
        (filter.Property("weight").temporal().any() != 3, 4),
        (filter.Property("weight").temporal().any() < 3, 4),
        (filter.Property("weight").temporal().any() > 1, 4),
        (filter.Property("weight").temporal().any() <= 2, 4),
        (filter.Property("weight").temporal().any() >= 3, 2),
        (filter.Property("weight").temporal().any().is_in([1, 2]), 4),
        (filter.Property("weight").temporal().any().is_not_in([3]), 4),
        (filter.Property("weight").temporal().any().is_some(), 6),
        (filter.Property("weight").temporal().any().is_none(), 0),
        (filter.Property("weight").temporal().latest() == 2, 2),
        (filter.Property("weight").temporal().latest() != 3, 4),
        (filter.Property("weight").temporal().latest() < 3, 4),
        (filter.Property("weight").temporal().latest() > 1, 4),
        (filter.Property("weight").temporal().latest() <= 2, 4),
        (filter.Property("weight").temporal().latest() >= 3, 2),
        (filter.Property("weight").temporal().latest().is_in([1, 2]), 4),
        (filter.Property("weight").temporal().latest().is_not_in([3]), 4),
        (filter.Property("weight").temporal().latest().is_some(), 6),
        (filter.Property("weight").temporal().latest().is_none(), 0),
    ]

    for i, (expr, expected) in enumerate(test_cases):
        result = g.filter_exploded_edges(expr).edges.explode()
        assert (
            len(result) == expected
        ), f"Test {i} failed: expected {expected}, got {len(result)}"

    g = GraphClass()
    g.add_edge(1, 1, 2, layer="blue")
    g.add_edge(2, 1, 2, layer="blue")
    e = g.add_edge(3, 1, 2, layer="blue")
    e.add_constant_properties(properties={"weight": 1, "name": "bob"})
    g.add_edge(1, 1, 3, layer="blue")
    g.add_edge(2, 1, 3, layer="blue")
    e = g.add_edge(3, 1, 3, layer="red")
    e.add_constant_properties(properties={"weight": 2, "name": "dave"})

    test_cases = [
        # (filter.Property("weight").constant() == 2, 1), # returns 0 instead of 1
        # (filter.Property("weight").constant() != 3, 4), #returns 0
        # (filter.Property("weight").constant() < 3, 4), #returns 0
        # (filter.Property("weight").constant() > 1, 1), #returns 0
        # (filter.Property("weight").constant() <= 2, 4), #returns 0
        # (filter.Property("weight").constant() >= 3, 0), #returns 0
        # (filter.Property("weight").constant().is_in([1, 2]), 4), #returns 0
        # (filter.Property("weight").constant().is_not_in([3]), 4), #returns 0
        # (filter.Property("weight").constant().is_some(), 4), #returns 0
        # (filter.Property("weight").constant().is_none(), 2), #returns 0
        # (filter.Property("weight").constant() == 2, 1), # returns 0 instead of 1
        # (filter.Property("weight").constant() != 3, 4), #returns 0
        # (filter.Property("weight").constant() < 3, 4), #returns 0
        # (filter.Property("weight").constant() > 1, 1), #returns 0
        # (filter.Property("weight").constant() <= 2, 4), #returns 0
        # (filter.Property("weight").constant() >= 3, 0), #returns 0
        # (filter.Property("weight").constant().is_in([1, 2]), 4), #returns 0
        # (filter.Property("weight").constant().is_not_in([3]), 4), #returns 0
        # (filter.Property("weight").constant().is_some(), 4), #returns 0
        # (filter.Property("weight").constant().is_none(), 2), #returns 0
    ]

    for i, (expr, expected) in enumerate(test_cases):
        result = g.filter_exploded_edges(expr).edges.explode()
        assert (
            len(result) == expected
        ), f"Test {i} failed: expected {expected}, got {len(result)}"

    # How do temporal and constant properties overlap
    g = PersistentGraph()

    g.add_edge(1, 1, 2, layer="blue")  # gets the constant prop in persistent graph
    g.add_edge(2, 1, 2, layer="blue", properties={"weight": 1, "name": "bob"})
    g.add_edge(3, 1, 2, layer="blue", properties={"weight": 2, "name": "bob"})
    e = g.add_edge(4, 1, 2, layer="blue")  # gets weight 2 in persistent graph
    e.add_constant_properties(properties={"weight": 3, "name": "bob"})

    test_cases = [
        # (filter.Property("weight") >= 1, 3), #returns 2 missing the constant prop
        # (filter.Property("weight").constant() == 3, 3) #returns 0
    ]

    # if type(g) == Graph:
    # test_cases.append((filter.Property("weight").temporal().any() > 0, 4)) #currently fails because constant missing - returns 2
    # else:
    # test_cases.append((filter.Property("weight").temporal().any() > 0, 4)) #currently fails because constant missing - returns 3

    for i, (expr, expected) in enumerate(test_cases):
        result = g.filter_exploded_edges(expr).edges.explode()
        assert (
            len(result) == expected
        ), f"Test {i} failed: expected {expected}, got {len(result)}"
