"""`select` asks its question of the items of the collection it is called on:
once per edge on `edges`, once per exploded edge on `edges { explode }`. The
kind of the predicate plays no part, and a view used as a predicate never
narrows the exploded edges of a kept edge; the view form of the window does.
"""

from utils import run_group_graphql_test
from raphtory import Graph


def create_graph() -> Graph:
    # a -> b at t=1 [x] w=1 · a -> b at t=7 [y] w=7 · b -> c at t=2 [x] w=2
    graph = Graph()
    graph.add_edge(1, "a", "b", properties={"w": 1}, layer="x")
    graph.add_edge(7, "a", "b", properties={"w": 7}, layer="y")
    graph.add_edge(2, "b", "c", properties={"w": 2}, layer="x")
    return graph


EVENTS = "list { src { name } dst { name } time { timestamp } }"
WINDOW = "{ view: [{ window: { start: 0, end: 5 } }] }"
UPDATE_IS_SEVEN = (
    '{ explodedEdge: { cmp: { op: EQ, lhs: { read: { property: { name: "w" } } }, '
    "rhs: { const: { i64: 7 } } } } }"
)
LATEST_IS_SEVEN = (
    '{ edge: { cmp: { op: EQ, lhs: { read: { property: { name: "w" } } }, '
    "rhs: { const: { i64: 7 } } } } }"
)


def event(src, dst, t):
    return {"src": {"name": src}, "dst": {"name": dst}, "time": {"timestamp": t}}


AB1, AB7, BC2 = event("a", "b", 1), event("a", "b", 7), event("b", "c", 2)


def test_select_asks_about_the_items_of_the_collection():
    cases = []
    for expr, on_edges, on_exploded in [
        # a view predicate keeps the edges active in it, with all their updates
        (WINDOW, [AB1, AB7, BC2], [AB1, BC2]),
        # an exploded-edge predicate keeps the edges with such an update
        (UPDATE_IS_SEVEN, [AB1, AB7], [AB7]),
        # an edge predicate reads the latest value on edges and each update's
        # own value on exploded edges
        (LATEST_IS_SEVEN, [AB1, AB7], [AB7]),
    ]:
        on_edges_query = f"""
        {{ graph(path: "g") {{ edges {{ select(expr: {expr}) {{ explode {{ {EVENTS} }} }} }} }} }}
        """
        cases.append(
            (
                on_edges_query,
                {"graph": {"edges": {"select": {"explode": {"list": on_edges}}}}},
            )
        )
        on_exploded_query = f"""
        {{ graph(path: "g") {{ edges {{ explode {{ select(expr: {expr}) {{ {EVENTS} }} }} }} }} }}
        """
        cases.append(
            (
                on_exploded_query,
                {"graph": {"edges": {"explode": {"select": {"list": on_exploded}}}}},
            )
        )
    # the window as a view narrows what the whole right-hand side sees
    windowed_query = f"""
    {{ graph(path: "g") {{ window(start: 0, end: 5) {{ edges {{ explode {{ {EVENTS} }} }} }} }} }}
    """
    cases.append(
        (
            windowed_query,
            {"graph": {"window": {"edges": {"explode": {"list": [AB1, BC2]}}}}},
        )
    )
    run_group_graphql_test(cases, create_graph())
