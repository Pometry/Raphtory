import json
import os
import random
import shutil
import tempfile
from datetime import datetime, timedelta

from raphtory import graphql, PersistentGraph, Graph

## This is the test data for the UI tests so bare in mind they might fail if this file is changed


def random_created_at():
    today = datetime.now()
    days_ago = random.randint(0, 365 * 10)
    created = today - timedelta(days=days_ago)
    return int(created.timestamp() * 1000)


def setup_large_graph(graph):
    graph.add_node(0, "center")
    for i in range(0, 500):
        name = str(random.randint(0, 10000000000))
        created_date = random_created_at()
        graph.add_node(
            created_date,
            name,
            {
                "location": str(random.randint(0, 10000000000)),
                "age": random.randint(0, 100),
            },
            "User",
        )
        graph.add_edge(created_date, "center", name)

    return graph


SPECS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "graph-specs")


def apply_spec(graph, spec):
    """Apply a JSON-loaded graph spec to an existing Graph or PersistentGraph.

    Mirrors the positional-argument calling conventions Raphtory's Python
    API expects — `add_node(time, name)`, `add_node(time, name, props)`,
    `add_node(time, name, props, type)`, and the equivalents for add_edge —
    so the third-arg `{}` vs no-third-arg distinction is preserved exactly.
    """
    for node in spec["nodes"]:
        time = node["time"]
        name = node["name"]
        if "nodeType" in node:
            graph.add_node(time, name, node.get("properties", {}), node["nodeType"])
        elif "properties" in node:
            graph.add_node(time, name, node["properties"])
        else:
            graph.add_node(time, name)
    for edge in spec["edges"]:
        time = edge["time"]
        src = edge["src"]
        dst = edge["dst"]
        layer = edge.get("layer")
        props = edge.get("properties", {})
        if layer is not None:
            graph.add_edge(time, src, dst, props, layer)
        elif "properties" in edge:
            graph.add_edge(time, src, dst, props)
        else:
            graph.add_edge(time, src, dst)
    for deletion in spec.get("deletions", []):
        graph.delete_edge(
            deletion["time"],
            deletion["src"],
            deletion["dst"],
            deletion.get("layer"),
        )
    if "metadata" in spec:
        graph.add_metadata(spec["metadata"])
    return graph


def load_spec(name):
    with open(os.path.join(SPECS_DIR, f"{name}.json")) as f:
        return json.load(f)


def build_from_spec(name):
    spec = load_spec(name)
    cls = PersistentGraph if spec["graphType"] == "PERSISTENT" else Graph
    return apply_spec(cls(), spec)


def __main__():
    port = int(os.environ.get("RAPHTORY_PORT", "1736"))
    work_dir = os.environ.get(
        "RAPHTORY_WORK_DIR", os.path.join(tempfile.gettempdir(), "vanilla-graphs")
    )

    for sub in ("vanilla", "new_folder"):
        target = os.path.join(work_dir, sub)
        shutil.rmtree(target, ignore_errors=True)
        os.makedirs(target, exist_ok=True)

    def graph_path(*parts):
        return os.path.join(work_dir, *parts)

    build_from_spec("event").save_to_file(graph_path("vanilla", "event"))
    build_from_spec("persistent").save_to_file(graph_path("vanilla", "persistent"))

    setup_large_graph(Graph()).save_to_file(graph_path("vanilla", "large"))

    build_from_spec("filler").save_to_file(graph_path("vanilla", "filler"))
    g = build_from_spec("persistent_filler")
    g.save_to_file(graph_path("vanilla", "persistent_filler"))
    g.save_to_file(graph_path("new_folder", "persistent_filler"))

    build_from_spec("second_filler").save_to_file(
        graph_path("vanilla", "second_filler")
    )
    g = build_from_spec("persistent_second_filler")
    g.save_to_file(graph_path("new_folder", "persistent_second_filler"))
    g.save_to_file(graph_path("vanilla", "persistent_second_filler"))

    build_from_spec("variant_test").save_to_file(graph_path("vanilla", "variant_test"))

    build_from_spec("temporal_props").save_to_file(
        graph_path("vanilla", "temporal_props")
    )

    build_from_spec("numerical").save_to_file(graph_path("vanilla", "numerical"))

    server = graphql.GraphServer(work_dir=work_dir)
    server.run(port=port)


if __name__ == "__main__":
    __main__()
