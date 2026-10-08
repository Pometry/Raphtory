"""
Validates a graph against SHACL shapes now, as of the start of each year of its history, and on its
event view (every triple ever asserted, ignoring retractions).

    python scripts/validate.py GRAPH_PATH SHAPES
"""

import collections
import datetime
import sys

from raphtory import PersistentGraph


def summary(report):
    if report["conforms"]:
        return "conforms"
    by_constraint = collections.Counter(
        (result["path"] or "-", result["constraint_component"].rsplit("#", 1)[-1])
        for result in report["results"]
    )
    details = ", ".join(f"{n} x {path} {constraint}" for (path, constraint), n in by_constraint.most_common(3))
    return f"{len(report['results'])} violations ({details})"


def main(graph_path, shapes):
    graph = PersistentGraph.load_from_file(graph_path)
    print(f"now:          {summary(graph.validate_shacl(shapes))}")
    first, last = graph.earliest_time.dt.year, graph.latest_time.dt.year
    years = [datetime.datetime(year, 1, 1, tzinfo=datetime.timezone.utc) for year in range(first + 1, last + 1)]
    for time, report in graph.validate_shacl_at(shapes, years):
        moment = datetime.datetime.fromtimestamp(time / 1000, tz=datetime.timezone.utc)
        print(f"as of {moment:%Y-%m-%d}: {summary(report)}")
    print(f"event view:   {summary(graph.event_graph().validate_shacl(shapes))}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
