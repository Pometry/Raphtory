import os
import random
from datetime import datetime, timedelta

from raphtory import graphql, PersistentGraph, Graph


## This is the test data for the UI tests so bare in mind they might fail if this file is changed


def setup_graph(graph):
    graph.add_node(1640995200000, "None")
    graph.add_node(1648598400000, "Pedro", {"age": 28}, "Person")
    graph.add_node(1656288000000, "Ben", {"age": 30}, "Person")
    graph.add_node(1663977600000, "Hamza", {"age": 30}, "Person")

    graph.add_edge(1671667200000, "Ben", "Hamza", {"where": "London"}, "meets")
    graph.add_edge(1679356800000, "Ben", "Pedro", {"where": "Madrid"}, "meets")

    graph.add_node(1687046400000, "Pometry", {}, "Company")
    graph.add_edge(1687132800000, "Ben", "Pometry", {}, "founds")
    graph.add_edge(1687132800000, "Hamza", "Pometry", {}, "founds")

    graph.add_edge(1689734400000, "Hamza", "Pedro", {"where": "London"}, "meets")
    graph.add_edge(1697424000000, "Hamza", "Pedro", {"where": "Madrid"}, "meets")

    graph.add_edge(1705017600000, "Hamza", "Pedro", {"amount": 20}, "transfers")
    graph.add_edge(1707609600000, "Pedro", "Hamza", {"amount": 40}, "transfers")
    graph.add_edge(1710115200000, "Ben", "Hamza", {"amount": 40}, "transfers")

    return graph


def filler_graph(graph):
    graph.add_node(0, "None")
    graph.add_node(10, "Ben", {"age": 30}, "Person")
    graph.add_node(20, "Hamza", {"age": 30}, "Person")

    graph.add_edge(40, "Ben", "Hamza", {"where": "London"}, "meets")
    graph.add_edge(50, "Ben", "Pedro", {"where": "Madrid"}, "meets")

    graph.add_node(60, "Pometry", {}, "Company")
    graph.add_edge(60, "Ben", "Pometry", {}, "founds")
    graph.add_edge(60, "Hamza", "Pometry", {}, "founds")

    return graph


def second_filler_graph(graph):
    graph.add_node(0, "None")
    graph.add_node(10, "John", {"age": 40}, "Person")
    graph.add_node(20, "Fred", {"age": 30}, "Person")
    graph.add_node(25, "Judy", {"age": 30}, "Person")
    graph.add_node(30, "Jasper", {"age": 20}, "Person")

    graph.add_edge(
        40,
        "John",
        "Fred",
        {"where": "Amsterdam", "when": "2013-09-12", "reason": "coursemates"},
        "meets",
    )
    graph.add_edge(
        50,
        "Fred",
        "Judy",
        {"where": "London", "when": "2020-04-23", "how_long": "2 days"},
        "meets",
    )

    graph.add_node(
        60, "Rabbit Inc", {"industry": "Agriculture", "founded": 2019}, "Company"
    )

    graph.add_edge(
        61, "John", "Rabbit Inc", {"role": "Farmer", "start": "2019-01-01"}, "founds"
    )
    graph.add_edge(
        62,
        "Fred",
        "Rabbit Inc",
        {"role": "Tractor Driver", "start": "2019-01-01"},
        "founds",
    )

    graph.add_edge(
        70,
        "Fred",
        "Judy",
        {"where": "Chipping Norton", "when": "2023-09-15", "topic": "hiring"},
        "meets",
    )
    graph.add_edge(
        80,
        "John",
        "Jasper",
        {"where": "Hertfordshire", "when": "2024-03-10", "topic": "hiring"},
        "meets",
    )

    graph.add_edge(
        90,
        "Judy",
        "Jasper",
        {
            "where": "Chipping Norton",
            "when": "2024-11-02",
            "duration": "3 days",
            "reason": "build a farm",
        },
        "collaborates",
    )

    graph.add_edge(
        100, "Judy", "Rabbit Inc", {"role": "Advisor", "since": "2020-05-01"}, "advises"
    )

    graph.add_edge(
        110,
        "Fred",
        "None",
        {"reason": "reflects", "when": "2025-01-01"},
        "thinks_about",
    )

    return graph


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


def __main__():
    os.system("rm -rf /tmp/vanilla-graphs/vanilla")
    os.system("rm -rf /tmp/vanilla-graphs/new_folder")
    os.system("mkdir -p /tmp/vanilla-graphs/vanilla")
    os.system("mkdir -p /tmp/vanilla-graphs/new_folder")
    graph = setup_graph(Graph())
    graph.save_to_file("/tmp/vanilla-graphs/vanilla/event")

    graph = setup_graph(PersistentGraph())
    graph.delete_edge(90, "Ben", "Pedro", "meets")
    graph.save_to_file("/tmp/vanilla-graphs/vanilla/persistent")

    graph = setup_large_graph(Graph())
    graph.save_to_file("/tmp/vanilla-graphs/vanilla/large")

    graph = filler_graph(Graph())
    graph.save_to_file("/tmp/vanilla-graphs/vanilla/filler")
    graph = filler_graph(PersistentGraph())
    graph.save_to_file("/tmp/vanilla-graphs/vanilla/persistent_filler")
    graph.save_to_file("/tmp/vanilla-graphs/new_folder/persistent_filler")

    graph = second_filler_graph(Graph())
    graph.save_to_file("/tmp/vanilla-graphs/vanilla/second_filler")
    graph = second_filler_graph(PersistentGraph())
    graph.save_to_file("/tmp/vanilla-graphs/new_folder/persistent_second_filler")
    graph.save_to_file("/tmp/vanilla-graphs/vanilla/persistent_second_filler")

    server = graphql.GraphServer(work_dir="/tmp/vanilla-graphs")
    server.run(port=1736)


if __name__ == "__main__":
    __main__()
