import csv
from raphtory import Graph

structure_file = "../../../resource/lotr-without-header.csv"
graph = Graph(1)

with open(structure_file, 'r') as csvfile:
    datareader = csv.reader(csvfile)
    for row in datareader:

        source_node = row[0]
        destination_node = row[1]
        timestamp = int(row[2])

        graph.add_vertex(timestamp, source_node, {"vertex_type": "Character"})
        graph.add_vertex(timestamp, destination_node, {"vertex_type": "Character"})
        graph.add_edge(timestamp, source_node, destination_node, {"edge_type": "Character_Co-occurence"})

graph.save_to_file("lotr")
