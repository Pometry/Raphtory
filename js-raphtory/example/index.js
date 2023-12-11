import { Graph } from "js-raphtory";

const g = new Graph();

g.addNode(2n, "Bob");
g.addNode(3n, "Alice");
g.addNode(12n, "Charlie");

// both src and dst must be the same type
g.addEdge(9n, "Bob", "Alice", { relation: "knows" });

const bob = g.getNode("Bob");
const alice = g.getNode("Alice");

function printNode(v) {

    console.log(
        {
            id: v.id(),
            name: v.name(),
            neighbours_out: v.outNeighbours(),
            neighbours_in: v.inNeighbours(),
            neighbours: v.neighbours(),
            inDegree: v.inDegree(),
            outDegree: v.outDegree(),
            props: v.properties(),
            edges: v.edges(),
        }
    );
}

const wg = g.window(3n, 9n);

const charlie = g.getNode("Charlie");
console.log(charlie);

printNode(charlie);
printNode(bob);
printNode(alice);