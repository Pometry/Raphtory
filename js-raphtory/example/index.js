import { Graph, Direction } from "js-raphtory";

const g = new Graph();

g.addVertex(2n, "Bob");
g.addVertex(3n, "Alice");
g.addVertex(12n, "Charlie");

// both src and dst must be the same type
g.addEdge(9n, "Bob", "Alice", { relation: "knows" });

const bob = g.getVertex("Bob");
const alice = g.getVertex("Alice");

function printVertex(v) {

    console.log(
        {
            id: v.id(),
            name: v.name(),
            neighbours_out: v.outNeighbours(),
            neighbours_in: v.inNeighbours(),
            neighbours: v.neighbours(),
            inDegree: v.inDegree(),
            outDegree: v.outDegree()
        }
    );
}

const wg = g.window(3n, 9n);

const charlie = g.getVertex("Charlie");
console.log(charlie);

printVertex(charlie);
printVertex(bob);
printVertex(alice);