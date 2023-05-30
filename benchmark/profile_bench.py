from raphtory_bench import RaphtoryBench
from kuzu_bench import KuzuBench
from networkx_bench import NetworkXBench
from neo4j_bench import Neo4jBench
from graphtool_bench import GraphToolBench
from memgraph_bench import MemgraphBench
import time


# Display menu and get user's choice
def display_menu():
    print("Benchmark Options:")
    print("1. Run Raphtory Benchmark")
    print("2. Run GraphTool Benchmark")
    print("3. Run Kuzu Benchmark")
    print("4. Run NetworkX Benchmark")
    print("5. Run Neo4j Benchmark")
    print("6. Run Memgraph Benchmark")
    print("0. Exit")
    choice = int(input("Enter your choice: "))
    return choice


def setup():
    return {
        1: RaphtoryBench(),
        2: GraphToolBench(),
        3: KuzuBench(),
        4: NetworkXBench(),
        5: Neo4jBench(),
        6: MemgraphBench()
    }


def run_benchmark(choice):
    driver = setup()[choice]
    fns = ['setup', 'degree', 'out_neighbours', 'page_rank', 'connected_components']
    for fn in fns:
        print("** Running " + fn + "...")
        start_time = time.time()
        getattr(driver, fn)()
        end_time = time.time()
        print(fn + " time: " + str(end_time - start_time))


def main():
    # while True:
    #     choice = display_menu()
    #     if choice == 0:
    #         break
    run_benchmark(1)


if __name__ == "__main__":
    main()
