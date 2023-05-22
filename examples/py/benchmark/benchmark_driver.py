from raphtory_bench import RaphtoryBench
from kuzu_bench import KuzuBench
from networkx_bench import NetworkXBench
from neo4j_bench import Neo4jBench
from graphtool_bench import GraphToolBench
from memgraph_bench import MemgraphBench
from cozo_bench import CozoDBBench
import time


# Display menu and get user's choice
def display_menu():
    print("Benchmark Options:")
    print("0. Run All")
    print("1. Run Raphtory Benchmark")
    print("2. Run GraphTool Benchmark")
    print("3. Run Kuzu Benchmark")
    print("4. Run NetworkX Benchmark")
    print("5. Run Neo4j Benchmark")
    print("6. Run Memgraph Benchmark")
    print("7. Run CozoDB Benchmark")
    print("8. Exit")
    choice = int(input("Enter your choice: "))
    return choice


def setup():
    return {
        # 0: RunAll(),
        1: RaphtoryBench(),
        2: GraphToolBench(),
        3: KuzuBench(),
        4: NetworkXBench(),
        5: Neo4jBench(),
        6: MemgraphBench(),
        7: CozoDBBench()
    }


def run_benchmark(choice):
    driver = setup()[choice]
    times = {}
    fns = ['setup', 'degree', 'out_neighbours', 'page_rank', 'connected_components']
    for fn in fns:
        print("** Running " + fn + "...")
        start_time = time.time()
        getattr(driver, fn)()
        end_time = time.time()
        print(fn + " time: " + str(end_time - start_time))
        times[fn] = end_time - start_time
    return times

def main():
    results = {}
    while True:
        choice = display_menu()
        if choice not in setup():
            print(str(choice) + " not found. Exiting...")
            break
        results[choice] = run_benchmark(choice)


if __name__ == "__main__":
    main()
