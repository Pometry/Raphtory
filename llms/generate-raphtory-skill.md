# Generate Raphtory Python API Examples

You are generating a Python script that demonstrates the entire raphtory Python API through compact, runnable examples. This script will later be converted into a Claude Code skill file automatically.

## Step 1: Discover the API surface

First, get the installed version:

```bash
python3 -c "import raphtory; print(raphtory.__version__)"
```

Then dynamically discover all raphtory submodules and read their pydoc:

```bash
python3 -c "
import raphtory, pkgutil, importlib
modules = ['raphtory']
for importer, modname, ispkg in pkgutil.walk_packages(raphtory.__path__, prefix='raphtory.'):
    if not modname.startswith('raphtory._'):
        modules.append(modname)
print('\n'.join(modules))
"
```

Run `pydoc <module>` for the top-level `raphtory` module and every discovered submodule. Read through ALL of them carefully — this is the authoritative source for the current API.

## Step 2: Generate the examples script

Write the file `llms/raphtory_python_examples.py`. This is a plain Python script where:

- **Section headers** are comments starting with `## ` (e.g. `## Graph Creation & Basics`)
- **Explanations** are regular `#` comments
- **Code** is normal Python statements
- Lines that produce meaningful output should use `print()` so the output can be captured later
- The script must run top-to-bottom without errors

### Topics to cover (adapt based on what actually exists in the API):

- Graph creation and basic metrics
- Adding nodes and edges (with timestamps, properties, types, layers)
- Bulk loading from DataFrames, dicts, CSV
- Querying nodes and edges (iteration, properties, temporal properties, history)
- Time views and windowing
- Layer views
- Subgraph and filtering
- ALL algorithms (grouped by category)
- Graph generators and built-in dataset loaders
- GraphQL server basics (import and setup, but don't actually start the server)
- Saving and loading graphs

### Style rules

- Keep the script to ~300-500 lines
- Use comments for section headers and brief explanations only — no walls of text
- Group related operations together
- Use built-in loaders (e.g. `raphtory.graph_loader.karate_club_graph()`) or simple hand-built graphs — no external CSV files
- Every algorithm should have at least a one-line example
- Use `print()` for outputs you want to show (e.g. `print(g.count_nodes())`)
- Do NOT wrap everything in functions or classes — keep it as a flat script

### Example of expected style:

```python
## Graph Creation & Basics
from raphtory import Graph

g = Graph()
print(g)

# Add nodes with timestamps and properties
g.add_node(1, "alice", properties={"age": 30})
g.add_node(2, "bob", properties={"age": 25})
print(g.count_nodes())

# Add edges with layers
g.add_edge(3, "alice", "bob", properties={"weight": 1.0}, layer="friends")
print(g.count_edges())
```

## Step 3: Verify the script

**This is critical.** Run the entire script and confirm it executes without errors:

```bash
python llms/raphtory_python_examples.py
```

If any part fails:

1. Debug why it failed
2. Fix the script
3. Re-run to verify

## Step 4: Final review

Before writing the final file, verify:

- [ ] All major API areas discovered in Step 1 are covered
- [ ] All algorithms have at least a one-line example
- [ ] The entire script executes successfully end-to-end
- [ ] The script is ~300-500 lines
- [ ] No external data file dependencies

Write the result to `llms/raphtory_python_examples.py` using Bash (do NOT use the Write tool).
