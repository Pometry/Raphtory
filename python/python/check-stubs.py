# %% Imports

import ast
import inspect
from typing import Dict, List, Optional, TypedDict

import raphtory

# %% Functions


def parse_stub_file(filename: str) -> Dict[str, List[str]]:
    with open(filename, "r") as file:
        tree = ast.parse(file.read())

    stub_info = {}
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ClassDef) and node.name == "Graph":
            for method in node.body:
                if isinstance(method, ast.FunctionDef):
                    stub_info[method.name] = [arg.arg for arg in method.args.args]

    return stub_info


def get_actual_signatures() -> Dict[str, List[str]]:
    actual_info = {}
    for name, method in inspect.getmembers(raphtory.Graph, inspect.isfunction):
        try:
            sig = inspect.signature(method)
            actual_info[name] = list(sig.parameters.keys())
        except ValueError:
            print(f"Couldn't get signature for {name}")

    return actual_info


def validate_stubs(stub_info: Dict[str, List[str]], actual_info: Dict[str, List[str]]):
    all_methods = set(stub_info.keys()) | set(actual_info.keys())

    for method in all_methods:
        if method not in stub_info:
            print(f"Method {method} is missing from the stub file")
        elif method not in actual_info:
            print(f"Method {method} is in the stub file but not in the actual class")
        else:
            stub_params = stub_info[method]
            actual_params = actual_info[method]

            if stub_params != actual_params:
                print(f"Mismatch in parameters for method {method}:")
                print(f"  Stub: {stub_params}")
                print(f"  Actual: {actual_params}")
            else:
                print(f"Method {method} matches between stub and actual class")


# %% Testing


stub_filename = "./raphtory/raphtory.pyi"

with open(stub_filename, "r") as file:
    tree = ast.parse(file.read())

classes = {}
functions = {}


class FunctionTypeDef(TypedDict):
    name: str
    args: "list[tuple[str, Optional[str]]]"
    returns: Optional[str]


def parse_annotation(node: Optional[ast.AST]) -> Optional[str]:
    if node is None:
        return None
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{parse_annotation(node.value)}.{node.attr}"
    if isinstance(node, ast.Subscript):
        value = parse_annotation(node.value)
        slice_value = parse_annotation(node.slice)
        return f"{value}[{slice_value}]"
    if isinstance(node, ast.Index):
        return parse_annotation(node.value)
    if isinstance(node, ast.Tuple):
        elements = [parse_annotation(elem) or "Any" for elem in node.elts]
        return f"Tuple[{', '.join(elements)}]"
    if isinstance(node, ast.List):
        elements = [parse_annotation(elem) or "Any" for elem in node.elts]
        return f"List[{', '.join(elements)}]"
    if isinstance(node, ast.Constant):
        return repr(node.value)
    return str(node)


def parse_func(f: ast.FunctionDef) -> FunctionTypeDef:
    args: list[tuple[str, Optional[str]]] = [
        (arg.arg, parse_annotation(arg.annotation)) for arg in f.args.args
    ]

    returns: Optional[str] = parse_annotation(f.returns)

    return FunctionTypeDef(name=f.name, args=args, returns=returns)


for node in ast.iter_child_nodes(tree):
    if isinstance(node, ast.ClassDef):
        methods = {}
        attributes = []

        for item in node.body:
            if isinstance(item, ast.FunctionDef):
                print("FUNC: ", parse_func(item))
