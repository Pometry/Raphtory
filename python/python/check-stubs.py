# %% Imports

import ast
from typing import Optional, TypedDict

# %% Functions


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


# %% Testing

stub_filename = "./raphtory/raphtory.pyi"

with open(stub_filename, "r") as file:
    tree = ast.parse(file.read())

for node in ast.iter_child_nodes(tree):
    if isinstance(node, ast.ClassDef):
        methods = {}
        attributes = []

        for item in node.body:
            if isinstance(item, ast.FunctionDef):
                print("FUNC: ", parse_func(item))
