#!/usr/bin/env python3
from stub_gen import gen_module, set_imports
from pathlib import Path

imports = """
from typing import *
from raphtory import *
from raphtory.algorithms import *
from raphtory.vectors import *
from raphtory.node_state import *
from raphtory.graphql import *
from raphtory.typing import *
from datetime import datetime
from pandas import DataFrame
from os import PathLike
import networkx as nx  # type: ignore
import pyvis  # type: ignore"""


if __name__ == "__main__":
    import raphtory

    path = Path(__file__).parent.parent / "python"
    set_imports(imports)
    gen_module(raphtory, "raphtory", path, "raphtory")
