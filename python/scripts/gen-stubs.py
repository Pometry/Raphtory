#!/usr/bin/env python3
from stub_gen import gen_module, set_imports
from pathlib import Path

imports = [
    "from typing import *",
    "from raphtory import *",
    "import raphtory.filter as filter",
    "from raphtory.algorithms import *",
    "from raphtory.vectors import *",
    "from raphtory.node_state import *",
    "from raphtory.graphql import *",
    "from raphtory.typing import *",
    "import numpy as np",
    "from numpy.typing import NDArray",
    "from datetime import datetime",
    "import pandas",
    "from pandas import DataFrame",
    "import pyarrow  # type: ignore[import-untyped]",
    "from pyarrow import DataType  # type: ignore[import-untyped]",
    "from os import PathLike",
    "from decimal import Decimal",
    "import networkx as nx  # type: ignore",
    "import pyvis  # type: ignore",
    "from raphtory.iterables import *",
]


# Submodules this wheel does not build. `gql` comes from clam-core, which only
# the outer workspace links in, so a `raphtory` imported from that build carries
# a `gql` this package never ships. Generating its stub here would promise a
# module that is not there; the outer repo's own gen-stubs.py keeps it.
NOT_BUILT_BY_THIS_WHEEL = {"gql"}

if __name__ == "__main__":
    import raphtory

    path = Path(__file__).parent.parent / "python"
    set_imports(imports)
    raphtory.__all__ = [
        name for name in raphtory.__all__ if name not in NOT_BUILT_BY_THIS_WHEEL
    ]
    print("Creating stubs...")
    gen_module(raphtory, "raphtory", path, "raphtory")
