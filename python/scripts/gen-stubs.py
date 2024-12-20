#!/usr/bin/env python3
from stub_gen import gen_module
from pathlib import Path


if __name__ == "__main__":
    import raphtory

    path = Path(__file__).parent.parent / "python"
    gen_module(raphtory, "raphtory", path, "raphtory")
