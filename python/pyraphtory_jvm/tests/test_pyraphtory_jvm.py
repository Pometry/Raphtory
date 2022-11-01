from pyraphtory_jvm import __version__
from pathlib import Path

version_file = Path(__file__).parent.parent.parent.parent / "version"


def test_version():
    with open(version_file) as f:
        version = f.readline()
    assert __version__ == version
