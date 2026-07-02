"""
Durability regression tests for read-only opens.

These tests exercise Raphtory's Drop-side-effect fixes for concurrent
read-only opens of a disk-backed graph:

  Bug #1  Reader Drop appended a shutdown checkpoint to the writer's
          WAL, overwriting live records at LSN 256 of ``log.0``.  On
          next reopen after a writer crash, recovery failed with
          "Expected checkpoint at given LSN".
  Bug #2  Reader Drop rewrote ``.meta`` via a fixed-name ``.tmp`` file.
          Racing renames intermittently ENOENT'd at
          ``meta_file.rs:80``.

Both bugs are fixed by Raphtory-side Drop guards (see
``WriterShutdownGuard`` on ``GraphStore`` in ``db4-storage`` and
``MetadataRefreshGuard`` on ``Storage`` in ``raphtory``).

A separate reader-vs-writer race in pometry-storage's segment
publication has its own regression test in pometry-storage's
``python/tests/test_read_only_durability.py``.

Each test is parametrised over ``Graph`` and ``PersistentGraph`` — the
underlying storage layer is shared, but the Python entry points are
separate ``#[pymethods]`` blocks that could drift independently.
"""

import hashlib
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    "DISK_TEST_MARK" not in os.environ,
    reason="disk-backed graph tests require the storage feature",
)


GRAPH_CLASSES = ["Graph", "PersistentGraph"]


def _writer_script(graph_cls_name: str) -> str:
    """Build the subprocess writer script for the given graph class."""
    # Double braces in the outer f-string escape to a literal single
    # brace, so ``{{i}}`` becomes ``{i}`` in the emitted script (an
    # f-string in the subprocess), and ``{{"v": i}}`` becomes
    # ``{"v": i}`` (a dict).
    return f"""
import sys, time
from raphtory import {graph_cls_name}
g = {graph_cls_name}(sys.argv[1])
print("UP", flush=True)
i = 0
while True:
    for _ in range(20):
        g.add_node(1 + i, f"n{{i}}", properties={{"v": i}})
        i += 1
    g.flush()
    time.sleep(0.01)
"""


def _spawn_writer(path, graph_cls_name):
    p = subprocess.Popen(
        [sys.executable, "-c", _writer_script(graph_cls_name), path],
        stdout=subprocess.PIPE,
        text=True,
    )
    up = p.stdout.readline()
    assert up.strip() == "UP", f"writer failed to start: {up!r}"
    # Give the writer a moment to accumulate real data before observing it —
    # otherwise the reader races the very first flush.
    time.sleep(0.2)
    return p


def _kill(p):
    p.send_signal(signal.SIGKILL)
    p.wait(timeout=10)


def _hash_all_files(root):
    """Return {relative_path: sha256_hex} for every file under `root`.
    Any change to any file shows up as a differing dict value."""
    root = Path(root)
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _graph_cls(graph_cls_name):
    import raphtory

    return getattr(raphtory, graph_cls_name)


# --- Test 1: CONTROL --------------------------------------------------------


@pytest.mark.parametrize("graph_cls_name", GRAPH_CLASSES)
def test_writer_crash_recovers_cleanly_without_readers(tmp_path, graph_cls_name):
    """CONTROL: a writer that is SIGKILLed with no concurrent readers must
    still be recoverable on reopen.  Ordinary crash recovery must work
    before we can meaningfully assert anything about the concurrent-reader
    case below."""
    graph_cls = _graph_cls(graph_cls_name)

    path = str(tmp_path / "g")
    w = _spawn_writer(path, graph_cls_name)
    try:
        time.sleep(1.5)
    finally:
        _kill(w)

    g = graph_cls.load(path)
    assert g.count_nodes() > 0


# --- Test 2: Bugs #1 and #2 (deterministic Drop side-effect test) ----------


@pytest.mark.parametrize("graph_cls_name", GRAPH_CLASSES)
def test_readonly_open_and_drop_does_not_modify_any_file(tmp_path, graph_cls_name):
    """Bugs #1 and #2: a read-only ``Graph.load(...)`` followed by drop
    must not modify any file in the graph directory.

    We freeze the writer subprocess with ``SIGSTOP`` so it cannot be the
    source of any file change during our observation window.  Any diff
    between the before/after directory hashes is therefore caused by the
    read-only handle itself.

    Before the fix this catches:

    * Bug #1 - Drop for GraphStore appends a shutdown checkpoint to the
      WAL, changing ``wal/logs/log.0`` bytes.
    * Bug #2 - Drop for Storage rewrites ``.meta``, changing its bytes
      (and racing on ``.tmp`` under concurrency).
    """
    graph_cls = _graph_cls(graph_cls_name)

    path = Path(tmp_path) / "g"
    w = _spawn_writer(str(path), graph_cls_name)
    # Give the writer time to produce enough data that log.0 has real
    # records past the header (which is what Bug #1 clobbers).
    time.sleep(0.5)

    os.kill(w.pid, signal.SIGSTOP)
    try:
        before = _hash_all_files(path)

        rg = graph_cls.load(str(path), read_only=True)
        rg.count_nodes()
        del rg

        after = _hash_all_files(path)
    finally:
        # Resume the writer so it can be cleanly SIGKILLed.
        os.kill(w.pid, signal.SIGCONT)
        _kill(w)

    diffs = {
        k: (before.get(k), after.get(k))
        for k in before.keys() | after.keys()
        if before.get(k) != after.get(k)
    }
    assert not diffs, (
        f"read-only open+drop modified {len(diffs)} file(s) in the graph "
        f"directory: {sorted(diffs.keys())}"
    )
