"""
Durability regression tests for concurrent read-only opens.

There are three distinct bugs in this area, targeted by three separate
tests below:

  Bug #1  Reader Drop appends a shutdown checkpoint to the writer's WAL,
          overwriting live records at LSN 256 of ``log.0``.  On next
          reopen after a writer crash, recovery fails with
          "Expected checkpoint at given LSN".
  Bug #2  Reader Drop rewrites ``.meta`` via a fixed-name ``.tmp`` file.
          Racing renames intermittently ENOENT at ``meta_file.rs:80``.
  Bug #3  Reader ``Graph.load`` races the writer's segment-file
          creation and can list a segment directory before its layer
          stats file has been written, causing ENOENT at
          ``disk_layer_stats/mod.rs:301``.

Bugs #1 and #2 are Drop side-effects on shared state.  Bug #3 is a
reader-open race, independent of Drop.

See ``docs/db-v4-wal-explainer.md`` for the underlying mechanism.
"""

import hashlib
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest


pytestmark = pytest.mark.skipif(
    "DISK_TEST_MARK" not in os.environ,
    reason="disk-backed graph tests require the storage feature",
)


# Subprocess writer: opens the graph at argv[1] and appends nodes + flushes
# in a tight loop.  Prints ``UP`` to stdout so the parent knows the writer
# has taken the writer lock and started producing data.
_WRITER_SCRIPT = """
import sys, time
from raphtory import Graph
g = Graph(sys.argv[1])
print("UP", flush=True)
i = 0
while True:
    for _ in range(20):
        g.add_node(1 + i, f"n{i}", properties={"v": i})
        i += 1
    g.flush()
    time.sleep(0.01)
"""


def _spawn_writer(path):
    p = subprocess.Popen(
        [sys.executable, "-c", _WRITER_SCRIPT, path],
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


# --- Test 1: CONTROL --------------------------------------------------------


def test_writer_crash_recovers_cleanly_without_readers(tmp_path):
    """CONTROL: a writer that is SIGKILLed with no concurrent readers must
    still be recoverable on reopen.  Ordinary crash recovery must work
    before we can meaningfully assert anything about the concurrent-reader
    cases below."""
    from raphtory import Graph

    path = str(tmp_path / "g")
    w = _spawn_writer(path)
    try:
        time.sleep(1.5)
    finally:
        _kill(w)

    g = Graph.load(path)
    assert g.count_nodes() > 0


# --- Test 2: Bugs #1 and #2 (deterministic Drop side-effect test) ----------


def test_readonly_open_and_drop_does_not_modify_any_file(tmp_path):
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
    from raphtory import Graph

    path = Path(tmp_path) / "g"
    w = _spawn_writer(str(path))
    # Give the writer time to produce enough data that log.0 has real
    # records past the header (which is what Bug #1 clobbers).
    time.sleep(0.5)

    os.kill(w.pid, signal.SIGSTOP)
    try:
        before = _hash_all_files(path)

        rg = Graph.load(str(path), read_only=True)
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


# --- Test 3: Bug #3 (reader-open race, xfail placeholder) ------------------


@pytest.mark.xfail(
    reason=(
        "Bug #3: reader Graph.load races the writer's segment-file creation; "
        "reader can list a segment directory before its layer stats file has "
        "been written, causing ENOENT at disk_layer_stats/mod.rs:301. "
        "Separate follow-up from the Drop-side-effect fix (Bugs #1 and #2)."
    ),
    strict=False,
)
def test_reader_open_does_not_race_writer_segment_creation(tmp_path):
    """Bug #3: while a writer is writing + flushing continuously, reader
    threads that repeatedly open the graph read-only should not encounter
    ENOENT on layer stats files that the writer is mid-creating.

    Marked ``xfail`` because the fix is a separate change — the writer's
    segment creation needs to become atomic before this can pass.
    """
    from raphtory import Graph

    path = str(tmp_path / "g")
    w = _spawn_writer(path)

    stop = threading.Event()
    enoent_errors = []
    lock = threading.Lock()

    def reader():
        while not stop.is_set():
            try:
                rg = Graph.load(path, read_only=True)
                rg.count_nodes()
                del rg
            except Exception as exc:  # noqa: BLE001 - collect any error
                msg = str(exc)
                # Focus on the specific race we're tracking here — ignore
                # any other transient errors that a general stress test
                # might surface.
                if "disk_layer_stats" in msg and "No such file" in msg:
                    with lock:
                        enoent_errors.append(repr(exc))

    threads = [threading.Thread(target=reader, daemon=True) for _ in range(4)]
    try:
        for t in threads:
            t.start()
        time.sleep(1.5)
        stop.set()
        for t in threads:
            t.join(timeout=5)
    finally:
        _kill(w)

    assert not enoent_errors, (
        f"reader-open raced writer segment creation: {len(enoent_errors)} "
        f"ENOENT errors, first: {enoent_errors[0]}"
    )
