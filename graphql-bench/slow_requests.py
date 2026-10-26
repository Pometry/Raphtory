"""Pretty-print the slow requests captured by the stress test into a log.

The stress test records every request slower than SLOW_MS as one JSONL line (see the sampling
section of src/utils.ts); k6 can only write to a single console file, so the readable form is
produced here instead:

    python slow_requests.py samples.jsonl --output slow_req.log

Entries are appended slowest first and keyed by (time, id), so re-running over the same samples
file does not duplicate what the log already holds.
"""

import argparse
import json
import os
import re
import textwrap
from datetime import datetime, timezone

RULE = "=" * 96


OPERATION = re.compile(r"\s*(query|mutation|subscription)\s*(?:\((.*?)\))?\s*(?=\{)", re.S)


def pretty_query(query, width=88):
    """Indent a minified GraphQL query: one field per line, nested by selection set.

    The operation's variable declarations are wrapped rather than indented one per line: there
    are a dozen or more of them on a composed query and none of them is what you are reading
    the log for.
    """
    operation = OPERATION.match(query)
    if not operation:
        return format_selection(query)

    keyword, params = operation.group(1), operation.group(2)
    body = format_selection(query[operation.end() :]).split("\n")
    if not params:
        return "\n".join([f"{keyword} {body[0]}"] + body[1:])

    wrapped = textwrap.wrap(
        ", ".join(split_top_level(params)), width=width, break_long_words=False
    )
    if len(wrapped) == 1:
        return "\n".join([f"{keyword} ({wrapped[0]}) {body[0]}"] + body[1:])
    return "\n".join(
        [f"{keyword} ("]
        + [f"  {line}" for line in wrapped]
        + [f") {body[0]}"]
        + body[1:]
    )


def split_top_level(params):
    """Split a variable declaration list on the commas that separate declarations."""
    parts, depth, buf = [], 0, []
    for char in params:
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(char)
    if "".join(buf).strip():
        parts.append("".join(buf).strip())
    return parts


def format_selection(query):
    out, buf, indent, parens, in_str = [], [], 0, 0, False

    def flush():
        chunk = "".join(buf).strip()
        del buf[:]
        if chunk:
            out.append("  " * indent + chunk)

    prev = ""
    for char in query:
        if in_str:
            buf.append(char)
            if char == '"' and prev != "\\":
                in_str = False
        elif char == '"':
            in_str = True
            buf.append(char)
        elif char == "(":
            parens += 1
            buf.append(char)
        elif char == ")":
            parens -= 1
            buf.append(char)
        elif parens > 0:  # argument lists stay on one line
            buf.append(char)
        elif char == "{":
            buf.append(" {")
            flush()
            indent += 1
        elif char == "}":
            flush()
            indent -= 1
            out.append("  " * indent + "}")
        elif char == ",":
            flush()
        else:
            buf.append(char)
        prev = char
    flush()
    return "\n".join(out)


def block(sample, body_chars):
    timings = sample.get("timings", {})
    breakdown = " ".join(
        f"{name} {timings[name]:.1f}ms"
        for name in ("waiting", "blocked", "connecting", "sending", "receiving")
        if name in timings
    )
    size = sample.get("responseChars")
    if size is None:
        response = sample.get("response")
        size = len(response if isinstance(response, str) else json.dumps(response or ""))

    lines = [
        RULE,
        f"{sample['time']}  {sample['id']}",
        f"  op        {sample.get('op', '?')} ({sample.get('opKind', '?')})"
        f"  reason={sample.get('reason', '?')}  status={sample.get('status')}"
        f"  ok={str(sample.get('ok')).lower()}",
        f"  duration  {sample['durationMs']:.1f}ms  ({breakdown})",
        f"  response  {size} chars"
        + ("  [truncated]" if sample.get("truncated") else ""),
        "  query",
        indent_lines(pretty_query(sample["request"]["query"]), 4),
        "  variables",
        indent_lines(json.dumps(sample["request"].get("variables"), indent=2), 4),
    ]
    if not sample.get("ok", True):  # the error message is the point of a failed request
        body = sample.get("response")
        body = body if isinstance(body, str) else json.dumps(body, indent=2)
        lines += ["  response body", indent_lines(body[:body_chars], 4)]
    return "\n".join(lines) + "\n"


def indent_lines(text, width):
    pad = " " * width
    return "\n".join(pad + line for line in text.split("\n"))


def plural(count):
    return f"{count} slow request" + ("" if count == 1 else "s")


def already_logged(path):
    """(time, id) header lines of the entries the log already holds."""
    if not os.path.exists(path):
        return set()
    header = re.compile(r"^(\S+Z)\s+(vu\d+-iter\d+-req\d+)$")
    with open(path) as log:
        return {match.groups() for match in map(header.match, log) if match}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("samples", nargs="?", default="samples.jsonl")
    parser.add_argument("-o", "--output", default="slow_req.log")
    parser.add_argument(
        "-n", "--top", type=int, default=0, help="keep only the N slowest (0 = all)"
    )
    parser.add_argument(
        "--min-ms",
        type=float,
        default=0,
        help="only requests at least this slow (0 = whatever the run recorded as slow)",
    )
    parser.add_argument(
        "--response-chars", type=int, default=2000, help="cap on logged error bodies"
    )
    parser.add_argument(
        "--stdout", action="store_true", help="print the entries instead of appending"
    )
    args = parser.parse_args()

    if not os.path.exists(args.samples):
        parser.error(f"no samples file at {args.samples}, run `make stress-test-sampled` first")

    samples = []
    with open(args.samples) as handle:
        for line in handle:
            line = line.strip()
            if not line.startswith("{"):
                continue  # k6's own log lines, if any ended up in the file
            try:
                sample = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "durationMs" not in sample or "request" not in sample:
                continue
            # without --min-ms the log holds exactly what the run flagged as slow, so the
            # SLOW_MS the test ran with stays the single source of truth
            slow = (
                sample["durationMs"] >= args.min_ms
                if args.min_ms
                else sample.get("reason") == "slow"
            )
            if slow:
                samples.append(sample)

    samples.sort(key=lambda s: -s["durationMs"])
    total = len(samples)
    if args.top:
        samples = samples[: args.top]

    if args.stdout:
        for sample in samples:
            print(block(sample, args.response_chars))
        return

    logged = already_logged(args.output)
    new = [s for s in samples if (s["time"], s["id"]) not in logged]
    if not new:
        print(f"{args.output}: nothing new out of {plural(total)}")
        return

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with open(args.output, "a") as log:
        log.write(f"\n##### {len(new)} slow requests from {args.samples} appended {now}\n")
        for sample in new:
            log.write(block(sample, args.response_chars))

    print(f"{args.output}: appended {plural(len(new))} (of {total} in {args.samples})")
    for sample in new[:10]:
        print(
            f"  {sample['durationMs']:8.1f}ms  {sample.get('op', '?')}"
            f"/{sample.get('opKind', '?')}  {sample['id']}"
        )
    if len(new) > 10:
        print(f"  ... {len(new) - 10} more in {args.output}")


if __name__ == "__main__":
    main()
