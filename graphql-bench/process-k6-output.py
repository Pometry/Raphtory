import json
import os
import sys

import pandas as pd

CSV = "output.csv.gz"
OUT = "output.json"
SERVER_LOG = os.environ.get("BENCH_SERVER_LOG", "server.log")
K6_LOG = os.environ.get("BENCH_K6_LOG", "k6.log")
TAIL = int(os.environ.get("BENCH_LOG_TAIL", "100"))
VERBOSE = os.environ.get("BENCH_VERBOSE", "").strip().lower() in {"1", "true", "yes"}

NEEDED_COLS = ["timestamp", "metric_name", "metric_value", "scenario"]

# Everything interesting about a run is recorded, but a run that worked does not need to say so:
# the report is buffered and only printed if we end up failing (or if BENCH_VERBOSE is set).
_report = []


def log(msg=""):
    """Record a line for the failure report."""
    _report.append(msg)
    if VERBOSE:
        print(msg, flush=True)


def out(msg=""):
    """Print a line whatever happens: results, and warnings worth seeing on a green run."""
    print(msg, flush=True)


def flush_report():
    if not VERBOSE:
        for line in _report:
            print(line, flush=True)


def section(title):
    log()
    log(f"=== {title} " + "=" * max(0, 72 - len(title)))


def tail_file(path, lines=TAIL):
    section(f"tail -{lines} {path}")
    if not os.path.exists(path):
        log(f"(missing: {path})")
        return
    with open(path, errors="replace") as f:
        content = f.readlines()
    if not content:
        log(f"(empty: {path})")
        return
    log(f"({len(content)} lines total, showing last {min(lines, len(content))})")
    for line in content[-lines:]:
        log(line.rstrip("\n"))


def describe_csv_file():
    section(f"{CSV}")
    if not os.path.exists(CSV):
        log(f"(missing: {CSV} — k6 never wrote its CSV output)")
        return False
    size = os.path.getsize(CSV)
    log(f"size: {size} bytes")
    if size == 0:
        log("(empty file)")
        return False
    return True


def bail(reason, extra=()):
    """Print everything we know about the run, then fail loudly."""
    section("DIAGNOSIS")
    log(reason)
    for line in extra:
        log(line)
    log()
    log(
        "The most common cause is k6's setup() aborting the run before any scenario\n"
        "starts (e.g. the raphtory server was not listening yet, or it died): k6 then\n"
        "emits only setup-phase samples, which carry no scenario tag, so there are no\n"
        "per-scenario rates to report. The k6 and server logs below should say which."
    )
    tail_file(K6_LOG)
    tail_file(SERVER_LOG)
    flush_report()
    out()
    out(f"refusing to write an empty {OUT}; failing so this is visible in CI")
    sys.exit(1)


if not describe_csv_file():
    bail(f"No usable k6 CSV output in {CSV}.")

chunks = []
rows_read = 0
try:
    iter_csv = pd.read_csv(CSV, iterator=True, chunksize=10_000, compression="gzip")
    for chunk in iter_csv:
        rows_read += len(chunk)
        missing = [c for c in NEEDED_COLS if c not in chunk.columns]
        if missing:
            bail(
                f"k6 CSV is missing expected column(s) {missing}.",
                [f"columns present: {list(chunk.columns)}"],
            )
        chunks.append(chunk[NEEDED_COLS])
except pd.errors.EmptyDataError:
    bail(f"{CSV} has no CSV data at all (not even a header row).")

log(f"rows read: {rows_read} (in {len(chunks)} chunk(s))")

if not chunks:
    bail(f"{CSV} contained a header but zero data rows — k6 recorded no samples.")

output = pd.concat(chunks)
output["timestamp"] = pd.to_datetime(output["timestamp"], unit="s")
output = output.set_index("timestamp")

section("what k6 recorded")
log(f"time span: {output.index.min()} .. {output.index.max()}")
log()
log("samples per metric_name:")
log(output["metric_name"].value_counts().to_string())
log()
log("samples per scenario (NaN = setup/teardown, no scenario tag):")
log(output["scenario"].value_counts(dropna=False).to_string())


def find_max_rate(scenario_name):
    """Highest 1s request rate reached while p95 stayed under 200ms."""
    scenario = output[output["scenario"] == scenario_name]
    req_duration = scenario[scenario["metric_name"] == "http_req_duration"]
    if req_duration.empty:
        log(f"  {scenario_name}: no http_req_duration samples")
        return float("nan")

    p95 = req_duration["metric_value"].resample("1s").quantile(0.95)
    rate = req_duration["metric_value"].resample("1s").count()
    vus = scenario[scenario["metric_name"] == "vus"]["metric_value"].resample("1s").mean()

    trend = pd.DataFrame({"p95": p95, "rate": rate, "vus": vus})
    total_seconds = len(trend)
    trend = trend.iloc[10:].reset_index()  # discard first 10 seconds
    if trend.empty:
        log(
            f"  {scenario_name}: only {total_seconds}s of samples, all discarded by the"
            " 10s warm-up cut"
        )
        return float("nan")

    valid_mask = trend["p95"] < 200  # 200ms
    invalid_mask = ~valid_mask
    zero_is_valid = invalid_mask.cumsum()
    valid_trend = trend[zero_is_valid == 0]
    if valid_trend.empty:
        log(
            f"  {scenario_name}: p95 was already over 200ms in the first measured second"
            f" ({trend['p95'].iloc[0]:.1f}ms), so no rate qualifies"
        )
        return float("nan")

    value = valid_trend["rate"].max()
    log(
        f"  {scenario_name}: {len(req_duration)} requests over {total_seconds}s,"
        f" {len(valid_trend)}s under the p95 gate, max rate {value}"
    )
    return value


# The scheduling pair runs under deliberate saturation, so the p95-gated max-rate is meaningless
# for it; report the steady completed rate instead (collapses if short queries starve).
UNDER_LOAD_SCENARIOS = {"heavy_load", "short_queries_under_heavy_load"}


def steady_rate(scenario_name):
    scenario = output[output["scenario"] == scenario_name]
    req_duration = scenario[scenario["metric_name"] == "http_req_duration"]
    if req_duration.empty:
        log(f"  {scenario_name}: no http_req_duration samples")
        return float("nan")
    rate = req_duration["metric_value"].resample("1s").count()
    total_seconds = len(rate)
    rate = rate.iloc[10:]  # discard first 10 seconds
    if rate.empty:
        log(
            f"  {scenario_name}: only {total_seconds}s of samples, all discarded by the"
            " 10s warm-up cut"
        )
        return float("nan")
    value = rate.mean()
    log(
        f"  {scenario_name}: {len(req_duration)} requests over {total_seconds}s,"
        f" steady rate {value}"
    )
    return value


scenarios = list(output["scenario"].dropna().unique())

section("per-scenario results")
if not scenarios:
    bail(
        "k6 emitted no scenario-tagged samples, so there is nothing to report.",
        [f"metrics seen: {sorted(output['metric_name'].unique())}"],
    )

log(f"scenarios found ({len(scenarios)}): {scenarios}")
max_rates = [
    steady_rate(scenario) if scenario in UNDER_LOAD_SCENARIOS else find_max_rate(scenario)
    for scenario in scenarios
]

results = [
    # float() because these are numpy scalars, which json.dump cannot serialise
    {"name": name, "unit": "req/s", "value": float(value)}
    for (name, value) in zip(scenarios, max_rates)
    if pd.notna(value)
]
dropped = [name for (name, value) in zip(scenarios, max_rates) if pd.isna(value)]

if not results:
    bail(
        "Every scenario was present but none produced a usable rate.",
        [f"scenarios: {scenarios}"],
    )

with open(OUT, "w") as f:
    json.dump(results, f)

width = max(len(r["name"]) for r in results)
out(f"{OUT} ({len(results)} scenario(s), req/s):")
for r in results:
    out(f"  {r['name']:<{width}}  {r['value']:.1f}")
if dropped:
    # a scenario that ran but measured nothing is worth saying out loud on a green run
    out()
    out(f"WARNING: no usable value for {dropped} — dropped from {OUT}")
    out(f"         re-run with BENCH_VERBOSE=1, or see {K6_LOG}, for why")
