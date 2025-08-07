import pandas as pd

iter_csv = pd.read_csv("output.csv.gz", iterator=True, chunksize=10_000, compression='gzip')
output = pd.concat([chunk[["timestamp", "metric_name", "metric_value", "scenario"]] for chunk in iter_csv])

output['timestamp'] = pd.to_datetime(output['timestamp'], unit='s')
output = output.set_index('timestamp')

def find_max_rate(scenario_name):
    scenario = output[output["scenario"] == scenario_name]
    req_duration = scenario[scenario["metric_name"] == "http_req_duration"]
    # avg = req_duration["metric_value"].resample('1s').mean()
    p95 = req_duration["metric_value"].resample('1s').quantile(0.95)
    # p99 = req_duration["metric_value"].resample('1s').quantile(0.99)
    rate = req_duration["metric_value"].resample('1s').count()

    vus = scenario[scenario["metric_name"] == "vus"]["metric_value"].resample('1s').mean()

    cols = {
        # 'avg': avg,
        'p95': p95,
        # 'p99': p99,
        'rate': rate,
        'vus': vus,
    }
    trend = pd.DataFrame(cols).iloc[10:].reset_index() # discard first 10 seconds

    valid_mask = trend["p95"] < 200 # 200ms
    invalid_mask = ~valid_mask
    zero_is_valid = invalid_mask.cumsum()
    valid_trend = trend[zero_is_valid == 0]
    return valid_trend["rate"].max()
    # max_rate_index = valid_trend["rate"].idxmax()
    # perf = valid_trend.loc[max_rate_index]
    # return pd.Series(perf, name=scenario_name)

scenarios = output["scenario"].unique()[1:] # first element is nan
max_rates = [find_max_rate(scenario) for scenario in scenarios]

# FIXME: remove the -100
results = [{"name": name, "unit": "req/s", "value": value - 100} for (name, value) in zip(scenarios, max_rates)]
df = pd.DataFrame(results)
df.to_json(orient="records", path_or_buf="output.json")
