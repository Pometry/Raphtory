import pytest
from raphtory import plottingutils


@pytest.mark.parametrize(
    "observations, cdf, ccdf",
    [
        ([1, 2, 3, 4], [1, 2, 3, 4], [4, 3, 2, 1]),
        ([1, 1, 2], [2, 2, 3], [3, 3, 1]),
        ([5], [1], [1]),
    ],
)
def test_cdf_and_ccdf_count_the_observations(observations, cdf, ccdf):
    n = len(observations)
    assert plottingutils.cdf(observations, normalised=False)[1].tolist() == cdf
    assert plottingutils.ccdf(observations, normalised=False)[1].tolist() == ccdf
    assert plottingutils.cdf(observations)[1].tolist() == [c / n for c in cdf]
    assert plottingutils.ccdf(observations)[1].tolist() == [c / n for c in ccdf]


@pytest.mark.parametrize(
    "observations, y",
    [
        ([1, 1, 1, 1], [0.0, 0.25, 0.5, 0.75, 1.0]),
        ([0, 0, 0, 4], [0.0, 0.0, 0.0, 0.0, 1.0]),
        ([5], [0.0, 1.0]),
    ],
)
def test_lorenz_runs_from_the_origin_to_one(observations, y):
    x, lorenz_y = plottingutils.lorenz(observations)
    n = len(observations)
    assert x.tolist() == [i / n for i in range(n + 1)]
    assert lorenz_y.tolist() == y
